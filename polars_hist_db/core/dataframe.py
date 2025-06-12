from datetime import datetime, time
import logging
from types import MappingProxyType
from typing import Dict, Iterable, List, Literal, Mapping, Optional, Union
from uuid import uuid4

import polars as pl
from sqlalchemy import (
    and_,
    bindparam,
    column,
    Connection,
    DefaultClause,
    delete,
    select,
    Select,
    Selectable,
    Subquery,
    Table,
    TextClause,
)


from .db import DbOps
from .delta_table import DeltaTableOps
from .table import TableOps
from .table_config import TableConfigOps
from .timehint import TimeHint

from ..config import DeltaConfig, TableConfig

from ..types import SQLType, PolarsType

from ..utils.db_utils import (
    is_text_col,
    strip_outer_quotes,
)


LOGGER = logging.getLogger(__name__)


class DataframeOps:
    def __init__(self, connection: Connection):
        self.connection = connection

    def from_table(
        self,
        table_schema: str,
        table_name: str,
        time_hint: Optional[TimeHint] = None,
    ) -> pl.DataFrame:
        tbo = TableOps(table_schema, table_name, self.connection)
        tbl = tbo.get_table_metadata()
        select_sql = select(tbl)
        dtypes = PolarsType.get_dataframe_schema_from_selectable(select_sql)

        if time_hint:
            select_sql = time_hint.apply(select_sql, tbl)

        df = pl.read_database(
            select_sql, self.connection, schema_overrides=dtypes
        ).pipe(PolarsType.cast_str_to_cat)

        return df

    def from_selectable(
        self,
        query: Selectable | TextClause,
        schema_overrides: Optional[Mapping[str, pl.DataType]] = None,
    ) -> pl.DataFrame:
        inferred_dtypes = PolarsType.get_dataframe_schema_from_selectable(query)
        if schema_overrides is None:
            schema_overrides = dict()

        inferred_dtypes.update(schema_overrides)
        df = pl.read_database(
            query, self.connection, schema_overrides=inferred_dtypes
        ).pipe(PolarsType.cast_str_to_cat, ignore_cols=schema_overrides.keys())

        return df

    def from_raw_sql(
        self, query: str, schema_overrides: Optional[Mapping[str, pl.DataType]] = None
    ) -> pl.DataFrame:
        inferred_dtypes = PolarsType.get_dataframe_schema_from_sqltext(
            query, self.connection
        )
        if schema_overrides is None:
            schema_overrides = dict()

        inferred_dtypes.update(schema_overrides)
        df = pl.read_database(
            query, self.connection, schema_overrides=inferred_dtypes
        ).pipe(PolarsType.cast_str_to_cat, ignore_cols=schema_overrides.keys())

        return df

    @staticmethod
    def fill_nulls_with_defaults(
        df: pl.DataFrame, default_values: Dict[str, str]
    ) -> pl.DataFrame:
        for col in df.columns:
            if col in default_values.keys():
                col_polars_dtype = df[col].dtype
                if col_polars_dtype == pl.Boolean:
                    default_value = pl.lit(bool(default_values[col])).cast(
                        col_polars_dtype
                    )
                elif col_polars_dtype == pl.Time:
                    default_value = pl.lit(
                        time.fromisoformat(default_values[col])
                    ).cast(col_polars_dtype)
                else:
                    default_value = pl.lit(default_values[col]).cast(col_polars_dtype)

                df = df.with_columns(pl.col(col).fill_null(default_value))

        return df

    def table_create(
        self,
        table_schema: str,
        table_name: str,
        df: pl.DataFrame,
        primary_keys: List[str],
        tbl_for_types: Optional[Table] = None,
        is_temporary_table: bool = False,
    ):
        table_config = TableConfig.from_dataframe(
            df, table_schema, table_name, primary_keys, default_categorical_length=64
        )

        if tbl_for_types is not None:
            sql_types = SQLType.from_table(tbl_for_types)
            for col_cfg in table_config.columns:
                if col_cfg.name in sql_types:
                    col_cfg.data_type = sql_types[col_cfg.name]

        TableConfigOps(self.connection)._create_nontemporal(
            table_name,
            table_config,
            is_temporary_table=is_temporary_table,
        )

        return table_schema, table_name

    def table_query(
        self,
        table_schema: str,
        table_name: str,
        query_df: pl.DataFrame,
        column_selection: Optional[List[str]],
        time_hint: TimeHint = TimeHint(mode="none"),
    ) -> pl.DataFrame:
        tmp_table_name = f"tmp_{uuid4()}".lower()

        tmp_schema, tmp_name = self.table_create(
            table_schema,
            tmp_table_name,
            query_df,
            query_df.columns,
            is_temporary_table=True,
        )

        self.table_insert(
            query_df,
            tmp_schema,
            tmp_table_name,
            query_df.columns,
            prefill_nulls_with_default=False,
        )

        tmp_tbl = TableOps(table_schema, tmp_name, self.connection).get_table_metadata()
        tbl: Union[Table, Subquery] = TableOps(
            table_schema, table_name, self.connection
        ).get_table_metadata()

        if column_selection is None:
            column_selection = [c.name for c in tbl.columns]

        _sql: Select = select(tbl)
        if time_hint:
            _sql = time_hint.apply(_sql, tbl)

        tbl_query = (
            select(*[column(c.name, c.type) for c in tbl.columns])
            .select_from(_sql.subquery())
            .subquery()
        )

        join_on_clause = and_(*[c == tbl_query.c[c.name] for c in tmp_tbl.columns])

        select_stmt = select(*[tbl_query.c[c] for c in column_selection]).join(
            tmp_tbl, join_on_clause
        )
        df = self.from_selectable(select_stmt)
        return df

    def table_insert(
        self,
        df: pl.DataFrame,
        table_schema: str,
        table_name: str,
        uniqueness_col_set: Iterable[str],
        prefill_nulls_with_default: bool,
        clear_table_first: bool = False,
    ) -> int:
        tbo = TableOps(table_schema, table_name, self.connection)
        tbl = tbo.get_table_metadata()
        if clear_table_first:
            delete_sql = tbl.delete()
            result = DbOps(self.connection).execute_sqlalchemy(
                "sql.dataframe.insert.pre_clearout_all", delete_sql
            )

            LOGGER.debug(
                "deleted all %s rows from %s.%s",
                result.rowcount,
                table_schema,
                table_name,
            )

        if df.is_empty():
            return 0

        if prefill_nulls_with_default:
            for c in tbl.columns:
                if c.name not in df.columns:
                    continue

                if c.server_default is not None and c.server_default.has_argument:
                    assert isinstance(c.server_default, DefaultClause)
                    raw_default_value = strip_outer_quotes(str(c.server_default.arg))

                    dtype = PolarsType.from_sql(repr(c.type))
                    default_value = PolarsType.convert_str_value(
                        raw_default_value, dtype
                    )
                    df = df.with_columns(pl.col(c.name).fill_null(default_value))

                    LOGGER.debug(
                        "prefilled nulls: df[%s] <- %s", c.name, raw_default_value
                    )

        df = _remove_duplicate_rows(df, uniqueness_col_set)
        _prevalidate_insert_from_dataframe(df, tbl, disable_check=False)

        LOGGER.debug(
            "inserting dataframe %s into %s.%s", df.shape, table_schema, table_name
        )

        cols_to_upload = {c.name for c in tbl.columns}.intersection(df.columns)
        num_rows_changed: int = (
            df.select(cols_to_upload)
            .to_pandas()
            .to_sql(
                name=table_name,
                con=self.connection,
                schema=table_schema,
                if_exists="append",
                index=False,
                chunksize=1000,
            )
        )

        if num_rows_changed is None:
            num_rows_changed = 0

        LOGGER.debug("insert dataframe affected %d/%d rows", num_rows_changed, len(df))

        return num_rows_changed

    def table_update(
        self,
        df: pl.DataFrame,
        table_schema: str,
        table_name: str,
        primary_keys_override: Optional[List[str]] = None,
    ):
        if df.is_empty():
            return

        LOGGER.debug(
            "updating from dataframe %s in %s.%s", df.shape, table_schema, table_name
        )

        tbo = TableOps(table_schema, table_name, self.connection)
        tbl = tbo.get_table_metadata()
        primary_keys = [c.name for c in tbl.primary_key]
        if primary_keys_override is not None:
            primary_keys = primary_keys_override

        df = _remove_duplicate_rows(df, primary_keys)
        common_cols = set(df.columns).intersection([c.name for c in tbl.columns])

        update_sql = (
            tbl.update()
            .values(
                {
                    col: bindparam(f"_{col}")
                    for col in common_cols
                    if col not in primary_keys
                }
            )
            .where(and_(*[tbl.c[k] == bindparam(f"_{k}") for k in primary_keys]))
        )

        update_data = (
            df.rename({col: f"_{col}" for col in common_cols})
            .to_pandas()
            .to_dict(orient="records")
        )

        result = DbOps(self.connection).execute_sqlalchemy(
            f"sql.dataframe.update.{len(update_data)}",
            update_sql,
            update_data,
        )

        LOGGER.info(
            "updated from dataframe %d/%d rows in %s.%s",
            result.rowcount,
            len(df),
            table_schema,
            table_name,
        )

    def table_upsert_temporal(
        self,
        df: pl.DataFrame,
        table_schema: str,
        table_name: str,
        delta_config: DeltaConfig,
        update_time: Optional[datetime] = None,
        src_tgt_colname_map: Mapping[str, str] = MappingProxyType({}),
    ):
        # currently this function always inserts into a delta table first
        # then upserts from the delta table to the target table
        tbo = TableOps(table_schema, table_name, self.connection)
        common_columns = tbo.get_column_intersection(df.columns)

        if len(common_columns) == 0:
            raise ValueError(
                f"unable to upsert dataframe, it has no columns in common with target table {table_name}"
            )

        tmp_table_config = TableConfigOps(self.connection).from_table(
            table_schema, table_name
        )

        tmp_table_config.name = delta_config.tmp_table_name(table_name)

        TableConfigOps(self.connection).create(
            tmp_table_config, is_delta_table=True, is_temporary_table=True
        )

        self.table_insert(
            df,
            table_schema,
            tmp_table_config.name,
            tmp_table_config.primary_keys,
            clear_table_first=True,
            prefill_nulls_with_default=delta_config.prefill_nulls_with_default,
        )

        DeltaTableOps(
            table_schema, tmp_table_config.name, delta_config, self.connection
        ).upsert(
            table_name,
            update_time,
            is_main_table=True,
            source_columns=[c.name for c in common_columns],
            src_tgt_colname_map=src_tgt_colname_map,
        )

    def table_delete_rows_temporal(
        self,
        df: pl.DataFrame,
        table_schema: str,
        table_name: str,
        update_time: Optional[datetime] = None,
    ) -> int:
        DbOps(self.connection).set_system_versioning_time(update_time)

        num_deletions = 0
        if not df.is_empty():
            num_deletions = self.table_delete_rows(df, table_schema, table_name)

        DbOps(self.connection).set_system_versioning_time(None)

        return num_deletions

    def table_delete_rows(
        self, df: pl.DataFrame, table_schema: str, table_name: str
    ) -> int:
        if df.is_empty():
            return 0

        LOGGER.debug(
            "deleteing from %s.%s using dataframe %s",
            table_schema,
            table_name,
            df.shape,
        )

        tbo = TableOps(table_schema, table_name, self.connection)
        tbl = tbo.get_table_metadata()

        primary_keys = [c.name for c in tbl.primary_key]
        if len(set(df.columns).difference(primary_keys)) > 0:
            raise ValueError("missing primary keys in dataframe: %s", primary_keys)

        delete_sql = delete(tbl).where(
            and_(*[tbl.c[col] == bindparam(f"_{col}") for col in df.columns])
        )

        delete_data = (
            df.rename({col: f"_{col}" for col in df.columns})
            .to_pandas()
            .to_dict(orient="records")
        )

        count_before = tbo.row_count()
        _result = DbOps(self.connection).execute_sqlalchemy(
            f"sql.dataframe.delete.{len(delete_data)}",
            delete_sql,
            delete_data,
        )

        count_after = tbo.row_count()

        num_deleted_rows = count_after - count_before
        LOGGER.debug(
            "deleted %d rows from %s.%s", num_deleted_rows, table_schema, table_name
        )

        return num_deleted_rows


def _remove_duplicate_rows(
    df: pl.DataFrame,
    unique_columns: Iterable[str] = (),
    keep: Literal["first", "last", "any", "none"] = "last",
):
    rowcount_before = df.shape[0]
    unique_columns = [c for c in unique_columns if c in df.columns]

    if len(unique_columns) == 0:
        df = df.unique(keep=keep, maintain_order=True)
    else:
        df = df.unique(subset=unique_columns, keep=keep, maintain_order=True)

    rows_removed = rowcount_before - len(df)
    if rows_removed > 0:
        LOGGER.debug("removed %s duplicate rows", rows_removed)

    return df


def _prevalidate_insert_from_dataframe(
    df: pl.DataFrame, tbl: Table, disable_check: bool
):
    if disable_check:
        return

    for col in tbl.columns:
        if col not in df.columns:
            continue

        if is_text_col(str(col.type)):
            df_col = df.select(col.name).drop_nulls()
            if df_col.is_empty():
                continue

        try:
            max_col_len = col.type.length  # type: ignore[attr-defined]
            truncated_data = (
                df_col.select(
                    col.name,
                    length=(
                        pl.col(col.name)
                        .cast(pl.Utf8)
                        .map_elements(len, skip_nulls=True)
                    ),
                )
                .sort(pl.col("length"), descending=True)
                .filter(pl.col("length") >= pl.lit(max_col_len))
            )

            if not truncated_data.is_empty():
                LOGGER.error("data truncation in column %s", col.name)
                LOGGER.error(truncated_data)

        except Exception as e:
            LOGGER.error("failed to check column %s", col, exc_info=e)
