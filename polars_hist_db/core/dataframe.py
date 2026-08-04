import logging
from collections.abc import Iterable, Mapping
from datetime import datetime, time
from itertools import batched
from types import MappingProxyType
from typing import Literal
from uuid import uuid4

import polars as pl
from sqlalchemy import (
    Connection,
    DefaultClause,
    Select,
    Selectable,
    Subquery,
    Table,
    TextClause,
    and_,
    bindparam,
    column,
    delete,
    select,
)

from ..config import DeltaConfig, TableConfig
from ..types import PolarsType, SQLType
from ..utils.db_utils import (
    is_text_col,
    strip_outer_quotes,
)
from .db import DbOps
from .delta_table import DeltaTableOps
from .table import TableOps
from .table_config import TableConfigOps
from .timehint import TimeHint

LOGGER = logging.getLogger(__name__)
_NO_TIME_HINT = TimeHint(mode="none")


class DataframeOps:
    def __init__(self, connection: Connection):
        self.connection = connection

    def from_table(
        self,
        table_schema: str,
        table_name: str,
        time_hint: TimeHint | None = None,
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
        schema_overrides: Mapping[str, pl.DataType] | None = None,
    ) -> pl.DataFrame:
        inferred_dtypes = PolarsType.get_dataframe_schema_from_selectable(query)
        if schema_overrides is None:
            schema_overrides = {}

        inferred_dtypes.update(schema_overrides)
        df = pl.read_database(
            query, self.connection, schema_overrides=inferred_dtypes
        ).pipe(PolarsType.cast_str_to_cat, ignore_cols=schema_overrides.keys())

        return df

    def from_raw_sql(
        self, query: str, schema_overrides: Mapping[str, pl.DataType] | None = None
    ) -> pl.DataFrame:
        inferred_dtypes = PolarsType.get_dataframe_schema_from_sqltext(
            query, self.connection
        )
        if schema_overrides is None:
            schema_overrides = {}

        inferred_dtypes.update(schema_overrides)
        df = pl.read_database(
            query, self.connection, schema_overrides=inferred_dtypes
        ).pipe(PolarsType.cast_str_to_cat, ignore_cols=schema_overrides.keys())

        return df

    @staticmethod
    def fill_nulls_with_defaults(
        df: pl.DataFrame, default_values: dict[str, str]
    ) -> pl.DataFrame:
        for col in df.columns:
            if col in default_values:
                col_polars_dtype = df[col].dtype
                if col_polars_dtype == pl.Time:
                    default_value = pl.lit(
                        time.fromisoformat(default_values[col])
                    ).cast(col_polars_dtype)
                else:
                    default_value = PolarsType.convert_str_value(
                        default_values[col], col_polars_dtype
                    )

                df = df.with_columns(pl.col(col).fill_null(default_value))

        return df

    def table_create(
        self,
        table_schema: str,
        table_name: str,
        df: pl.DataFrame,
        primary_keys: list[str],
        tbl_for_types: Table | None = None,
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
        column_selection: list[str] | None,
        time_hint: TimeHint = _NO_TIME_HINT,
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
        tbl: Table | Subquery = TableOps(
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
        force_type_coercion: bool = False,
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
        df = PolarsType.enforce_database_schema(
            df,
            PolarsType._get_polars_dtypes_from_table(tbl),
            backend=getattr(getattr(self.connection, "dialect", None), "name", "sql"),
            operation="table_insert",
            force_type_coercion=force_type_coercion,
        )
        _prevalidate_insert_from_dataframe(df, tbl, disable_check=False)

        LOGGER.debug(
            "inserting dataframe %s into %s.%s", df.shape, table_schema, table_name
        )

        cols_to_upload = [c.name for c in tbl.columns if c.name in df.columns]
        num_rows_changed = 0
        for rows in batched(df.select(cols_to_upload).iter_rows(named=True), 1000):
            result = DbOps(self.connection).execute_sqlalchemy(
                f"sql.dataframe.insert.{len(rows)}", tbl.insert(), list(rows)
            )
            num_rows_changed += max(result.rowcount, 0)

        LOGGER.debug("insert dataframe affected %d/%d rows", num_rows_changed, len(df))

        return num_rows_changed

    def table_update(
        self,
        df: pl.DataFrame,
        table_schema: str,
        table_name: str,
        primary_keys_override: list[str] | None = None,
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

        update_data = [
            {f"_{col}": value for col, value in row.items()}
            for row in df.select(common_cols).iter_rows(named=True)
        ]

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
        update_time: datetime | None = None,
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
        update_time: datetime | None = None,
    ) -> int:
        DbOps(self.connection).set_system_versioning_time(update_time)
        try:
            return (
                self.table_delete_rows(df, table_schema, table_name)
                if not df.is_empty()
                else 0
            )
        finally:
            DbOps(self.connection).set_system_versioning_time(None)

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
        missing_primary_keys = set(primary_keys).difference(df.columns)
        if missing_primary_keys:
            raise ValueError(
                f"missing primary keys in dataframe: {missing_primary_keys}"
            )

        delete_sql = delete(tbl).where(
            and_(*[tbl.c[col] == bindparam(f"_{col}") for col in primary_keys])
        )

        delete_data = [
            {f"_{col}": value for col, value in zip(primary_keys, row, strict=True)}
            for row in df.select(primary_keys).iter_rows()
        ]
        result = DbOps(self.connection).execute_sqlalchemy(
            f"sql.dataframe.delete.{len(delete_data)}",
            delete_sql,
            delete_data,
        )
        LOGGER.debug(
            "deleted %d rows from %s.%s", result.rowcount, table_schema, table_name
        )
        return result.rowcount


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

        if not is_text_col(str(col.type)):
            continue
        max_col_len = col.type.length  # type: ignore[attr-defined]
        if max_col_len is None:
            continue
        truncated_data = df.filter(
            pl.col(col.name).str.len_chars() > pl.lit(max_col_len)
        )
        if not truncated_data.is_empty():
            LOGGER.error("data truncation in column %s", col.name)
            LOGGER.error(truncated_data)
