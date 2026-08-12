from collections.abc import Mapping
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, cast

import polars as pl

from ..config import TableConfig
from ..core import TimeHint
from ..types import PolarsType
from .temporal import system_time_hint_clause
from .xtdb_transport import (
    _driver_connection,
    _execute_xtdb_arrow_copy,
    _execute_xtdb_dml,
    _qualified_table_name,
    _validate_identifier,
    _xtdb_column_identifier,
    _xtdb_parameter_value,
)


def _xtdb_json_key_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, (date, datetime, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return value


def _xtdb_pgwire_key_parameters(df: pl.DataFrame) -> tuple[Any, ...]:
    from psycopg.types.json import Jsonb

    return (
        Jsonb(
            [
                {column: _xtdb_json_key_value(value) for column, value in row.items()}
                for row in df.iter_rows(named=True)
            ]
        ),
    )


def _xtdb_adbc_key_parameters(df: pl.DataFrame) -> Any:
    import pyarrow as pa

    from .xtdb_arrow import _normalize_xtdb_ingest_arrow

    table = _normalize_xtdb_ingest_arrow(df.to_arrow())
    rows = pa.StructArray.from_arrays(
        [table.column(column).combine_chunks() for column in table.column_names],
        names=table.column_names,
    )
    values = pa.ListArray.from_arrays([0, len(rows)], rows)
    return pa.record_batch({"v0": values})


def _xtdb_key_parameters(
    dataframe_ops: Any,
    df: pl.DataFrame,
) -> tuple[str, Any]:
    if isinstance(dataframe_ops, XtdbAdbcDataframeOps):
        return "?", _xtdb_adbc_key_parameters(df)
    return "%s", _xtdb_pgwire_key_parameters(df)


def _xtdb_key_expression(
    dataframe_ops: Any,
    column: str,
    cast_type: str,
) -> str:
    expression = f"(q.key).{_xtdb_column_identifier(column)}"
    if isinstance(dataframe_ops, XtdbAdbcDataframeOps):
        return expression
    return f"CAST({expression} AS {cast_type})"


def _table_config_ops(connection: Any) -> Any:
    from .xtdb_schema import XtdbTableConfigOps

    return XtdbTableConfigOps(connection)


class XtdbDataframeOps:
    def __init__(
        self,
        connection: Any,
        max_rows_per_insert: int | None = None,
    ):
        self.connection = connection
        self.max_rows_per_insert = max_rows_per_insert

    def from_raw_sql(
        self,
        query: str,
        schema_overrides: Mapping[str, pl.DataType] | None = None,
        execute_options: dict[str, Any] | None = None,
    ) -> pl.DataFrame:
        if schema_overrides is None:
            schema_overrides = {}
        kwargs: dict[str, Any] = {"schema_overrides": schema_overrides}
        if execute_options is not None:
            kwargs["execute_options"] = execute_options
        return pl.read_database(
            query,
            self.connection,
            **kwargs,
        )

    def from_table(
        self,
        table_schema: str,
        table_name: str,
        schema_overrides: Mapping[str, pl.DataType] | None = None,
        time_hint: TimeHint | None = None,
    ) -> pl.DataFrame:
        from .xtdb_arrow import (
            _restore_xtdb_logical_columns,
            _xtdb_physical_configured_column_dtypes,
        )

        table_config: TableConfig | None = None
        if schema_overrides is None:
            table_config = _table_config_ops(self.connection).from_table(
                table_schema,
                table_name,
            )
            schema_overrides = _xtdb_physical_configured_column_dtypes(table_config)

        hint_clause = system_time_hint_clause(time_hint)
        query = f"SELECT * FROM {table_schema}.{table_name}"
        if hint_clause:
            query = f"{query} {hint_clause}"

        return _restore_xtdb_logical_columns(
            self.from_raw_sql(
                query,
                schema_overrides,
            ),
            table_config,
        )

    def table_query(
        self,
        table_schema: str,
        table_name: str,
        query_df: pl.DataFrame,
        column_selection: list[str] | None,
        time_hint: TimeHint | None = None,
        table_config: TableConfig | None = None,
        basis_time: datetime | None = None,
    ) -> pl.DataFrame:
        from .xtdb_arrow import (
            _xtdb_document_id_cast_type,
            _xtdb_polars_type_or_none,
        )
        from .xtdb_query import (
            _xtdb_single_primary_key_alias,
            _xtdb_table_query_output_columns,
            _xtdb_table_query_select_expr,
            _xtdb_table_query_target_column,
            _xtdb_temporal_basis_clause,
            _xtdb_valid_time_clause,
        )

        if table_config is None:
            table_config = _table_config_ops(self.connection).from_table(
                table_schema,
                table_name,
            )
        output_columns = _xtdb_table_query_output_columns(
            table_config,
            column_selection,
        )
        schema_overrides = {}
        for column in table_config.columns:
            if column.name not in output_columns:
                continue
            dtype = _xtdb_polars_type_or_none(column.data_type)
            if dtype is not None:
                schema_overrides[column.name] = dtype
        if "_id" in output_columns:
            schema_overrides["_id"] = (
                _xtdb_polars_type_or_none(_xtdb_document_id_cast_type(table_config))
                or pl.String()
            )
        for temporal_column in ["__valid_from", "__valid_to"]:
            if temporal_column in output_columns:
                schema_overrides[temporal_column] = pl.Datetime("us")
        if query_df.is_empty():
            return pl.DataFrame(schema=schema_overrides)
        select_sql = ", ".join(
            _xtdb_table_query_select_expr(column, table_config)
            for column in output_columns
        )
        table_sql = _qualified_table_name(table_schema, table_name)
        valid_time_clause = (
            _xtdb_temporal_basis_clause(basis_time)
            if basis_time is not None
            else _xtdb_valid_time_clause(time_hint)
        )
        single_key_alias = _xtdb_single_primary_key_alias(table_config)
        if single_key_alias is not None and single_key_alias in output_columns:
            id_column = next(
                column
                for column in table_config.columns
                if column.name == single_key_alias
            )
            id_dtype = _xtdb_polars_type_or_none(id_column.data_type)
            if id_dtype is not None:
                schema_overrides[single_key_alias] = id_dtype
        from .xtdb_arrow import _xtdb_insert_casts, _xtdb_physical_column_name

        parameter_df = query_df.rename(
            {
                column: _xtdb_physical_column_name(column)
                for column in query_df.columns
                if column != _xtdb_physical_column_name(column)
            }
        )
        placeholder, parameters = _xtdb_key_parameters(self, parameter_df)
        join_clause = " AND ".join(
            "t."
            f"{_xtdb_table_query_target_column(column, table_config)} = "
            f"{_xtdb_key_expression(self, column, cast_type)}"
            for column, cast_type in zip(
                query_df.columns,
                _xtdb_insert_casts(query_df, table_config),
                strict=True,
            )
        )
        query = (
            f"SELECT {select_sql} "
            "FROM ("
            "SELECT *, _valid_from, _valid_to "
            f"FROM {table_sql}{valid_time_clause}"
            ") AS t "
            f"JOIN UNNEST({placeholder}) AS q(key) ON {join_clause}"
        )
        df = self.from_raw_sql(
            query,
            schema_overrides,
            {"parameters": parameters},
        )
        for temporal_column in ["__valid_from", "__valid_to"]:
            if (
                temporal_column in df.columns
                and isinstance(df[temporal_column].dtype, pl.Datetime)
                and getattr(df[temporal_column].dtype, "time_zone", None) is not None
            ):
                df = df.with_columns(pl.col(temporal_column).dt.replace_time_zone(None))
        if "__valid_to" in df.columns:
            df = df.with_columns(
                pl.col("__valid_to").fill_null(
                    datetime.fromisoformat("2106-02-07T06:28:15.999999")
                )
            )
        return df.pipe(PolarsType.cast_str_to_cat)

    def table_insert(
        self,
        df: pl.DataFrame,
        table_schema: str,
        table_name: str,
        table_config: TableConfig | None = None,
        update_time: datetime | None = None,
        force_type_coercion: bool = False,
    ) -> int:
        from .xtdb_arrow import (
            _apply_xtdb_configured_column_dtypes,
            _iter_xtdb_insert_chunks,
            _prepare_xtdb_insert_dataframe,
            _xtdb_insert_casts,
            _xtdb_physical_column_map,
            _xtdb_physical_column_name,
        )

        if table_config is not None:
            _xtdb_physical_column_map(table_config)
        df = _prepare_xtdb_insert_dataframe(df, table_config)
        df = _apply_xtdb_configured_column_dtypes(
            df,
            table_config,
            force_type_coercion=force_type_coercion,
        )
        if df.is_empty():
            return 0

        driver_connection = _driver_connection(self.connection)
        if driver_connection is not None:
            inserted_count = 0
            for chunk in _iter_xtdb_insert_chunks(df, self.max_rows_per_insert):
                table_sql = _qualified_table_name(table_schema, table_name)
                if chunk.height > 1:
                    physical_columns = {
                        column: _xtdb_physical_column_name(column)
                        for column in chunk.columns
                        if column != _xtdb_physical_column_name(column)
                    }
                    _execute_xtdb_arrow_copy(
                        self.connection,
                        table_sql,
                        chunk.rename(physical_columns),
                        system_time=update_time,
                    )
                    inserted_count += chunk.height
                    continue

                columns = [_xtdb_column_identifier(column) for column in chunk.columns]
                column_sql = ", ".join(columns)
                casts = _xtdb_insert_casts(chunk, table_config)
                rows = [
                    tuple(
                        _xtdb_parameter_value(value, cast)
                        for value, cast in zip(row, casts, strict=True)
                    )
                    for row in chunk.rows()
                ]
                values_sql = "(" + ", ".join(f"%s::{cast}" for cast in casts) + ")"
                insert_sql = (
                    f"INSERT INTO {table_sql} ({column_sql}) VALUES {values_sql}"
                )
                _execute_xtdb_dml(
                    self.connection,
                    insert_sql,
                    rows,
                    system_time=update_time,
                )
                inserted_count += len(rows)
            return inserted_count

        if update_time is not None:
            raise ValueError(
                "XTDB pgwire system-time writes require a live DBAPI connection"
            )

        inserted_count = 0
        for chunk in _iter_xtdb_insert_chunks(df, self.max_rows_per_insert):
            result = chunk.write_database(
                table_name=f"{table_schema}.{table_name}",
                connection=self.connection,
                if_table_exists="append",
            )
            inserted_count += int(result) if result is not None else 0
        return inserted_count


class XtdbAdbcDataframeOps:
    def __init__(
        self,
        connection: Any,
        max_rows_per_insert: int | None = None,
    ):
        self.connection = connection
        self.max_rows_per_insert = max_rows_per_insert

    def from_raw_sql(
        self,
        query: str,
        schema_overrides: Mapping[str, pl.DataType] | None = None,
        execute_options: dict[str, Any] | None = None,
    ) -> pl.DataFrame:
        from .xtdb_arrow import _apply_schema_overrides

        with self.connection.cursor() as cursor:
            cursor.execute(query, **(execute_options or {}))
            arrow_table = cursor.fetch_arrow_table()

        return _apply_schema_overrides(
            cast(pl.DataFrame, pl.from_arrow(arrow_table)),
            schema_overrides,
        )

    def from_table(
        self,
        table_schema: str,
        table_name: str,
        schema_overrides: Mapping[str, pl.DataType] | None = None,
        time_hint: TimeHint | None = None,
    ) -> pl.DataFrame:
        table_sql = _qualified_table_name(table_schema, table_name)
        hint_clause = system_time_hint_clause(time_hint)
        query = f"SELECT * FROM {table_sql}"
        if hint_clause:
            query = f"{query} {hint_clause}"

        return self.from_raw_sql(
            query,
            schema_overrides,
        )

    table_query = XtdbDataframeOps.table_query

    def table_insert(
        self,
        df: pl.DataFrame,
        table_schema: str,
        table_name: str,
        table_config: TableConfig | None = None,
        update_time: datetime | None = None,
        force_type_coercion: bool = False,
    ) -> int:
        from .xtdb_arrow import (
            _apply_xtdb_configured_column_dtypes,
            _iter_xtdb_insert_chunks,
            _normalize_xtdb_ingest_arrow,
            _prepare_xtdb_insert_dataframe,
        )

        if update_time is not None:
            raise NotImplementedError(
                "XTDB ADBC dataframe ingest does not yet support transaction "
                "SYSTEM_TIME; use the pgwire dataframe path for update_time"
            )

        table_schema = _validate_identifier(table_schema)
        table_name = _validate_identifier(table_name)
        if df.is_empty():
            return 0

        df = _prepare_xtdb_insert_dataframe(df, table_config)
        df = _apply_xtdb_configured_column_dtypes(
            df,
            table_config,
            force_type_coercion=force_type_coercion,
        )
        for chunk in _iter_xtdb_insert_chunks(df, self.max_rows_per_insert):
            arrow_table = _normalize_xtdb_ingest_arrow(chunk.to_arrow())
            with self.connection.cursor() as cursor:
                cursor.adbc_ingest(
                    table_name,
                    arrow_table,
                    mode="create_append",
                    db_schema_name=table_schema,
                )
        return df.height
