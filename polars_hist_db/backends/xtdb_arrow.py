import json
from collections.abc import Iterable, Mapping
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import polars as pl
import pyarrow as pa

from ..config import TableConfig
from ..types import PolarsType, is_polars_type


def _xtdb_physical_column_name(column_name: str) -> str:
    return column_name.replace(".", "_").replace("/", "_").replace("-", "_").lower()


def _xtdb_physical_column_map(table_config: TableConfig) -> dict[str, str]:
    column_map = {}
    reverse_map: dict[str, str] = {}
    for column in table_config.columns:
        physical_name = _xtdb_physical_column_name(column.name)
        existing_logical_name = reverse_map.get(physical_name)
        if existing_logical_name is not None and existing_logical_name != column.name:
            raise ValueError(
                "XTDB physical column name collision: "
                f"{existing_logical_name!r} and {column.name!r} both map to "
                f"{physical_name!r}"
            )
        column_map[column.name] = physical_name
        reverse_map[physical_name] = column.name
    return column_map


def _restore_xtdb_logical_columns(
    df: pl.DataFrame,
    table_config: TableConfig | None,
) -> pl.DataFrame:
    if table_config is None:
        return df

    rename_map = {
        physical: logical
        for logical, physical in _xtdb_physical_column_map(table_config).items()
        if physical != logical and physical in df.columns
    }
    if not rename_map:
        return df
    return df.rename(rename_map)


def _apply_schema_overrides(
    df: pl.DataFrame,
    schema_overrides: Mapping[str, pl.DataType] | None,
) -> pl.DataFrame:
    if not schema_overrides:
        return df

    casts = {
        column: dtype
        for column, dtype in schema_overrides.items()
        if column in df.columns
    }
    if not casts:
        return df
    return df.with_columns(
        pl.col(column).cast(dtype) for column, dtype in casts.items()
    )


def _normalize_xtdb_ingest_arrow(table: pa.Table) -> pa.Table:
    for index, field in enumerate(table.schema):
        if (
            pa.types.is_large_string(field.type)
            or pa.types.is_dictionary(field.type)
            and pa.types.is_large_string(field.type.value_type)
        ):
            table = table.set_column(
                index,
                field.with_type(pa.string()),
                table.column(field.name).cast(pa.string()),
            )
    return table


def _xtdb_document_id_columns(table_config: TableConfig) -> list[str]:
    primary_keys = list(table_config.primary_keys)
    configured_columns = [column.name for column in table_config.columns]

    if "_id" in configured_columns:
        return ["_id"]
    if primary_keys:
        return primary_keys

    raise ValueError(
        "XTDB backend requires at least one primary key or an explicit _id column"
    )


_XTDB_NATIVE_ID_CAST_TYPES = {"TEXT", "INTEGER", "BIGINT"}


def _xtdb_document_id_cast_type(table_config: TableConfig) -> str:
    document_id_columns = _xtdb_document_id_columns(table_config)
    if len(document_id_columns) > 1:
        return "TEXT"
    source_key = document_id_columns[0]
    source_type = next(
        column.data_type for column in table_config.columns if column.name == source_key
    )
    cast_type = _xtdb_cast_type(source_type)
    if document_id_columns == ["_id"] and cast_type not in _XTDB_NATIVE_ID_CAST_TYPES:
        raise ValueError(
            "XTDB explicit _id columns must be string or integer typed; "
            f"received {source_type}"
        )
    return cast_type if cast_type in _XTDB_NATIVE_ID_CAST_TYPES else "TEXT"


def _xtdb_document_id_is_encoded(table_config: TableConfig) -> bool:
    document_id_columns = _xtdb_document_id_columns(table_config)
    if document_id_columns == ["_id"]:
        return False
    if len(document_id_columns) > 1:
        return True
    source_type = next(
        column.data_type
        for column in table_config.columns
        if column.name == document_id_columns[0]
    )
    return _xtdb_cast_type(source_type) not in _XTDB_NATIVE_ID_CAST_TYPES


def _xtdb_document_id_value(table_config: TableConfig, row: Mapping[str, Any]) -> Any:
    document_id_columns = _xtdb_document_id_columns(table_config)
    if not _xtdb_document_id_is_encoded(table_config):
        return row[document_id_columns[0]]
    return _xtdb_composite_document_id(
        document_id_columns,
        tuple(row[column] for column in document_id_columns),
    )


def _xtdb_json_safe_key_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"binary_hex": value.hex()}
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return value


def _xtdb_composite_document_id(
    primary_keys: list[str],
    row: tuple[Any, ...],
) -> str:
    encoded_parts = [
        [key, _xtdb_json_safe_key_value(value)]
        for key, value in zip(primary_keys, row, strict=True)
    ]
    return "xtdb-pk-v1:" + json.dumps(
        encoded_parts,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def _xtdb_cast_type(data_type: str) -> str:
    normalized = data_type.upper()
    if normalized.startswith(("BINARY", "VARBINARY")) or normalized.endswith("BLOB"):
        return "VARBINARY"
    if normalized.startswith(("VARCHAR", "CHAR")) or "TEXT" in normalized:
        return "TEXT"
    if normalized in {"JSON", "JSONB"}:
        return "TEXT"
    if normalized in {"DOUBLE", "DOUBLE PRECISION"}:
        return "DOUBLE PRECISION"
    if normalized == "FLOAT":
        return "FLOAT"
    if normalized == "REAL":
        # MariaDB REAL is DOUBLE PRECISION unless REAL_AS_FLOAT is enabled.
        # The package contract likewise exposes REAL as Polars Float64.
        return "DOUBLE PRECISION"
    if normalized in {"INT", "INTEGER"}:
        return "INTEGER"
    if normalized in {"BOOL", "BOOLEAN"}:
        return "BOOLEAN"
    if normalized in {"BIT", "TINYINT", "SMALLINT", "MEDIUMINT"}:
        return "INTEGER"
    if normalized in {"BIGINT", "DATE", "TIME"}:
        return normalized
    if normalized.startswith("DATETIME"):
        return "TIMESTAMP WITH TIME ZONE"
    if normalized.startswith("TIMESTAMPTZ"):
        return "TIMESTAMP WITH TIME ZONE"
    if normalized.startswith(("DECIMAL", "NUMERIC")):
        return normalized
    if normalized.startswith("TIMESTAMP"):
        return normalized
    raise ValueError(f"Unsupported XTDB column type: {data_type}")


def _xtdb_cast_type_from_polars(dtype: pl.DataType) -> str:
    if is_polars_type(dtype, pl.Int8, pl.Int16, pl.Int32):
        return "INTEGER"
    if dtype == pl.Int64:
        return "BIGINT"
    if dtype == pl.Float32:
        return "FLOAT"
    if dtype == pl.Float64:
        return "DOUBLE PRECISION"
    if dtype == pl.Boolean:
        return "BOOLEAN"
    if dtype == pl.Date:
        return "DATE"
    if dtype == pl.Time:
        return "TIME"
    if isinstance(dtype, pl.Datetime):
        return (
            "TIMESTAMP WITH TIME ZONE" if dtype.time_zone is not None else "TIMESTAMP"
        )
    if isinstance(dtype, pl.Decimal):
        return f"DECIMAL({dtype.precision},{dtype.scale})"
    if dtype == pl.Binary:
        return "VARBINARY"
    if is_polars_type(dtype, pl.String, pl.Utf8, pl.Categorical):
        return "TEXT"
    raise ValueError(
        f"Unsupported XTDB Polars type {dtype}; provide a supported typed "
        "DataFrame or an explicit TableConfig"
    )


def _xtdb_insert_casts(
    df: pl.DataFrame,
    table_config: TableConfig | None,
) -> list[str]:
    configured_types = {}
    if table_config is not None:
        document_id_columns = _xtdb_document_id_columns(table_config)
        if document_id_columns != ["_id"]:
            configured_types["_id"] = _xtdb_document_id_cast_type(table_config)
        for column in table_config.columns:
            configured_types[column.name] = _xtdb_cast_type(column.data_type)

    casts = []
    for name, dtype in df.schema.items():
        casts.append(
            configured_types[name]
            if name in configured_types
            else _xtdb_cast_type_from_polars(dtype)
        )
    return casts


def _xtdb_polars_type_or_none(data_type: str) -> pl.DataType | None:
    normalized = data_type.upper()
    if normalized.startswith(("DATETIME", "TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE")):
        return pl.Datetime("us", "UTC")
    try:
        return PolarsType.from_sql(data_type)
    except ValueError:
        return None


def _xtdb_configured_column_dtypes(
    table_config: TableConfig,
) -> dict[str, pl.DataType]:
    document_id_columns = _xtdb_document_id_columns(table_config)
    dtypes: dict[str, pl.DataType] = {}

    for column in table_config.columns:
        dtype = _xtdb_polars_type_or_none(column.data_type)
        if dtype is None:
            continue
        dtypes[column.name] = dtype
    if document_id_columns != ["_id"]:
        dtypes["_id"] = (
            _xtdb_polars_type_or_none(_xtdb_document_id_cast_type(table_config))
            or pl.String()
        )
    dtypes["_valid_from"] = pl.Datetime("us", "UTC")
    dtypes["_valid_to"] = pl.Datetime("us", "UTC")
    return dtypes


def _xtdb_physical_configured_column_dtypes(
    table_config: TableConfig,
) -> dict[str, pl.DataType]:
    physical_column_map = _xtdb_physical_column_map(table_config)
    return {
        physical_column_map.get(column, _xtdb_physical_column_name(column)): dtype
        for column, dtype in _xtdb_configured_column_dtypes(table_config).items()
    }


def _apply_xtdb_configured_column_dtypes(
    df: pl.DataFrame,
    table_config: TableConfig | None,
    *,
    force_type_coercion: bool = False,
) -> pl.DataFrame:
    if table_config is None:
        return df

    return PolarsType.enforce_database_schema(
        df,
        _xtdb_configured_column_dtypes(table_config),
        backend="xtdb",
        operation="table_insert",
        force_type_coercion=force_type_coercion,
    )


def _iter_xtdb_insert_chunks(
    df: pl.DataFrame,
    max_rows_per_insert: int | None,
) -> Iterable[pl.DataFrame]:
    if max_rows_per_insert is None or df.height <= max_rows_per_insert:
        yield df
        return

    for offset in range(0, df.height, max_rows_per_insert):
        yield df.slice(offset, max_rows_per_insert)


def _prepare_xtdb_insert_dataframe(
    df: pl.DataFrame,
    table_config: TableConfig | None,
) -> pl.DataFrame:
    if table_config is None:
        return df

    document_id_columns = _xtdb_document_id_columns(table_config)
    if document_id_columns == ["_id"]:
        return df
    if "_id" in df.columns:
        raise ValueError(
            "XTDB insert dataframe already contains _id and cannot also map "
            "configured primary keys to _id"
        )
    missing_keys = [key for key in document_id_columns if key not in df.columns]
    if missing_keys:
        raise ValueError(
            "XTDB insert dataframe is missing configured primary key columns "
            f"{missing_keys!r}"
        )

    if not _xtdb_document_id_is_encoded(table_config):
        return df.with_columns(pl.col(document_id_columns[0]).alias("_id")).select(
            ["_id", *df.columns]
        )

    document_ids = [
        _xtdb_composite_document_id(document_id_columns, row)
        for row in df.select(document_id_columns).iter_rows()
    ]

    return df.with_columns(pl.Series("_id", document_ids)).select(["_id", *df.columns])
