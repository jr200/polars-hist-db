import json
import logging
import re
from typing import Any

import polars as pl

from ..config import ForeignKeyConfig, TableColumnConfig, TableConfig
from ..observability import record_database_type_contract
from ..types import TypeContractError
from .xtdb_arrow import (
    _xtdb_cast_type,
    _xtdb_document_id_cast_type,
    _xtdb_document_id_columns,
    _xtdb_document_id_is_encoded,
    _xtdb_physical_column_map,
)
from .xtdb_transport import (
    _execute_xtdb_dml,
    _qualified_table_name,
    _validate_identifier,
    _xtdb_column_identifier,
    _xtdb_sql_literal,
)

LOGGER = logging.getLogger(__name__)

_XTDB_SYSTEM_COLUMNS = {"_valid_from", "_valid_to", "_system_from", "_system_to"}
_XTDB_TABLE_CONFIG_METADATA_TABLE = "__polars_hist_db_xtdb_table_configs"


def _unwrap_xtdb_optional_type(data_type: str) -> str:
    normalized = data_type.upper().strip()
    if normalized.startswith("[:?") and normalized.endswith("]"):
        return normalized[3:-1].strip()
    return normalized


_XTDB_PHYSICAL_TYPE_FAMILIES = {
    ":NULL": "NULL",
    ":NOTHING": "NOTHING",
    ":BOOL": "BOOLEAN",
    ":I8": "INTEGER",
    ":I16": "INTEGER",
    ":I32": "INTEGER",
    ":I64": "BIGINT",
    ":F32": "FLOAT",
    ":F64": "DOUBLE PRECISION",
    ":UTF8": "TEXT",
    ":VARBINARY": "VARBINARY",
    ":FIXED-SIZE-BINARY": "FIXED-SIZE-BINARY",
    ":DECIMAL": "DECIMAL",
    ":INSTANT": "TIMESTAMP WITH TIME ZONE",
    ":TIMESTAMP-TZ": "TIMESTAMP WITH TIME ZONE",
    ":TIMESTAMP-LOCAL": "TIMESTAMP",
    ":DATE": "DATE",
    ":TIME-LOCAL": "TIME",
    ":DURATION": "DURATION",
    ":INTERVAL": "INTERVAL",
    ":TSTZ-RANGE": "TSTZ-RANGE",
    ":KEYWORD": "KEYWORD",
    ":OID": "OID",
    ":REGCLASS": "REGCLASS",
    ":REGPROC": "REGPROC",
    ":UUID": "UUID",
    ":URI": "URI",
    ":TRANSIT": "TRANSIT",
    ":LIST": "LIST",
    ":FIXED-SIZE-LIST": "FIXED-SIZE-LIST",
    ":SET": "SET",
    ":MAP": "MAP",
    ":STRUCT": "STRUCT",
}


def _xtdb_type_head(data_type: str) -> str:
    match = re.match(r"^\[?\s*(:[A-Z0-9?-]+)", data_type)
    if match is None:
        raise ValueError(f"Unsupported XTDB column type: {data_type}")
    return match.group(1)


def _split_xtdb_set_members(data_type: str) -> list[str]:
    body = data_type[2:-1].strip()
    members: list[str] = []
    start = 0
    depth = 0
    quoted = False
    escaped = False
    for idx, char in enumerate(body):
        if quoted:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                quoted = False
        elif char == '"':
            quoted = True
        elif char in "[{(":
            depth += 1
        elif char in "]})":
            depth -= 1
        elif char.isspace() and depth == 0:
            if member := body[start:idx].strip():
                members.append(member)
            start = idx + 1
    if member := body[start:].strip():
        members.append(member)
    return members


def _xtdb_union_members(data_type: str) -> list[str] | None:
    if data_type.startswith("#{") and data_type.endswith("}"):
        return _split_xtdb_set_members(data_type)
    if _xtdb_type_head(data_type) not in {":UNION", ":SPARSE-UNION"}:
        return None
    start = data_type.find("#{")
    if start < 0:
        raise ValueError(f"Unsupported XTDB union type: {data_type}")
    return _split_xtdb_set_members(data_type[start : data_type.rfind("}") + 1])


def _xtdb_type_to_config_type(data_type: str) -> str:
    normalized = _unwrap_xtdb_optional_type(data_type)
    xtdb_types = {
        ":I8": "TINYINT",
        ":I16": "SMALLINT",
        ":I32": "INT",
        ":I64": "BIGINT",
        ":F32": "FLOAT",
        ":F64": "DOUBLE",
        ":UTF8": "VARCHAR(255)",
        ":BOOL": "BOOLEAN",
        ":BOOLEAN": "BOOLEAN",
        ":TIMESTAMP": "TIMESTAMP",
    }
    if normalized in xtdb_types:
        return xtdb_types[normalized]
    if normalized.startswith(("DECIMAL", "NUMERIC")):
        return normalized

    family = _xtdb_physical_type_family(normalized)
    if family == "DECIMAL":
        precision_and_scale = re.search(r":DECIMAL\s+(\d+)\s+(-?\d+)", normalized)
        if precision_and_scale:
            return f"DECIMAL({precision_and_scale[1]},{precision_and_scale[2]})"
    config_types = {
        "TEXT": "VARCHAR(255)",
        "BOOLEAN": "BOOLEAN",
        "INTEGER": "INT",
        "BIGINT": "BIGINT",
        "FLOAT": "FLOAT",
        "DOUBLE PRECISION": "DOUBLE",
        "DATE": "DATE",
        "TIME": "TIME",
        "TIMESTAMP": "TIMESTAMP",
        "TIMESTAMP WITH TIME ZONE": "TIMESTAMP WITH TIME ZONE",
    }
    if family in config_types:
        return config_types[family]
    raise ValueError(f"Unsupported XTDB column type: {data_type}")


def _xtdb_physical_type_family(data_type: str) -> str:
    normalized = _unwrap_xtdb_optional_type(data_type)
    if (members := _xtdb_union_members(normalized)) is not None:
        families = {
            family
            for member in members
            if (family := _xtdb_physical_type_family(member)) != "NULL"
        }
        if not families:
            return "NULL"
        if len(families) != 1:
            return "UNION"
        return families.pop()
    if normalized.startswith(("DECIMAL", "NUMERIC")):
        return "DECIMAL"
    if normalized[:1].isalpha():
        return _xtdb_configured_type_family(normalized)
    head = _xtdb_type_head(normalized)
    try:
        return _XTDB_PHYSICAL_TYPE_FAMILIES[head]
    except KeyError as exc:
        raise ValueError(f"Unsupported XTDB column type: {data_type}") from exc


def _xtdb_configured_type_family(data_type: str) -> str:
    cast_type = _xtdb_cast_type(data_type)
    if cast_type.startswith(("DECIMAL", "NUMERIC")):
        return "DECIMAL"
    return cast_type


def _validate_xtdb_physical_types(
    metadata: pl.DataFrame,
    table_config: TableConfig,
) -> None:
    physical_columns = _xtdb_physical_column_map(table_config)
    expected = {
        physical_columns[column.name]: (
            _xtdb_configured_type_family(column.data_type),
            column.nullable,
        )
        for column in table_config.columns
        if column.name not in _XTDB_SYSTEM_COLUMNS
    }
    document_id_columns = _xtdb_document_id_columns(table_config)
    if document_id_columns != ["_id"]:
        expected["_id"] = (
            _xtdb_configured_type_family(_xtdb_document_id_cast_type(table_config)),
            False,
        )

    actual = {
        str(row["column_name"]): (
            _xtdb_physical_type_family(str(row["data_type"])),
            str(row["data_type"]),
        )
        for row in metadata.iter_rows(named=True)
        if str(row["column_name"]) not in _XTDB_SYSTEM_COLUMNS
    }
    for column, (expected_type, nullable) in expected.items():
        actual_type, physical_type = actual.get(column, ("MISSING", "MISSING"))
        if actual_type == expected_type or (
            nullable and actual_type in {"NULL", "MISSING"}
        ):
            continue

        record_database_type_contract(
            backend="xtdb",
            operation="schema_reflection",
            expected_type=expected_type,
            actual_type=actual_type,
            forced=False,
            outcome="rejected",
        )
        message = (
            f"XTDB physical schema does not satisfy the configured type contract "
            f"for {table_config.schema}.{table_config.name}.{column}: expected "
            f"{expected_type}, received {physical_type}"
        )
        LOGGER.error(message)
        raise TypeContractError(message)


def _is_nullable(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).upper() in {"YES", "TRUE", "1"}


def _xtdb_table_config_metadata_table(table_schema: str) -> str:
    return _qualified_table_name(table_schema, _XTDB_TABLE_CONFIG_METADATA_TABLE)


def _xtdb_declared_columns(table_config: TableConfig) -> list[str]:
    document_id_columns = _xtdb_document_id_columns(table_config)
    uses_explicit_id = document_id_columns == ["_id"]

    declared_columns = ["_id"]
    for column in table_config.columns:
        if column.name in _XTDB_SYSTEM_COLUMNS:
            continue
        if uses_explicit_id and column.name == "_id":
            continue
        declared_columns.append(column.name)

    return [_xtdb_column_identifier(column) for column in declared_columns]


def _xtdb_table_config_metadata(
    connection: Any,
    table_schema: str,
    table_name: str,
) -> pl.DataFrame | None:
    if not _xtdb_table_exists(
        connection,
        table_schema,
        _XTDB_TABLE_CONFIG_METADATA_TABLE,
    ):
        return None
    escaped_schema = table_schema.replace("'", "''")
    escaped_name = table_name.replace("'", "''")
    return pl.read_database(
        f"""
        SELECT *
        FROM {_xtdb_table_config_metadata_table(table_schema)}
        WHERE table_schema = '{escaped_schema}'
          AND table_name = '{escaped_name}'
    """,
        connection,
    )


def _xtdb_table_exists(connection: Any, table_schema: str, table_name: str) -> bool:
    table_schema = _validate_identifier(table_schema)
    table_name = _validate_identifier(table_name)
    metadata = pl.read_database(
        f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{table_schema}'
              AND table_name = '{table_name}'
        """,
        connection,
    )
    return not metadata.is_empty()


def _xtdb_primary_keys_from_metadata(
    metadata: pl.DataFrame | None,
    table_schema: str,
    table_name: str,
) -> list[str] | None:
    if (
        metadata is None
        or metadata.is_empty()
        or "primary_keys_json" not in metadata.columns
    ):
        return None

    primary_keys = json.loads(str(metadata[0, "primary_keys_json"]))
    if not isinstance(primary_keys, list) or not all(
        isinstance(key, str) for key in primary_keys
    ):
        raise ValueError(
            f"Invalid XTDB table-config metadata for {table_schema}.{table_name}"
        )
    return primary_keys


def _xtdb_table_config_from_metadata(
    metadata: pl.DataFrame | None,
    table_schema: str,
    table_name: str,
) -> TableConfig | None:
    if (
        metadata is None
        or metadata.is_empty()
        or "columns_json" not in metadata.columns
    ):
        return None

    columns_json = metadata[0, "columns_json"]
    if columns_json is None:
        return None

    raw_columns = json.loads(str(columns_json))
    if not isinstance(raw_columns, list):
        raise TypeError(f"Invalid XTDB column metadata for {table_schema}.{table_name}")

    columns = [
        TableColumnConfig(**column)
        for column in raw_columns
        if isinstance(column, dict)
    ]
    primary_keys = _xtdb_primary_keys_from_metadata(
        metadata,
        table_schema,
        table_name,
    )
    foreign_keys: list[ForeignKeyConfig] = []
    if "foreign_keys_json" in metadata.columns:
        foreign_keys_json = metadata[0, "foreign_keys_json"]
        if foreign_keys_json is not None:
            raw_foreign_keys = json.loads(str(foreign_keys_json))
            if not isinstance(raw_foreign_keys, list):
                raise ValueError(
                    f"Invalid XTDB foreign-key metadata for {table_schema}.{table_name}"
                )
            foreign_keys = [
                ForeignKeyConfig(**foreign_key)
                for foreign_key in raw_foreign_keys
                if isinstance(foreign_key, dict)
            ]
    return TableConfig(
        name=table_name,
        schema=table_schema,
        columns=columns,
        foreign_keys=foreign_keys,
        primary_keys=primary_keys or [],
        is_temporal=(
            bool(metadata[0, "is_temporal"])
            if "is_temporal" in metadata.columns
            and metadata[0, "is_temporal"] is not None
            else True
        ),
    )


def _xtdb_foreign_keys_json(table_config: TableConfig) -> str:
    foreign_keys = []
    for foreign_key in table_config.foreign_keys:
        ref_table = foreign_key.references.table
        ref_table_name = (
            ref_table.name if isinstance(ref_table, TableConfig) else str(ref_table)
        )
        foreign_keys.append(
            {
                "name": foreign_key.name,
                "references": {
                    "schema": foreign_key.references.schema,
                    "table": ref_table_name,
                    "column": foreign_key.references.column,
                },
            }
        )
    return json.dumps(foreign_keys, separators=(",", ":"))


def _xtdb_id_policy(table_config: TableConfig) -> str:
    document_id_columns = _xtdb_document_id_columns(table_config)
    if document_id_columns == ["_id"]:
        return "explicit-id"
    if len(document_id_columns) == 1 and not _xtdb_document_id_is_encoded(table_config):
        return "single-key"
    return "xtdb-pk-v1"


class XtdbTableConfigOps:
    def __init__(self, connection: Any):
        self.connection = connection

    def create_all(self, tcs: Any) -> None:
        for table_config in tcs.items:
            self.create(table_config)

    def drop_all(self, tcs: Any) -> None:
        for table_config in reversed(tcs.items):
            self.drop(table_config)

    def drop(self, table_config: TableConfig) -> None:
        data_table_exists = self.table_exists(table_config.schema, table_config.name)

        if data_table_exists:
            table_name = _qualified_table_name(table_config.schema, table_config.name)
            _execute_xtdb_dml(self.connection, f"ERASE FROM {table_name} WHERE TRUE")

        if self.table_exists(table_config.schema, _XTDB_TABLE_CONFIG_METADATA_TABLE):
            metadata_table = _xtdb_table_config_metadata_table(table_config.schema)
            metadata_id = _xtdb_sql_literal(
                f"{table_config.schema}.{table_config.name}", "TEXT"
            )
            _execute_xtdb_dml(
                self.connection,
                f"DELETE FROM {metadata_table} WHERE _id = {metadata_id}",
            )

    def create(
        self,
        table_config: TableConfig,
        *args,
        **kwargs,
    ) -> TableConfig:
        if self.table_exists(table_config.schema, table_config.name):
            return self.from_table(table_config.schema, table_config.name)

        self._record_table_config_metadata(table_config)
        return table_config

    def _record_table_config_metadata(self, table_config: TableConfig) -> None:
        primary_keys_json = json.dumps(
            list(table_config.primary_keys),
            separators=(",", ":"),
        )
        columns_json = json.dumps(
            [column.__dict__ for column in table_config.columns],
            separators=(",", ":"),
        )
        values = [
            f"{table_config.schema}.{table_config.name}",
            table_config.schema,
            table_config.name,
            primary_keys_json,
            _xtdb_id_policy(table_config),
            columns_json,
            _xtdb_foreign_keys_json(table_config),
        ]
        values_sql = ", ".join(_xtdb_sql_literal(value, "TEXT") for value in values)
        values_sql = (
            f"{values_sql}, {_xtdb_sql_literal(table_config.is_temporal, 'BOOLEAN')}"
        )
        metadata_table = _xtdb_table_config_metadata_table(table_config.schema)
        _execute_xtdb_dml(
            self.connection,
            f"INSERT INTO {metadata_table} "
            "(_id, table_schema, table_name, primary_keys_json, "
            "id_policy, columns_json, foreign_keys_json, is_temporal) "
            f"VALUES ({values_sql})",
        )

    def table_exists(self, table_schema: str, table_name: str) -> bool:
        return _xtdb_table_exists(self.connection, table_schema, table_name)

    def from_table(self, table_schema: str, table_name: str) -> TableConfig:
        table_schema = _validate_identifier(table_schema)
        table_name = _validate_identifier(table_name)
        metadata = pl.read_database(
            f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = '{table_schema}'
              AND table_name = '{table_name}'
            ORDER BY ordinal_position
        """,
            self.connection,
        )
        config_metadata = _xtdb_table_config_metadata(
            self.connection,
            table_schema,
            table_name,
        )
        configured_table = _xtdb_table_config_from_metadata(
            config_metadata,
            table_schema,
            table_name,
        )
        if configured_table is not None:
            if not metadata.is_empty():
                _validate_xtdb_physical_types(metadata, configured_table)
            return configured_table

        if metadata.is_empty():
            raise ValueError(
                f"XTDB table metadata not found for {table_schema}.{table_name}"
            )

        columns = []
        primary_keys = []
        for row in metadata.iter_rows(named=True):
            col_name = str(row["column_name"])
            if col_name in _XTDB_SYSTEM_COLUMNS:
                continue
            if col_name == "_id":
                primary_keys.append(col_name)

            columns.append(
                TableColumnConfig(
                    table=table_name,
                    name=col_name,
                    data_type=_xtdb_type_to_config_type(str(row["data_type"])),
                    nullable=_is_nullable(row["is_nullable"]),
                )
            )

        configured_primary_keys = _xtdb_primary_keys_from_metadata(
            config_metadata,
            table_schema,
            table_name,
        )
        if configured_primary_keys is not None:
            primary_keys = configured_primary_keys

        return TableConfig(
            name=table_name,
            schema=table_schema,
            columns=columns,
            primary_keys=primary_keys,
            is_temporal=True,
        )
