import polars as pl
from sqlalchemy import MetaData, Table
from sqlalchemy import types as sa_types
from sqlalchemy.dialects import mysql
from sqlalchemy.schema import CreateTable

from polars_hist_db.backends.xtdb import _xtdb_sql_literal
from polars_hist_db.backends.xtdb_transport import _xtdb_cast_type
from polars_hist_db.overrides import (
    ArrowOverrideStoreConfig,
    build_arrow_override_table_configs,
)
from polars_hist_db.types import PolarsType, SQLAlchemyType


def test_arrow_store_uses_typed_columns_and_normalized_repeating_values() -> None:
    heads, operations, references, string_lists = build_arrow_override_table_configs(
        ArrowOverrideStoreConfig(
            schema="configured",
            heads_table="heads",
            operations_table="operations",
            references_table="references",
            string_list_values_table="string_values",
        )
    )
    operation_types = {column.name: column.data_type for column in operations.columns}

    assert heads.schema == operations.schema == "configured"
    assert operation_types["operation_id"] == "BINARY(16)"
    assert operation_types["payload_hash"] == "BINARY(32)"
    assert operation_types["value_decimal"] == "DECIMAL(38,12)"
    assert operation_types["value_float"] == "DOUBLE"
    assert operation_types["value_timestamp"] == "TIMESTAMP WITH TIME ZONE"
    assert operation_types["value_date"] == "DATE"
    assert operation_types["value_time"] == "TIME"
    assert operation_types["value_duration_us"] == "BIGINT"
    assert operation_types["value_binary"] == "BLOB"
    assert references.primary_keys == (
        "operation_id",
        "reference_kind",
        "ordinal",
    )
    assert string_lists.primary_keys == ("operation_id", "value_role", "ordinal")
    assert all("JSON" not in column.data_type for column in operations.columns)


def test_binary_types_are_first_class_in_table_contracts() -> None:
    assert PolarsType.from_sql("BINARY(16)") == pl.Binary
    assert PolarsType.from_sql("VARBINARY(255)") == pl.Binary
    assert PolarsType.from_sql("BLOB") == pl.Binary
    binary = SQLAlchemyType.from_sql("BINARY(16)")
    varbinary = SQLAlchemyType.from_sql("VARBINARY(255)")
    assert isinstance(binary, sa_types.BINARY)
    assert isinstance(varbinary, sa_types.VARBINARY)
    assert binary.length == 16
    assert varbinary.length == 255


def test_arrow_store_tables_compile_for_mariadb() -> None:
    configs = build_arrow_override_table_configs(ArrowOverrideStoreConfig())

    statements = [
        str(
            CreateTable(
                Table(
                    config.name,
                    MetaData(schema=config.schema),
                    *config.build_sqlalchemy_columns(is_delta_table=False),
                )
            ).compile(dialect=mysql.dialect())
        )
        for config in configs
    ]

    assert all("JSON" not in statement for statement in statements)
    assert "BINARY(16)" in statements[1]
    assert "NUMERIC(38, 12)" in statements[1]
    assert "DATETIME" not in statements[1]


def test_xtdb_preserves_binary_storage_as_varbinary() -> None:
    assert _xtdb_cast_type("BINARY(16)") == "VARBINARY"
    assert _xtdb_cast_type("BLOB") == "VARBINARY"
    assert _xtdb_sql_literal(b"\x01\xaf", "VARBINARY") == "X('01af')::VARBINARY"
