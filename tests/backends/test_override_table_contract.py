from sqlalchemy.dialects import mysql

from polars_hist_db.overrides import (
    OverrideLedgerConfig,
    build_override_table_config,
    build_override_valid_time_config,
)
from polars_hist_db.types import SQLAlchemyType


def test_override_table_config_builds_sqlalchemy_columns_for_mariadb_path():
    table_config = build_override_table_config(OverrideLedgerConfig())

    columns = table_config.build_sqlalchemy_columns(is_delta_table=False)

    assert {column.name for column in columns} >= {
        "operation_id",
        "owner_user_id",
        "field_path",
        "valid_from",
        "valid_to",
        "metadata_json",
        "format_version",
        "layer_id",
        "actor_id",
        "supersedes_operation_ids_json",
        "removes_operation_ids_json",
        "recorded_at",
        "payload_hash",
    }
    operation_id = next(column for column in columns if column.name == "operation_id")
    assert operation_id.primary_key is True
    assert {
        column.name: column.type.fsp
        for column in columns
        if isinstance(column.type, mysql.DATETIME)
    } == {"valid_from": 6, "valid_to": 6, "recorded_at": 6}


def test_mariadb_datetime_without_precision_remains_supported():
    assert repr(SQLAlchemyType.from_sql("DATETIME()")) == "DATETIME()"


def test_override_valid_time_config_targets_business_window_for_xtdb_path():
    config = OverrideLedgerConfig()
    table_config = build_override_table_config(config)
    valid_time = build_override_valid_time_config(config)

    assert valid_time.matches(table_config.schema, table_config.name)
    assert valid_time.from_column == "valid_from"
    assert valid_time.to_column == "valid_to"
    assert {"valid_from", "valid_to"}.issubset(
        {column.name for column in table_config.columns}
    )
