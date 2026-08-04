from sqlalchemy import Column, Integer, MetaData, Table, create_engine, text

from polars_hist_db.config.table import TableColumnConfig, TableConfig
from polars_hist_db.core.table import TableOps
from polars_hist_db.core.table_config import TableConfigOps


def test_table_metadata_is_cached_per_connection_and_can_be_invalidated():
    engine = create_engine("sqlite:///:memory:")
    with engine.connect() as connection:
        Table("items", MetaData(), Column("id", Integer)).create(connection)
        ops = TableOps("main", "items", connection)

        first = ops.get_table_metadata()
        assert ops.get_table_metadata() is first

        connection.execute(text("ALTER TABLE items ADD COLUMN value INTEGER"))
        assert "value" not in ops.get_table_metadata().columns

        TableOps.invalidate_metadata(connection, "main", "items")
        refreshed = ops.get_table_metadata()
        assert refreshed is not first
        assert "value" in refreshed.columns


def test_table_config_ddl_invalidates_cached_metadata():
    engine = create_engine("sqlite:///:memory:")
    with engine.connect() as connection:
        Table("items", MetaData(), Column("id", Integer)).create(connection)
        ops = TableOps("main", "items", connection)
        cached = ops.get_table_metadata()
        config = TableConfig(
            "items",
            "main",
            [
                TableColumnConfig("items", "id", "INTEGER"),
                TableColumnConfig("items", "value", "INTEGER"),
            ],
        )

        TableConfigOps(connection)._add_missing_columns(ops, config)

        refreshed = ops.get_table_metadata()
        assert refreshed is not cached
        assert "value" in refreshed.columns
