from datetime import UTC, datetime, timezone

import pytest
from sqlalchemy import Column, DateTime, Integer, MetaData, String, Table, create_engine

from polars_hist_db.config import DatasetConfig, TableConfigs
from polars_hist_db.dataset.primary_item import (
    _validate_sql_valid_time_source_table,
    scrape_primary_item,
)


def _source_table(connection):
    metadata = MetaData()
    table = Table(
        "stage_events",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("event_value", String(64)),
        Column("event_timestamp", DateTime(timezone=True)),
        Column("valid_until", DateTime(timezone=True)),
    )
    metadata.create_all(connection)
    return table


@pytest.fixture
def connection():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        yield conn


def test_sql_valid_time_source_table_accepts_non_null_columns(connection):
    table = _source_table(connection)
    connection.execute(
        table.insert(),
        {
            "id": 1,
            "event_value": "Alpha",
            "event_timestamp": datetime(2030, 1, 1, tzinfo=UTC),
            "valid_until": datetime(2030, 2, 1, tzinfo=UTC),
        },
    )

    _validate_sql_valid_time_source_table(
        connection,
        table,
        ["event_timestamp", "valid_until"],
    )


def test_sql_valid_time_source_table_rejects_missing_columns(connection):
    table = _source_table(connection)

    with pytest.raises(ValueError, match="missing source column.*event_timestamp"):
        _validate_sql_valid_time_source_table(
            connection,
            table,
            ["event_timestamp_missing"],
        )


def test_sql_valid_time_source_table_rejects_null_values(connection):
    table = _source_table(connection)
    connection.execute(
        table.insert(),
        {
            "id": 1,
            "event_value": "Alpha",
            "event_timestamp": None,
            "valid_until": datetime(2030, 2, 1, tzinfo=UTC),
        },
    )

    with pytest.raises(ValueError, match="null source value.*event_timestamp"):
        _validate_sql_valid_time_source_table(
            connection,
            table,
            ["event_timestamp", "valid_until"],
        )


def test_mariadb_primary_ingest_rejects_null_valid_time_source(monkeypatch, connection):
    table = _source_table(connection)
    connection.execute(
        table.insert(),
        {
            "id": 1,
            "event_value": "Alpha",
            "event_timestamp": None,
        },
    )
    dataset = DatasetConfig(
        name="stage_events",
        delta_table_schema="source",
        input_config={"type": "dsv", "search_paths": []},
        valid_time=[
            {
                "schema": "source",
                "table": "events",
                "from_column": "event_timestamp",
            }
        ],
        pipeline=[
            {
                "schema": "source",
                "table": "events",
                "type": "primary",
                "columns": [
                    {"source": "id", "target": "id"},
                    {"source": "event_value", "target": "event_value"},
                ],
            }
        ],
    )
    tables = TableConfigs(
        items=[
            {
                "schema": "source",
                "name": "events",
                "primary_keys": ["id"],
                "columns": [
                    {"name": "id", "data_type": "INT", "nullable": False},
                    {"name": "event_value", "data_type": "VARCHAR(64)"},
                ],
                "is_temporal": True,
            }
        ]
    )

    class _FakeTableConfigOps:
        def __init__(self, _connection):
            pass

        def create(self, _table_config):
            return _table_config

    class _FakeTableOps:
        def __init__(self, *_args):
            pass

        def get_table_metadata(self):
            return table

        def get_column_intersection(self, _selected_columns):
            raise AssertionError("valid_time validation should fail before upsert")

    monkeypatch.setattr(
        "polars_hist_db.dataset.primary_item.TableConfigOps", _FakeTableConfigOps
    )
    monkeypatch.setattr("polars_hist_db.dataset.primary_item.TableOps", _FakeTableOps)

    with pytest.raises(ValueError, match="null source value.*event_timestamp"):
        scrape_primary_item(
            0,
            dataset,
            tables,
            datetime(2030, 1, 1, tzinfo=UTC),
            connection,
        )
