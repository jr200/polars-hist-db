from datetime import UTC, datetime, timezone
from types import SimpleNamespace

import polars as pl

from polars_hist_db.config import (
    DatasetConfig,
    TableConfigs,
    ValidTimeConfig,
)
from polars_hist_db.dataset.extract_item import scrape_extract_item
from polars_hist_db.dataset.primary_item import scrape_primary_item


class _FakeXtdbBackend:
    def __init__(self):
        self.temporal_upsert_calls = []

    def temporal_upsert(self, *args, **kwargs):
        self.temporal_upsert_calls.append((args, kwargs))
        return 1


class _FakeStagingOps:
    def __init__(self, dataframe):
        self.dataframe = dataframe
        self.prepare_calls = []
        self.bulk_dataframe_ops = object()

    def prepare_pipeline_item_dataframe(self, *args, **kwargs):
        self.prepare_calls.append((args, kwargs))
        return self.dataframe

    def bulk_dataframes(self):
        return self.bulk_dataframe_ops


def test_xtdb_primary_ingest_uses_staged_dataframe_for_temporal_upsert():
    tables = TableConfigs(
        items=[
            {
                "schema": "fakedata",
                "name": "records",
                "primary_keys": ["record_id"],
                "columns": [
                    {"name": "record_id", "data_type": "INT", "nullable": False},
                    {"name": "destination", "data_type": "VARCHAR(64)"},
                ],
                "is_temporal": True,
            }
        ]
    )
    record_table = tables["records"]
    dataset = DatasetConfig(
        name="record_stream",
        delta_table_schema="fakedata",
        input_config={"type": "dsv", "search_paths": []},
        valid_time=[
            {
                "schema": "fakedata",
                "table": "records",
                "from_column": "msg_timestamp",
            }
        ],
        pipeline=[
            {
                "schema": "fakedata",
                "table": "records",
                "type": "primary",
                "columns": [
                    {"source": "record_id", "target": "record_id"},
                    {"source": "destination_name", "target": "destination"},
                ],
            }
        ],
    )
    msg_timestamp = datetime(2030, 1, 1, 12, 0, tzinfo=UTC)
    update_time = datetime(2030, 1, 1, 12, 5, tzinfo=UTC)
    staged_df = pl.DataFrame(
        {
            "record_id": [1],
            "destination": ["Alpha"],
            "msg_timestamp": [msg_timestamp],
        }
    )
    backend = _FakeXtdbBackend()
    staging = _FakeStagingOps(staged_df)

    did_modify = scrape_primary_item(
        0,
        dataset,
        tables,
        update_time,
        SimpleNamespace(),
        stage_run_id="stage-1",
        staging=staging,
        backend=backend,
    )

    assert did_modify is True
    assert staging.prepare_calls == [
        (
            ("stage-1", dataset, 0, record_table),
            {
                "valid_time": ValidTimeConfig(
                    schema="fakedata",
                    table="records",
                    from_column="msg_timestamp",
                )
            },
        )
    ]
    args, kwargs = backend.temporal_upsert_calls[0]
    assert args[1:] == ("fakedata", "records")
    assert args[0].to_dict(as_series=False) == {
        "record_id": [1],
        "destination": ["Alpha"],
        "msg_timestamp": [msg_timestamp],
    }
    assert kwargs["table_config"] == record_table
    assert kwargs["delta_config"] == dataset.delta_config
    assert kwargs["update_time"] == update_time
    assert kwargs["valid_time"] == ValidTimeConfig(
        schema="fakedata",
        table="records",
        from_column="msg_timestamp",
    )


def test_xtdb_primary_ingest_uses_bulk_dataframe_ops_for_non_temporal_table():
    tables = TableConfigs(
        items=[
            {
                "schema": "fakedata",
                "name": "records",
                "primary_keys": ["record_id"],
                "columns": [
                    {"name": "record_id", "data_type": "INT", "nullable": False},
                    {"name": "destination", "data_type": "VARCHAR(64)"},
                ],
                "is_temporal": False,
            }
        ]
    )
    dataset = DatasetConfig(
        name="record_stream",
        delta_table_schema="fakedata",
        input_config={"type": "dsv", "search_paths": []},
        pipeline=[
            {
                "schema": "fakedata",
                "table": "records",
                "type": "primary",
                "columns": [
                    {"source": "record_id", "target": "record_id"},
                    {"source": "destination_name", "target": "destination"},
                ],
            }
        ],
    )
    update_time = datetime(2030, 1, 1, 12, 5, tzinfo=UTC)
    staged_df = pl.DataFrame(
        {
            "record_id": [1],
            "destination": ["Alpha"],
        }
    )
    backend = _FakeXtdbBackend()
    staging = _FakeStagingOps(staged_df)

    did_modify = scrape_primary_item(
        0,
        dataset,
        tables,
        update_time,
        SimpleNamespace(),
        stage_run_id="stage-1",
        staging=staging,
        backend=backend,
    )

    assert did_modify is True
    args, kwargs = backend.temporal_upsert_calls[0]
    assert args[1:] == ("fakedata", "records")
    assert kwargs["dataframe_ops"] is staging.bulk_dataframe_ops
    assert "update_time" not in kwargs
    assert kwargs["valid_time"] is None


def test_xtdb_extract_ingest_uses_bulk_dataframe_ops_for_non_temporal_table():
    tables = TableConfigs(
        items=[
            {
                "schema": "source_a",
                "name": "vectors",
                "primary_keys": ["entity_id"],
                "columns": [
                    {"name": "entity_id", "data_type": "INT", "nullable": False},
                    {"name": "latitude", "data_type": "DOUBLE"},
                ],
                "is_temporal": True,
            },
            {
                "schema": "source_a",
                "name": "entity_info",
                "primary_keys": ["entity_id"],
                "columns": [
                    {"name": "entity_id", "data_type": "INT", "nullable": False},
                    {"name": "name", "data_type": "VARCHAR(64)"},
                ],
            },
        ]
    )
    entity_info = tables["entity_info"]
    dataset = DatasetConfig(
        name="source_a_stream",
        delta_table_schema="source_a",
        input_config={"type": "dsv", "search_paths": []},
        pipeline=[
            {
                "schema": "source_a",
                "table": "vectors",
                "type": "primary",
                "columns": [
                    {"source": "source_entity_id", "target": "entity_id"},
                    {"source": "source_latitude", "target": "latitude"},
                ],
            },
            {
                "schema": "source_a",
                "table": "entity_info",
                "columns": [
                    {"source": "source_entity_id", "target": "entity_id"},
                    {"source": "source_name", "target": "name"},
                ],
            },
        ],
    )
    update_time = datetime(2030, 1, 1, 12, 5, tzinfo=UTC)
    staged_df = pl.DataFrame(
        {
            "entity_id": [123],
            "name": ["Commodity Test"],
        }
    )
    backend = _FakeXtdbBackend()
    staging = _FakeStagingOps(staged_df)

    did_modify = scrape_extract_item(
        1,
        dataset,
        "entity_info",
        tables,
        update_time,
        SimpleNamespace(),
        stage_run_id="stage-1",
        staging=staging,
        backend=backend,
    )

    assert did_modify is True
    assert staging.prepare_calls == [
        (("stage-1", dataset, 1, entity_info), {"valid_time": None})
    ]
    args, kwargs = backend.temporal_upsert_calls[0]
    assert args[1:] == ("source_a", "entity_info")
    assert args[0].to_dict(as_series=False) == {
        "entity_id": [123],
        "name": ["Commodity Test"],
    }
    assert kwargs["table_config"] == entity_info
    assert kwargs["delta_config"] == dataset.delta_config
    assert kwargs["dataframe_ops"] is staging.bulk_dataframe_ops
    assert "update_time" not in kwargs
    assert kwargs["valid_time"] is None
