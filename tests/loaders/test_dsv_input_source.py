from datetime import UTC, datetime
from types import SimpleNamespace

import polars as pl
import pytest

from polars_hist_db.config import TableConfig
from polars_hist_db.config.dataset import TimePartition
from polars_hist_db.loaders.dsv_input_source import (
    DsvCrawlerInputSource,
    _make_dsv_file_finalizer,
)


@pytest.mark.asyncio
async def test_dsv_file_commit_succeeds_when_no_tables_changed(monkeypatch):
    def fail_audit_ops(schema):
        raise AssertionError("no audit entry should be written without changes")

    monkeypatch.setattr(
        "polars_hist_db.loaders.dsv_input_source.AuditOps", fail_audit_ops
    )

    finalizer = _make_dsv_file_finalizer(
        "/tmp/source.csv",
        datetime(2026, 1, 1, tzinfo=UTC),
    )

    assert finalizer.write_audit_before_commit(object(), []) is True


def test_time_partitioning_handles_interleaved_csv_rows():
    source = object.__new__(DsvCrawlerInputSource)
    source.tables = {
        "events": TableConfig(
            name="events",
            schema="sample",
            columns=[],
            primary_keys=["id"],
        )
    }
    source.dataset = SimpleNamespace(
        pipeline=SimpleNamespace(
            get_main_table_name=lambda: ("sample", "events"),
            get_header_map=lambda table: {"id": "id"},
        ),
        time_partition=TimePartition(
            column="event_time",
            bucket_interval="1d",
            bucket_strategy="round_down",
        ),
    )
    source.config = SimpleNamespace(filter_past_events=False)
    source.previous_payload_time = datetime.min.replace(tzinfo=UTC)
    jan_1 = datetime(2026, 1, 1, tzinfo=UTC)
    jan_2 = datetime(2026, 1, 2, tzinfo=UTC)
    rows = pl.DataFrame(
        {
            "id": [2, 1, 3, 4],
            "event_time": [jan_2, jan_1, jan_2, jan_1],
        }
    )

    partitions = source._apply_time_partitioning(rows, jan_2)

    assert [timestamp for timestamp, _ in partitions] == [jan_1, jan_2]
    assert [frame.get_column("id").to_list() for _, frame in partitions] == [
        [1, 4],
        [2, 3],
    ]
