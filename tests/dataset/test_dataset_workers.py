import asyncio
from datetime import UTC, datetime, timezone
from threading import get_ident
from types import SimpleNamespace

import polars as pl
import pytest

from polars_hist_db.config import IngestionConfig, PolarsHistDbConfig
from polars_hist_db.dataset.entrypoint import _run_dataset_workers
from polars_hist_db.loaders.dsv_input_source import DsvCrawlerInputSource


def _dataset(name: str, table: str):
    return SimpleNamespace(
        name=name,
        input_config=object(),
        pipeline=SimpleNamespace(get_pipeline_items=lambda: {0: ("sample", table)}),
    )


def _backend():
    return SimpleNamespace(
        open_ingest_connection=lambda config: None,
        close_ingest_connection=lambda connection: None,
    )


def test_ingestion_worker_config_is_bounded():
    assert IngestionConfig() == IngestionConfig(max_workers=1, queue_size=1)
    with pytest.raises(ValueError, match="max_workers"):
        IngestionConfig(max_workers=0)
    with pytest.raises(ValueError, match="queue_size"):
        IngestionConfig(queue_size=0)

    config = PolarsHistDbConfig(
        {
            "table_configs": [],
            "datasets": [],
            "ingestion": {"max_workers": 3, "queue_size": 4},
        },
        table_configs_path=["table_configs"],
        datasets_path=["datasets"],
    )
    assert config.ingestion == IngestionConfig(max_workers=3, queue_size=4)


@pytest.mark.asyncio
async def test_dsv_preparation_runs_off_the_event_loop():
    event_loop_thread = get_ident()
    preparation_threads = []
    payload_time = datetime(2026, 1, 1, tzinfo=UTC)
    source = object.__new__(DsvCrawlerInputSource)
    source.config = SimpleNamespace(
        has_payload=lambda: True,
        payload="id\n1\n",
        payload_time=payload_time,
    )
    source.dataset = SimpleNamespace(
        pipeline=SimpleNamespace(get_main_table_name=lambda: ("sample", "items"))
    )

    def filter_files(candidates, table_schema, table_name, engine):
        preparation_threads.append(get_ident())
        return candidates

    def process_payload(payload, timestamp):
        preparation_threads.append(get_ident())
        return [(timestamp, pl.DataFrame({"id": [1]}))]

    source._search_and_filter_files = filter_files
    source._process_payload = process_payload

    generator = await source.next_df(object())
    partitions, _finalizer = await anext(generator)
    await generator.aclose()

    assert partitions[0][1]["id"].to_list() == [1]
    assert preparation_threads
    assert all(thread != event_loop_thread for thread in preparation_threads)


@pytest.mark.asyncio
async def test_independent_datasets_run_concurrently(monkeypatch):
    active = 0
    peak = 0
    both_started = asyncio.Event()
    release = asyncio.Event()

    async def fake_run_dataset(*args, **kwargs):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        if active == 2:
            both_started.set()
        try:
            await release.wait()
        finally:
            active -= 1

    monkeypatch.setattr(
        "polars_hist_db.dataset.entrypoint._run_dataset", fake_run_dataset
    )
    datasets = [_dataset("first", "one"), _dataset("second", "two")]
    task = asyncio.create_task(
        _run_dataset_workers(
            datasets,
            SimpleNamespace(db_config=object(), tables=object()),
            object(),
            _backend(),
            IngestionConfig(max_workers=2, queue_size=1),
            [None, None],
            None,
            None,
            True,
        )
    )

    await asyncio.wait_for(both_started.wait(), timeout=1)
    release.set()
    await task

    assert peak == 2


@pytest.mark.asyncio
async def test_datasets_writing_the_same_table_remain_ordered(monkeypatch):
    active = 0
    peak = 0

    async def fake_run_dataset(*args, **kwargs):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0)
        active -= 1

    monkeypatch.setattr(
        "polars_hist_db.dataset.entrypoint._run_dataset", fake_run_dataset
    )
    datasets = [_dataset("first", "shared"), _dataset("second", "shared")]

    await _run_dataset_workers(
        datasets,
        SimpleNamespace(db_config=object(), tables=object()),
        object(),
        _backend(),
        IngestionConfig(max_workers=2, queue_size=1),
        [None, None],
        None,
        None,
        True,
    )

    assert peak == 1


@pytest.mark.asyncio
async def test_worker_failure_cancels_sibling_dataset(monkeypatch):
    both_started = asyncio.Event()
    cancelled = asyncio.Event()
    started = 0

    async def fake_run_dataset(input_config, dataset, *args, **kwargs):
        nonlocal started
        started += 1
        if started == 2:
            both_started.set()
        await both_started.wait()
        if dataset.name == "failing":
            raise RuntimeError("dataset failed")
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    monkeypatch.setattr(
        "polars_hist_db.dataset.entrypoint._run_dataset", fake_run_dataset
    )
    datasets = [_dataset("failing", "one"), _dataset("sibling", "two")]

    with pytest.raises(RuntimeError, match="dataset failed"):
        await _run_dataset_workers(
            datasets,
            SimpleNamespace(db_config=object(), tables=object()),
            object(),
            _backend(),
            IngestionConfig(max_workers=2, queue_size=1),
            [None, None],
            None,
            None,
            True,
        )

    assert cancelled.is_set()
