import asyncio
from contextlib import nullcontext
from datetime import UTC, datetime, timezone
from threading import Event
from types import SimpleNamespace

import pytest

from polars_hist_db.dataset.scrape import (
    _run_pipeline_as_transaction,
    try_run_pipeline_as_transaction,
)
from polars_hist_db.loaders.input_source import BatchFinalizer
from polars_hist_db.types import TypeContractError
from polars_hist_db.utils import NonRetryableException


@pytest.mark.asyncio
async def test_pipeline_cancellation_waits_for_started_thread(monkeypatch):
    started = Event()
    release = Event()

    def run_batch(*args):
        started.set()
        release.wait()

    monkeypatch.setattr(
        "polars_hist_db.dataset.scrape._run_pipeline_as_transaction", run_batch
    )
    task = asyncio.create_task(
        try_run_pipeline_as_transaction(
            [], object(), object(), object(), BatchFinalizer()
        )
    )
    assert await asyncio.to_thread(started.wait, 1)

    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task


def test_xtdb_atomic_batch_rejects_multiple_system_times():
    backend = SimpleNamespace(
        name="xtdb", connection_scope=lambda engine: nullcontext(object())
    )
    partitions = [
        (datetime(2026, 1, 1, tzinfo=UTC), object()),
        (datetime(2026, 1, 2, tzinfo=UTC), object()),
    ]

    with pytest.raises(NonRetryableException, match="one system-time"):
        _run_pipeline_as_transaction(
            partitions,
            object(),
            object(),
            object(),
            BatchFinalizer(),
            backend=backend,
        )


@pytest.mark.asyncio
async def test_pipeline_acks_only_after_transaction_finishes(monkeypatch):
    events = []

    def run_batch(*args):
        events.append("committed")

    async def ack():
        events.append("acked")

    monkeypatch.setattr(
        "polars_hist_db.dataset.scrape._run_pipeline_as_transaction", run_batch
    )

    await try_run_pipeline_as_transaction(
        [], object(), object(), object(), BatchFinalizer(ack_after_commit=ack)
    )

    assert events == ["committed", "acked"]


@pytest.mark.asyncio
async def test_pipeline_does_not_ack_when_transaction_fails(monkeypatch):
    events = []

    def fail(*args):
        events.append("failed")
        raise RuntimeError("commit failed")

    async def ack():
        events.append("acked")

    monkeypatch.setattr(
        "polars_hist_db.dataset.scrape._run_pipeline_as_transaction", fail
    )

    with pytest.raises(RuntimeError, match="commit failed"):
        await try_run_pipeline_as_transaction(
            [],
            object(),
            object(),
            object(),
            BatchFinalizer(ack_after_commit=ack),
            num_retries=1,
        )

    assert events == ["failed"]


@pytest.mark.asyncio
async def test_pipeline_surfaces_ack_failure_without_rerunning_transaction(monkeypatch):
    events = []

    def run_batch(*args):
        events.append("committed")

    async def fail_ack():
        events.append("ack_failed")
        raise RuntimeError("ack failed")

    monkeypatch.setattr(
        "polars_hist_db.dataset.scrape._run_pipeline_as_transaction", run_batch
    )

    with pytest.raises(RuntimeError, match="ack failed"):
        await try_run_pipeline_as_transaction(
            [],
            object(),
            object(),
            object(),
            BatchFinalizer(ack_after_commit=fail_ack),
        )

    assert events == ["committed", "ack_failed"]


@pytest.mark.asyncio
async def test_pipeline_raises_final_retry_error(monkeypatch):
    attempts = 0

    def fail(*args):
        nonlocal attempts
        attempts += 1
        raise RuntimeError("still broken")

    async def no_wait(delay):
        pass

    monkeypatch.setattr(
        "polars_hist_db.dataset.scrape._run_pipeline_as_transaction", fail
    )
    monkeypatch.setattr("polars_hist_db.dataset.scrape.asyncio.sleep", no_wait)

    with pytest.raises(RuntimeError, match="still broken"):
        await try_run_pipeline_as_transaction(
            [],
            SimpleNamespace(),
            object(),
            object(),
            BatchFinalizer(),
            num_retries=3,
        )

    assert attempts == 3


@pytest.mark.asyncio
async def test_pipeline_does_not_retry_type_contract_errors(monkeypatch):
    attempts = 0

    def fail(*args):
        nonlocal attempts
        attempts += 1
        raise TypeContractError("invalid schema")

    monkeypatch.setattr(
        "polars_hist_db.dataset.scrape._run_pipeline_as_transaction", fail
    )

    with pytest.raises(TypeContractError, match="invalid schema"):
        await try_run_pipeline_as_transaction(
            [],
            SimpleNamespace(),
            object(),
            object(),
            BatchFinalizer(),
            num_retries=3,
        )

    assert attempts == 1
