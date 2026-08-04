import asyncio
from datetime import UTC, datetime, timezone
from types import SimpleNamespace
from typing import Any

import pytest

from polars_hist_db.loaders.input_source import BatchFinalizer
from polars_hist_db.loaders.jetstream_input_source import JetStreamInputSource


def _source(js, heartbeat_interval: float = 0.001):
    source: Any = object.__new__(JetStreamInputSource)
    source._js = js
    source.config = SimpleNamespace(
        run_until="empty",
        jetstream=SimpleNamespace(
            subscription=SimpleNamespace(
                durable="test",
                subject="events",
                stream="EVENTS",
                consumer_args={},
                options={},
            ),
            fetch=SimpleNamespace(
                heartbeat_interval=heartbeat_interval,
                batch_size=10,
                batch_timeout=0.01,
            ),
        ),
    )
    source.dataset = SimpleNamespace(
        scrape_limit=1,
        pipeline=SimpleNamespace(get_main_table_name=lambda: ("test", "events")),
    )
    return source


@pytest.mark.asyncio
async def test_heartbeat_recovers_after_transient_failure():
    recovered = asyncio.Event()

    class Message:
        calls = 0

        async def in_progress(self):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("temporary heartbeat failure")
            recovered.set()

    message = Message()
    task = asyncio.create_task(_source(None)._heartbeat([message]))
    try:
        await asyncio.wait_for(recovered.wait(), timeout=0.5)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert message.calls >= 2


@pytest.mark.asyncio
async def test_heartbeat_runs_while_batch_is_yielded_and_stops_after_close():
    heartbeat = asyncio.Event()

    class Message:
        calls = 0
        metadata = SimpleNamespace(timestamp=datetime(2026, 7, 20, tzinfo=UTC))

        async def in_progress(self):
            self.calls += 1
            heartbeat.set()

    message = Message()

    class Subscription:
        async def fetch(self, batch_size, timeout):
            return [message]

    class JetStream:
        async def pull_subscribe(self, **kwargs):
            return Subscription()

    source = _source(JetStream())
    source._prepare_batch = lambda *args: ([], BatchFinalizer())
    batches = await source.next_df(object())

    await anext(batches)
    await asyncio.wait_for(heartbeat.wait(), timeout=0.5)
    await batches.aclose()
    calls_after_close = message.calls
    await asyncio.sleep(0.01)

    assert calls_after_close >= 1
    assert message.calls == calls_after_close
