import json
import logging
import os

import nats
import polars as pl
from nats.aio.client import Client as NATS
from nats.js.client import JetStreamContext

from polars_hist_db.config.input.jetstream_config import JetStreamSubscriptionConfig

LOGGER = logging.getLogger(__name__)

NATS_PORT = os.environ.get("NATS_PORT", "14222")


async def publish_dataframe_messages(
    js: JetStreamContext,
    df: pl.DataFrame,
    subscription: JetStreamSubscriptionConfig,
):
    nats_subject = subscription.subject
    stream = subscription.stream

    acks = []
    for row in df.iter_rows(named=True):
        # Convert row to JSON and publish
        row_data = json.dumps(row).encode()
        ack = await js.publish(nats_subject, row_data, stream=stream)
        acks.append(ack)

    # Verify all messages were published to the correct stream
    for ack in acks:
        assert ack.stream == stream

    info = await js.stream_info(stream)
    LOGGER.info("stream info after publish: %s", info)

    consumers = await js.consumers_info(stream)
    assert len(consumers) == 0

    LOGGER.info("published %d messages to %s", len(df), nats_subject)
    return len(df)


def create_nats_server():
    """Lifecycle no-op kept for back-compat with existing fixtures.

    nats-server used to be subprocess.Popen-spawned per test session;
    we now provide it externally (CI: docker run step that mounts
    tests/nats-server.conf; local: run nats-server yourself before
    pytest). Tests call this as a context manager out of habit; just
    yield None so they keep working.
    """
    yield None


async def try_create_test_stream(
    nats_js: nats.js.JetStreamContext, subscription: JetStreamSubscriptionConfig
):
    stream_name = subscription.stream
    subject = subscription.subject

    # delete the stream if it exists
    try:
        did_delete = await nats_js.delete_stream(stream_name)
        if did_delete:
            LOGGER.info("Deleted stream %s", stream_name)
    except nats.js.errors.NotFoundError:
        pass

    try:
        stream_info = await nats_js.add_stream(name=stream_name, subjects=[subject])
        assert stream_info.config.name == stream_name
        assert stream_info.config.subjects is not None
        assert subject in stream_info.config.subjects
    except nats.js.errors.BadRequestError:
        # Stream might already exist
        pass


async def _create_nats_core_client():
    nc = NATS()

    try:
        await nc.connect(f"nats://localhost:{NATS_PORT}")
        yield nc
    finally:
        await nc.close()


async def create_nats_test_client():
    async for nc in _create_nats_core_client():
        js = nc.jetstream()

        try:
            yield nc, js
        finally:
            await nc.close()
