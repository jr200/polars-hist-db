import json
import logging
import subprocess
import time
import signal
import os
import polars as pl
from nats.aio.client import Client as NATS
import nats
from nats.js.client import JetStreamContext

LOGGER = logging.getLogger(__name__)

TEST_NATS_PORT = "4112"


async def publish_dataframe_messages(
    js: JetStreamContext,
    df: pl.DataFrame,
    nats_subject: str,
    stream: str,
):
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
    # Start NATS server with JetStream enabled
    server = subprocess.Popen(
        ["nats-server", "-js", "-p", TEST_NATS_PORT],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        preexec_fn=os.setsid,  # This ensures we can kill the entire process group
    )

    # Give the server a moment to start
    time.sleep(1)

    try:
        yield server
    finally:
        try:
            # Cleanup: kill the server and all its child processes
            os.killpg(os.getpgid(server.pid), signal.SIGTERM)
            server.wait()
        except ProcessLookupError:
            # Process might have already terminated
            pass


async def try_create_test_stream(
    nats_js: nats.js.JetStreamContext, stream_name: str, subject: str
):
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


async def create_nats_js_client():
    nc = NATS()
    await nc.connect(f"nats://localhost:{TEST_NATS_PORT}")
    js = nc.jetstream()

    try:
        yield js
    finally:
        await nc.close()
