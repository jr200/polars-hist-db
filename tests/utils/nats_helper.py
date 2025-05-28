import json
import subprocess
import time
import signal
import os
import polars as pl
from nats.aio.client import Client as NATS
from nats.js.client import JetStreamContext

TEST_NATS_PORT = "4112"


async def publish_dataframe_messages(
    js: JetStreamContext,
    df: pl.DataFrame,
    nats_subject: str,
    stream_name: str,
) -> list:
    """
    Publish messages from a Polars DataFrame to NATS JetStream.
    Each row becomes a JSON message.

    Args:
        js: JetStream context
        df: Polars DataFrame containing the data
        nats_subject: NATS subject to publish to
        stream_name: Name of the NATS stream to publish to

    Returns:
        List of acknowledgment objects from the publish operations
    """

    acks = []
    for row in df.iter_rows(named=True):
        # Convert row to JSON and publish
        ack = await js.publish(nats_subject, json.dumps(row).encode())
        acks.append(ack)

    # Verify all messages were published to the correct stream
    for ack in acks:
        assert ack.stream == stream_name

    return acks


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


async def create_nats_js_client(name: str, subjects: list[str]):
    nc = NATS()
    await nc.connect(f"nats://localhost:{TEST_NATS_PORT}")
    js = nc.jetstream()

    try:
        await js.add_stream(name=name, subjects=subjects)
    except Exception:
        # Stream might already exist
        pass

    try:
        yield js
    finally:
        await nc.close()
