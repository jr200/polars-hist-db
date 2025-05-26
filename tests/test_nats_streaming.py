import json
import pytest
import pytest_asyncio
import polars as pl
import nats

from tests.utils.nats_helper import (
    create_nats_js_client,
    create_nats_server,
    publish_dataframe_messages,
)


@pytest.fixture(scope="session")
def nats_server():
    yield from create_nats_server()


@pytest_asyncio.fixture
async def nats_js(nats_server):
    async for js in create_nats_js_client("test_stream", ["test.>"]):
        yield js


@pytest.mark.asyncio
async def test_nats_streaming(nats_js: nats.js.JetStreamContext):
    test_data = pl.read_json("tests/_testdata_dataset_data/turkey_food_prices.json")

    # Publish messages from DataFrame
    await publish_dataframe_messages(nats_js, test_data, "test.message", "test_stream")

    # Create a consumer
    sub = await nats_js.subscribe(
        "test.>", stream="test_stream", durable="test_consumer"
    )

    # Wait for and verify all messages
    received_messages = []
    async for msg in sub.messages:
        received_data = json.loads(msg.data.decode())
        received_messages.append(received_data)
        await msg.ack()
        if len(received_messages) == len(test_data):
            break

    # Convert received messages to DataFrame for comparison
    received_df = pl.DataFrame(received_messages)

    # Verify all data was received correctly
    assert received_df.equals(test_data)

    # Cleanup
    await sub.unsubscribe()
