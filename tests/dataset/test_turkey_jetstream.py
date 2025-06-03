import asyncio
import pytest
import pytest_asyncio
import polars as pl
import logging

from polars_hist_db.config import FunctionRegistry
from polars_hist_db.dataset import run_workflows
from .helpers import custom_try_to_usd
from ..utils.nats_helper import (
    create_nats_server,
    create_nats_js_client,
    publish_dataframe_messages,
    try_create_test_stream,
)

from ..utils.dsv_helper import (
    setup_fixture_dataset,
)

LOGGER = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def nats_server():
    yield from create_nats_server()


@pytest_asyncio.fixture
async def nats_js(nats_server):
    async for js in create_nats_js_client():
        yield js


@pytest.fixture
def fixture_with_config():
    fn_reg = FunctionRegistry()
    fn_reg.register_function("try_to_usd", custom_try_to_usd, allow_overwrite=True)

    yield from setup_fixture_dataset("foodprices.yaml")


@pytest.mark.asyncio
async def test_turkey_stream(nats_js, fixture_with_config):
    unique_keys = ["Year", "Month", "ProductId", "UmId"]
    test_data = (
        pl.read_csv("tests/_testdata_dataset_data/turkey_food_prices.csv")
        .sort(*unique_keys)
        .unique(subset=unique_keys, maintain_order=True, keep="last")
    )

    engine, base_config = fixture_with_config
    dataset_name = "turkey_food_prices_jetstream"
    dataset = base_config.datasets[dataset_name]

    js_config = dataset.input_config.js_sub

    # Publish messages from DataFrame
    await try_create_test_stream(nats_js, js_config.stream, js_config.subject)
    expected_msgs = await publish_dataframe_messages(
        nats_js, test_data, js_config.subject, js_config.stream
    )

    print(expected_msgs)

    # wait for 1 second
    await asyncio.sleep(1)

    time_partitions = dict()
    await run_workflows(
        base_config,
        engine,
        "turkey_food_prices_jetstream",
        debug_capture_output=time_partitions,
    )

    df = pl.concat(time_partitions.values())
    assert not df.is_empty()

    # # Create a consumer
    # sub = await nats_js.subscribe(
    #     _SUBJECT_PREFIX + ".>", stream=_STREAM_NAME, durable="test_consumer"
    # )

    # # Wait for and verify all messages
    # received_messages = []
    # async for msg in sub.messages:
    #     received_data = json.loads(msg.data.decode())
    #     received_messages.append(received_data)
    #     await msg.ack()
    #     if len(received_messages) == len(test_data):
    #         break

    # # Convert received messages to DataFrame for comparison
    # received_df = pl.DataFrame(received_messages)

    # # Verify all data was received correctly
    # assert received_df.equals(test_data)

    # Cleanup
    # await sub.unsubscribe()
