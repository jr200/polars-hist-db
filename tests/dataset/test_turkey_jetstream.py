import asyncio
import pytest
import pytest_asyncio
import polars as pl
import nats

from polars_hist_db.config import FunctionRegistry
from polars_hist_db.dataset import run_workflows
from .helpers import custom_try_to_usd
from ..utils.nats_helper import (
    create_nats_js_client,
    create_nats_server,
    publish_dataframe_messages,
)

from ..utils.dsv_helper import (
    setup_fixture_dataset,
)

_STREAM_NAME = "turkey_test_food_stream"
_SUBJECT_PREFIX = "turkey_test"


@pytest.fixture(scope="session")
def nats_server():
    yield from create_nats_server()


@pytest_asyncio.fixture
async def nats_js(nats_server):
    async for js in create_nats_js_client(_STREAM_NAME, [_SUBJECT_PREFIX + ".>"]):
        yield js


@pytest.fixture
def fixture_with_config():
    fn_reg = FunctionRegistry()
    fn_reg.register_function("try_to_usd", custom_try_to_usd, allow_overwrite=True)

    yield from setup_fixture_dataset("dataset_config/dataset_foodprices.yaml")


async def try_create_stream(
    nats_js: nats.js.JetStreamContext, stream_name: str, subjects: list[str]
):
    try:
        await nats_js.add_stream(name=stream_name, subjects=subjects)
    except nats.js.errors.BadRequestError:
        # Stream might already exist
        pass


# @pytest.mark.asyncio
async def xtest_turkey_stream(nats_js, fixture_with_config):
    test_data = pl.read_json("tests/_testdata_dataset_data/turkey_food_prices.json")

    engine, base_config = fixture_with_config
    dataset_name = "turkey_food_prices_jetstream"
    dataset = base_config.datasets[dataset_name]

    js_config = dataset.input_config.js_args

    # Publish messages from DataFrame
    await try_create_stream(nats_js, js_config.name, js_config.subjects)
    await publish_dataframe_messages(
        nats_js, test_data, js_config.subjects[0], js_config.name
    )

    # wait for 1 second
    await asyncio.sleep(1)

    await run_workflows(base_config, engine, "turkey_food_prices_jetstream")

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
