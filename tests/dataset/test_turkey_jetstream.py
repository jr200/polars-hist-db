import asyncio
import pytest
import pytest_asyncio
import polars as pl
import logging

from polars_hist_db.config import FunctionRegistry
from polars_hist_db.dataset import run_workflows
from polars_hist_db.utils.compare import compare_dataframes
from .helpers import custom_try_to_usd
from ..utils.nats_helper import (
    create_nats_server,
    create_nats_js_client,
    publish_dataframe_messages,
    try_create_test_stream,
)

from ..utils.dsv_helper import (
    read_df_from_db,
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
    _num_expected_msgs = await publish_dataframe_messages(
        nats_js, test_data, js_config.subject, js_config.stream
    )

    # wait for 1 second
    await asyncio.sleep(1)

    time_partitions = list()
    await run_workflows(
        base_config,
        engine,
        "turkey_food_prices_jetstream",
        debug_capture_output=time_partitions,
    )

    uploaded_df = pl.concat([df for _, df in time_partitions])
    assert not uploaded_df.is_empty()
    assert len(uploaded_df) == len(test_data)

    diff_df, missing_cols = compare_dataframes(
        uploaded_df,
        test_data,
        on=["UmId", "ProductId", "Place", "Price"],
    )
    assert len(diff_df) == 0
    assert len(missing_cols) == 4
    assert missing_cols == [
        "missing:Month_lhs",
        "missing:Year_lhs",
        "missing:price_usd_rhs",
        "missing:time_rhs",
    ]

    table_schema = base_config.tables.schemas()[0]
    table_config = base_config.tables["food_prices"]
    df_read, df_read_history = read_df_from_db(engine, table_schema, table_config)

    assert df_read_history.shape == (2845, 6)
    assert df_read.shape == (52, 6)

    expected_df_read = (
        uploaded_df.group_by("ProductId", "UmId", maintain_order=True)
        .last()
        .unique(subset=["ProductId", "UmId"], maintain_order=True, keep="last")
    )

    renamings = {
        c.target: c.source
        for c in dataset.pipeline.build_ingestion_column_definitions(base_config.tables)
        if c.target is not None and c.source is not None and c.target in df_read.columns
    }

    diff_df, missing_cols = compare_dataframes(
        df_read.rename(renamings),
        expected_df_read,
        on=["UmId", "ProductId", "Price", "price_usd"],
    )

    assert len(diff_df) == 0
    assert missing_cols == [
        "missing:Place_lhs",
        "missing:ProductName_lhs",
        "missing:UmName_lhs",
        "missing:time_lhs",
        "missing:__valid_from_rhs",
        "missing:__valid_to_rhs",
    ]
