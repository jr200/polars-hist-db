from datetime import datetime
import pytest

import polars as pl

from polars_hist_db.core.table import TableOps
from polars_hist_db.dataset.workflow import run_workflows
from polars_hist_db.utils.compare import compare_dataframes

from ..utils.dsv_helper import (
    read_df_from_db,
    setup_fixture_dataset,
)


@pytest.fixture
def fixture_with_defaults():
    yield from setup_fixture_dataset(
        "dataset_config/dataset_all_col_types_defaults.yaml"
    )


@pytest.mark.asyncio
async def test_value_if_missing(fixture_with_defaults):
    engine, base_config = fixture_with_defaults
    table_schema = base_config.tables.schemas()[0]
    table_config = base_config.tables.items[0]

    # upload then test initial df
    ts_1 = datetime.fromisoformat("1985-01-01T00:00:01Z")
    dsv_1 = """
        id,bigint_col,bit_col,bool_col,boolean_col,char_col,date_col,datetime_col,decimal_col,double_col,float_col,int_col,integer_col,mediumint_col,numeric_col,real_col,smallint_col,text_col,time_col,timestamp_col,tinyint_col,varchar_col
        1,1000000000,1,true,false,A,1985-01-01,1985-01-01T12:00:00,123.45,123.456789,12.34,100,101,1000,987.65,45.67,10,Sample text 1,12:34:56,1985-01-01T12:34:56,1,Short text 1
    """

    base_config.datasets.datasets[0].input_config.set_payload(dsv_1, ts_1)
    uploaded_partitions = dict()
    await run_workflows(base_config, engine, debug_capture_output=uploaded_partitions)

    df_1 = pl.concat(uploaded_partitions.values())
    df_read, df_read_history = read_df_from_db(engine, table_schema, table_config)

    assert df_read_history.is_empty()
    assert len(df_read) == len(df_1) == 1
    diff_df, missing_cols = compare_dataframes(
        df_read,
        df_1,
        on=["id"],
    )
    assert len(diff_df) == 0
    assert len(missing_cols) == len(TableOps.system_versioning_columns())

    # # insert a dataframe with defaults
    ts_2 = datetime.fromisoformat("1986-01-01T00:00:01Z")
    dsv_2 = """
        id,bigint_col,bit_col,bool_col,boolean_col,char_col,date_col,datetime_col,decimal_col,double_col,float_col,int_col,integer_col,mediumint_col,numeric_col,real_col,smallint_col,text_col,time_col,timestamp_col,tinyint_col,varchar_col
        1,1000000000,1,true,false,A,1985-01-01,1985-01-01T12:00:00,123.45,123.456789,12.34,100,101,1000,987.65,45.67,10,Sample text 1,12:34:56,1985-01-01T12:34:56,1,Short text 1
        2,,,,,,,,,,,,,,,,,,,,,
    """

    base_config.datasets.datasets[0].input_config.set_payload(dsv_2, ts_2)
    uploaded_partitions = dict()
    await run_workflows(base_config, engine, debug_capture_output=uploaded_partitions)

    df_2 = pl.concat(uploaded_partitions.values())
    df_read, df_read_history = read_df_from_db(engine, table_schema, table_config)

    assert df_read_history.is_empty()
    assert len(df_read) == len(df_2) == 2

    diff_df, missing_cols = compare_dataframes(
        df_read,
        df_2,
        on=["id"],
    )
    assert len(diff_df) == 0
    assert len(missing_cols) == len(TableOps.system_versioning_columns())
