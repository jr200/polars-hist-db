from datetime import datetime, timedelta
import pytest
import polars as pl
from polars.testing import assert_frame_equal

from polars_hist_db.core.dataframe import TimeHint, DataframeOps

from tests.utils import (
    add_random_row,
    from_test_result,
    modify_and_read,
    read_df_from_db,
    set_random_seed,
    setup_fixture_tableconfigs,
)


@pytest.fixture
def temp_table():
    yield from setup_fixture_tableconfigs("all_col_types_nullable.yaml")


def test_time_hints(temp_table):
    engine, table_configs, table_schema = temp_table
    table_config = table_configs.items[0]
    table_config.delta_config.drop_unchanged_rows = True

    # upload then test initial df
    ts_1 = datetime.fromisoformat("1988-01-01T00:00:01Z")
    df_1 = from_test_result(
        """
        id,bigint_col,bit_col,bool_col,boolean_col,char_col,date_col,datetime_col,decimal_col,double_col,float_col,int_col,integer_col,mediumint_col,numeric_col,real_col,smallint_col,text_col,time_col,timestamp_col,tinyint_col,varchar_col
        1,1000000001,0,false,true,Z,1988-01-01,1988-01-01T12:00:00,123.46,123.456790,12.35,101,102,1001,987.66,45.68,11,Updated text 1,12:34:57,1988-01-01T12:34:57,2,Updated short 1
    """,
        table_config.name,
        table_configs
    )

    set_random_seed(1)
    for i in range(0, 7):
        df_i = add_random_row(df_1, table_config, {"id": 1})
        ts_i = ts_1 + timedelta(days=i * 30)
        df_read, df_read_history = modify_and_read(
            engine, df_i, table_schema, table_config, ts_i, "upload"
        )

    df_read, df_read_history = read_df_from_db(engine, table_schema, table_config)

    assert df_read.shape == (1, 24)
    assert df_read_history.shape == (6, 24)

    # no hint -- current table only
    with engine.begin() as connection:
        query_df = pl.from_dict({"id": 1}, schema_overrides={"id": pl.Int32})
        asof_utc = df_read_history[3]["__valid_from"][0]
        asof_utc = None
        df_1 = (
            DataframeOps(connection)
            .table_query(  # noqa: F821
                table_schema,
                table_configs.items[0].name,
                query_df,
                column_selection=None,
            )
            .sort("id")
        )

    assert_frame_equal(df_1, df_read)

    # asof a particualr date, no history
    with engine.begin() as connection:
        query_df = pl.from_dict({"id": 1}, schema_overrides={"id": pl.Int32})
        asof_utc = df_read_history[3]["__valid_from"][0]
        df_2 = (
            DataframeOps(connection)
            .table_query(
                table_schema,
                table_configs.items[0].name,
                query_df,
                column_selection=None,
                time_hint=TimeHint(mode="asof", asof_utc=asof_utc),
            )
            .sort("id")
        )

    assert_frame_equal(df_2, df_read_history[3])

    # asof a particualr date, 0 days history
    with engine.begin() as connection:
        query_df = pl.from_dict({"id": 1}, schema_overrides={"id": pl.Int32})
        asof_utc = df_read_history[3]["__valid_from"][0]
        df_3 = (
            DataframeOps(connection)
            .table_query(
                table_schema,
                table_configs.items[0].name,
                query_df,
                column_selection=None,
                time_hint=TimeHint(
                    mode="span", asof_utc=asof_utc, history_span=timedelta(days=0)
                ),
            )
            .sort("id")
        )

    assert_frame_equal(df_3, df_read_history[3])

    # asof a particualr date, 2 days history
    with engine.begin() as connection:
        query_df = pl.from_dict({"id": 1}, schema_overrides={"id": pl.Int32})
        asof_utc = df_read_history[5]["__valid_from"][0]
        df_4 = (
            DataframeOps(connection)
            .table_query(
                table_schema,
                table_configs.items[0].name,
                query_df,
                column_selection=None,
                time_hint=TimeHint(
                    mode="span", asof_utc=asof_utc, history_span=timedelta(days=2)
                ),
            )
            .sort("__valid_from")
        )

    assert_frame_equal(df_4, df_read_history.slice(4, 2))
