from datetime import datetime
import pytest
from polars.testing import assert_frame_equal

from polars_hist_db.utils.compare import compare_dataframes

from ..utils.dsv_helper import (
    from_test_result,
    modify_and_read,
    setup_fixture_dataset,
)


@pytest.fixture
def fixture_with_defaults():
    yield from setup_fixture_dataset(
        "dataset_config/dataset_all_col_types_defaults.yaml"
    )


def test_value_if_missing(fixture_with_defaults):
    engine, base_config = fixture_with_defaults
    table_schema = base_config.tables.schemas()[0]
    table_config = base_config.tables.items[0]

    # upload then test initial df
    ts_1 = datetime.fromisoformat("1985-01-01T00:00:01Z")
    df_1 = from_test_result(
        """
        id,bigint_col,bit_col,bool_col,boolean_col,char_col,date_col,datetime_col,decimal_col,double_col,float_col,int_col,integer_col,mediumint_col,numeric_col,real_col,smallint_col,text_col,time_col,timestamp_col,tinyint_col,varchar_col
        1,1000000000,1,true,false,A,1985-01-01,1985-01-01T12:00:00,123.45,123.456789,12.34,100,101,1000,987.65,45.67,10,Sample text 1,12:34:56,1985-01-01T12:34:56,1,Short text 1
    """,
        table_config.name,
        base_config.tables,
    )

    df_read, df_read_history = modify_and_read(
        engine, df_1, table_schema, table_config, ts_1, "upload"
    )

    df_expected = from_test_result(
        """
        id,bigint_col,bit_col,bool_col,boolean_col,char_col,date_col,datetime_col,decimal_col,double_col,float_col,int_col,integer_col,mediumint_col,numeric_col,real_col,smallint_col,text_col,time_col,timestamp_col,tinyint_col,varchar_col,__valid_from,__valid_to
        1,1000000000,1,true,false,A,1985-01-01,1985-01-01T12:00:00,123.45,123.456789,12.34,100,101,1000,987.65,45.67,10,Sample text 1,12:34:56,1985-01-01T12:34:56,1,Short text 1,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
    """,
        table_config.name,
        base_config.tables,
    )

    assert_frame_equal(df_expected, df_read)
    assert df_read_history.is_empty()

    # insert a dataframe with defaults
    ts_2 = datetime.fromisoformat("1986-01-01T00:00:01Z")
    df_2 = from_test_result(
        """
        id,bigint_col,bit_col,bool_col,boolean_col,char_col,date_col,datetime_col,decimal_col,double_col,float_col,int_col,integer_col,mediumint_col,numeric_col,real_col,smallint_col,text_col,time_col,timestamp_col,tinyint_col,varchar_col
        1,1000000000,1,true,false,A,1985-01-01,1985-01-01T12:00:00,123.45,123.456789,12.34,100,101,1000,987.65,45.67,10,Sample text 1,12:34:56,1985-01-01T12:34:56,1,Short text 1
        2,,,,,,,,,,,,,,,,,,,,,
    """,
        table_config.name,
        base_config.tables,
    )

    df_read, df_read_history = modify_and_read(
        engine, df_2, table_schema, table_config, ts_2, "upload"
    )

    df_expected = from_test_result(
        """
        id,bigint_col,bit_col,bool_col,boolean_col,char_col,date_col,datetime_col,decimal_col,double_col,float_col,int_col,integer_col,mediumint_col,numeric_col,real_col,smallint_col,text_col,time_col,timestamp_col,tinyint_col,varchar_col,__valid_from,__valid_to
        1,1000000000,1,true,false,A,1985-01-01,1985-01-01T12:00:00,123.45,123.456789,12.34,100,101,1000,987.65,45.67,10,Sample text 1,12:34:56,1985-01-01T12:34:56,1,Short text 1,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
        2,9223372036854775807,0,false,false,?,1985-10-26,1985-10-26T01:21:00.000000,2.71,1.6180339887,1.1,1,1,1,3.14159,0.0,1,?abcdef?,01:20:00.000000000,1985-10-26T01:20:00.000000,1,?abcdefghijklmn?,1986-01-01T00:00:01.000000,2106-02-07T06:28:15.999999
    """,
        table_config.name,
        base_config.tables,
    )

    # using server-side defaults, the loaded df will have nulls
    diff_df, missing_cols = compare_dataframes(
        df_2,
        df_expected,
        on=["id"],
    )
    assert len(diff_df) == 1
    assert len(missing_cols) == 2

    diff_df, missing_cols = compare_dataframes(
        df_read,
        df_expected,
        on=["id"],
    )
    assert diff_df.is_empty()
    assert len(missing_cols) == 0
