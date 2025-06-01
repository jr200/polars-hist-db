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
def fixture_with_nullable():
    yield from setup_fixture_dataset("all_col_types_nullable.yaml")


@pytest.fixture
def fixture_with_defaults():
    yield from setup_fixture_dataset("all_col_types_defaults.yaml")


def test_types_nullable(fixture_with_nullable):
    engine, config = fixture_with_nullable
    table_schema = config.tables.schemas()[0]
    table_configs = config.tables
    table_config = config.tables.items[0]
    table_config.delta_config.drop_unchanged_rows = True

    # upload then test initial df
    ts_1 = datetime.fromisoformat("1985-01-01T00:00:01Z")
    df_1 = from_test_result(
        """
        id,bigint_col,bit_col,bool_col,boolean_col,char_col,date_col,datetime_col,decimal_col,double_col,float_col,int_col,integer_col,mediumint_col,numeric_col,real_col,smallint_col,text_col,time_col,timestamp_col,tinyint_col,varchar_col
        1,1000000000,1,true,false,A,1985-01-01,1985-01-01T12:00:00,123.45,123.456789,12.34,100,101,1000,987.65,45.67,10,Sample text 1,12:34:56,1985-01-01T12:34:56,1,Short text 1
        2,2000000000,0,false,true,B,1985-01-02,1985-01-02T13:00:00,234.56,234.567890,23.45,200,201,2000,876.54,56.78,20,Sample text 2,13:45:57,1985-01-02T13:45:57,2,Short text 2
        3,3000000000,1,true,false,C,1985-01-03,1985-01-03T14:00:00,345.67,345.678901,34.56,300,301,3000,765.43,67.89,30,Sample text 3,14:56:58,1985-01-03T14:56:58,3,Short text 3
    """,
        table_config.name,
        table_configs,
    )

    df_read, df_read_history = modify_and_read(
        engine, df_1, table_schema, table_config, ts_1, "upload"
    )

    df_expected = from_test_result(
        """
        id,bigint_col,bit_col,bool_col,boolean_col,char_col,date_col,datetime_col,decimal_col,double_col,float_col,int_col,integer_col,mediumint_col,numeric_col,real_col,smallint_col,text_col,time_col,timestamp_col,tinyint_col,varchar_col,__valid_from,__valid_to
        1,1000000000,1,true,false,A,1985-01-01,1985-01-01T12:00:00,123.45,123.456789,12.34,100,101,1000,987.65,45.67,10,Sample text 1,12:34:56,1985-01-01T12:34:56,1,Short text 1,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
        2,2000000000,0,false,true,B,1985-01-02,1985-01-02T13:00:00,234.56,234.567890,23.45,200,201,2000,876.54,56.78,20,Sample text 2,13:45:57,1985-01-02T13:45:57,2,Short text 2,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
        3,3000000000,1,true,false,C,1985-01-03,1985-01-03T14:00:00,345.67,345.678901,34.56,300,301,3000,765.43,67.89,30,Sample text 3,14:56:58,1985-01-03T14:56:58,3,Short text 3,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
    """,
        table_config.name,
        table_configs,
    )

    assert_frame_equal(df_expected, df_read)
    assert df_read_history.is_empty()

    # insert the same dataframe, should be no inserts
    ts_2 = datetime.fromisoformat("1986-01-01T00:00:01Z")
    df_read, df_read_history = modify_and_read(
        engine, df_1, table_schema, table_config, ts_2, "upload"
    )

    assert_frame_equal(df_expected, df_read)
    assert df_read_history.is_empty()

    # insert a dataframe with one new row
    ts_3 = datetime.fromisoformat("1987-01-01T00:00:01Z")
    df_3 = from_test_result(
        """
        id,bigint_col,bit_col,bool_col,boolean_col,char_col,date_col,datetime_col,decimal_col,double_col,float_col,int_col,integer_col,mediumint_col,numeric_col,real_col,smallint_col,text_col,time_col,timestamp_col,tinyint_col,varchar_col
        1,1000000000,1,true,false,A,1985-01-01,1985-01-01T12:00:00,123.45,123.456789,12.34,100,101,1000,987.65,45.67,10,Sample text 1,12:34:56,1985-01-01T12:34:56,1,Short text 1
        2,2000000000,0,false,true,B,1985-01-02,1985-01-02T13:00:00,234.56,234.567890,23.45,200,201,2000,876.54,56.78,20,Sample text 2,13:45:57,1985-01-02T13:45:57,2,Short text 2
        3,3000000000,1,true,false,C,1985-01-03,1985-01-03T14:00:00,345.67,345.678901,34.56,300,301,3000,765.43,67.89,30,Sample text 3,14:56:58,1985-01-03T14:56:58,3,Short text 3
        4,4000000000,0,true,true,D,1987-01-04,1987-01-04T15:00:00,456.78,456.789012,45.67,400,401,4000,654.32,78.90,40,Sample text 4,15:57:59,1987-01-04T15:57:59,4,Short text 4
    """,
        table_config.name,
        table_configs,
    )

    df_read, df_read_history = modify_and_read(
        engine, df_3, table_schema, table_config, ts_3, "upload"
    )

    df_expected = from_test_result(
        """
        id,bigint_col,bit_col,bool_col,boolean_col,char_col,date_col,datetime_col,decimal_col,double_col,float_col,int_col,integer_col,mediumint_col,numeric_col,real_col,smallint_col,text_col,time_col,timestamp_col,tinyint_col,varchar_col,__valid_from,__valid_to
        1,1000000000,1,true,false,A,1985-01-01,1985-01-01T12:00:00,123.45,123.456789,12.34,100,101,1000,987.65,45.67,10,Sample text 1,12:34:56,1985-01-01T12:34:56,1,Short text 1,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
        2,2000000000,0,false,true,B,1985-01-02,1985-01-02T13:00:00,234.56,234.567890,23.45,200,201,2000,876.54,56.78,20,Sample text 2,13:45:57,1985-01-02T13:45:57,2,Short text 2,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
        3,3000000000,1,true,false,C,1985-01-03,1985-01-03T14:00:00,345.67,345.678901,34.56,300,301,3000,765.43,67.89,30,Sample text 3,14:56:58,1985-01-03T14:56:58,3,Short text 3,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
        4,4000000000,0,true,true,D,1987-01-04,1987-01-04T15:00:00,456.78,456.789012,45.67,400,401,4000,654.32,78.90,40,Sample text 4,15:57:59,1987-01-04T15:57:59,4,Short text 4,1987-01-01T00:00:01,2106-02-07T06:28:15.999999
    """,
        table_config.name,
        table_configs,
    )

    assert_frame_equal(df_expected, df_read)
    assert df_read_history.is_empty()

    # insert a dataframe with one updated row
    ts_4 = datetime.fromisoformat("1988-01-01T00:00:01Z")
    df_4 = from_test_result(
        """
        id,bigint_col,bit_col,bool_col,boolean_col,char_col,date_col,datetime_col,decimal_col,double_col,float_col,int_col,integer_col,mediumint_col,numeric_col,real_col,smallint_col,text_col,time_col,timestamp_col,tinyint_col,varchar_col
        1,1000000001,0,false,true,Z,1988-01-01,1988-01-01T12:00:00,123.46,123.456790,12.35,101,102,1001,987.66,45.68,11,Updated text 1,12:34:57,1988-01-01T12:34:57,2,Updated short 1
        2,2000000000,0,false,true,B,1985-01-02,1985-01-02T13:00:00,234.56,234.567890,23.45,200,201,2000,876.54,56.78,20,Sample text 2,13:45:57,1985-01-02T13:45:57,2,Short text 2
        3,3000000000,1,true,false,C,1985-01-03,1985-01-03T14:00:00,345.67,345.678901,34.56,300,301,3000,765.43,67.89,30,Sample text 3,14:56:58,1985-01-03T14:56:58,3,Short text 3
        4,4000000000,0,true,true,D,1987-01-04,1987-01-04T15:00:00,456.78,456.789012,45.67,400,401,4000,654.32,78.90,40,Sample text 4,15:57:59,1987-01-04T15:57:59,4,Short text 4
    """,
        table_config.name,
        table_configs,
    )

    df_read, df_read_history = modify_and_read(
        engine, df_4, table_schema, table_config, ts_4, "upload"
    )

    df_expected = from_test_result(
        """
        id,bigint_col,bit_col,bool_col,boolean_col,char_col,date_col,datetime_col,decimal_col,double_col,float_col,int_col,integer_col,mediumint_col,numeric_col,real_col,smallint_col,text_col,time_col,timestamp_col,tinyint_col,varchar_col,__valid_from,__valid_to
        1,1000000001,0,false,true,Z,1988-01-01,1988-01-01T12:00:00,123.46,123.456790,12.35,101,102,1001,987.66,45.68,11,Updated text 1,12:34:57,1988-01-01T12:34:57,2,Updated short 1,1988-01-01T00:00:01,2106-02-07T06:28:15.999999
        2,2000000000,0,false,true,B,1985-01-02,1985-01-02T13:00:00,234.56,234.567890,23.45,200,201,2000,876.54,56.78,20,Sample text 2,13:45:57,1985-01-02T13:45:57,2,Short text 2,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
        3,3000000000,1,true,false,C,1985-01-03,1985-01-03T14:00:00,345.67,345.678901,34.56,300,301,3000,765.43,67.89,30,Sample text 3,14:56:58,1985-01-03T14:56:58,3,Short text 3,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
        4,4000000000,0,true,true,D,1987-01-04,1987-01-04T15:00:00,456.78,456.789012,45.67,400,401,4000,654.32,78.90,40,Sample text 4,15:57:59,1987-01-04T15:57:59,4,Short text 4,1987-01-01T00:00:01,2106-02-07T06:28:15.999999
    """,
        table_config.name,
        table_configs,
    )

    df_expected_history = from_test_result(
        """
        id,bigint_col,bit_col,bool_col,boolean_col,char_col,date_col,datetime_col,decimal_col,double_col,float_col,int_col,integer_col,mediumint_col,numeric_col,real_col,smallint_col,text_col,time_col,timestamp_col,tinyint_col,varchar_col,__valid_from,__valid_to
        1,1000000000,1,true,false,A,1985-01-01,1985-01-01T12:00:00,123.45,123.456789,12.34,100,101,1000,987.65,45.67,10,Sample text 1,12:34:56,1985-01-01T12:34:56,1,Short text 1,1985-01-01T00:00:01,1988-01-01T00:00:01
    """,
        table_config.name,
        table_configs,
    )

    assert_frame_equal(df_expected, df_read)
    assert_frame_equal(df_expected_history, df_read_history)

    # insert a dataframe with nulls
    ts_5 = datetime.fromisoformat("1989-01-01T00:00:01Z")
    df_5 = from_test_result(
        """
        id,bigint_col,bit_col,bool_col,boolean_col,char_col,date_col,datetime_col,decimal_col,double_col,float_col,int_col,integer_col,mediumint_col,numeric_col,real_col,smallint_col,text_col,time_col,timestamp_col,tinyint_col,varchar_col
        1,1000000001,0,false,true,Z,1988-01-01,1988-01-01T12:00:00,123.46,123.456790,12.35,101,102,1001,987.66,45.68,11,Updated text 1,12:34:57,1988-01-01T12:34:57,2,Updated short 1
        2,2000000000,0,false,true,B,1985-01-02,1985-01-02T13:00:00,234.56,234.567890,23.45,200,201,2000,876.54,56.78,20,Sample text 2,13:45:57,1985-01-02T13:45:57,2,Short text 2
        3,3000000000,1,true,false,C,1985-01-03,1985-01-03T14:00:00,345.67,345.678901,34.56,300,301,3000,765.43,67.89,30,Sample text 3,14:56:58,1985-01-03T14:56:58,3,Short text 3
        4,4000000000,0,true,true,D,1987-01-04,1987-01-04T15:00:00,456.78,456.789012,45.67,400,401,4000,654.32,78.90,40,Sample text 4,15:57:59,1987-01-04T15:57:59,4,Short text 4
        5,,,,,,,,,,,,,,,,,,,,,
    """,
        table_config.name,
        table_configs,
    )

    df_read, df_read_history = modify_and_read(
        engine, df_5, table_schema, table_config, ts_5, "upload"
    )

    df_expected = from_test_result(
        """
        id,bigint_col,bit_col,bool_col,boolean_col,char_col,date_col,datetime_col,decimal_col,double_col,float_col,int_col,integer_col,mediumint_col,numeric_col,real_col,smallint_col,text_col,time_col,timestamp_col,tinyint_col,varchar_col,__valid_from,__valid_to
        1,1000000001,0,false,true,Z,1988-01-01,1988-01-01T12:00:00,123.46,123.456790,12.35,101,102,1001,987.66,45.68,11,Updated text 1,12:34:57,1988-01-01T12:34:57,2,Updated short 1,1988-01-01T00:00:01,2106-02-07T06:28:15.999999
        2,2000000000,0,false,true,B,1985-01-02,1985-01-02T13:00:00,234.56,234.567890,23.45,200,201,2000,876.54,56.78,20,Sample text 2,13:45:57,1985-01-02T13:45:57,2,Short text 2,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
        3,3000000000,1,true,false,C,1985-01-03,1985-01-03T14:00:00,345.67,345.678901,34.56,300,301,3000,765.43,67.89,30,Sample text 3,14:56:58,1985-01-03T14:56:58,3,Short text 3,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
        4,4000000000,0,true,true,D,1987-01-04,1987-01-04T15:00:00,456.78,456.789012,45.67,400,401,4000,654.32,78.90,40,Sample text 4,15:57:59,1987-01-04T15:57:59,4,Short text 4,1987-01-01T00:00:01,2106-02-07T06:28:15.999999
        5,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,1989-01-01T00:00:01,2106-02-07T06:28:15.999999
    """,
        table_config.name,
        table_configs,
    )

    assert_frame_equal(df_expected, df_read)


def test_types_defaults(fixture_with_defaults):
    engine, config = fixture_with_defaults
    table_schema = config.tables.schemas()[0]
    table_configs = config.tables
    table_config = config.tables.items[0]

    # upload then test initial df
    ts_1 = datetime.fromisoformat("1985-01-01T00:00:01Z")
    df_1 = from_test_result(
        """
        id,bigint_col,bit_col,bool_col,boolean_col,char_col,date_col,datetime_col,decimal_col,double_col,float_col,int_col,integer_col,mediumint_col,numeric_col,real_col,smallint_col,text_col,time_col,timestamp_col,tinyint_col,varchar_col
        1,1000000000,1,true,false,A,1985-01-01,1985-01-01T12:00:00,123.45,123.456789,12.34,100,101,1000,987.65,45.67,10,Sample text 1,12:34:56,1985-01-01T12:34:56,1,Short text 1
    """,
        table_config.name,
        table_configs,
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
        table_configs,
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
        table_configs,
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
        table_configs,
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
