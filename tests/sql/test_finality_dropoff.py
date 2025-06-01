from datetime import datetime
import pytest
import polars as pl
from polars.testing import assert_frame_equal

from ..utils.dsv_helper import (
    from_test_result,
    modify_and_read,
    setup_fixture_dataset,
)


@pytest.fixture
def fixture_with_simple_table():
    yield from setup_fixture_dataset("simple.yaml")


def test_dataframe_reinsert(fixture_with_simple_table):
    engine, config = fixture_with_simple_table
    table_schema = config.tables.schemas()[0]
    table_configs = config.tables
    table_config = config.tables.items[0]

    # upload then test initial df
    ts_1 = datetime.fromisoformat("1985-01-01T00:00:01Z")
    df_1 = from_test_result(
        """
        id,double_col,varchar_col
        1,123.456,aaaa
    """,
        table_config.name,
        table_configs,
    )

    df_read, df_read_history = modify_and_read(
        engine, df_1, table_schema, table_config, ts_1, "upload"
    )

    df_expected = from_test_result(
        """
        id,double_col,varchar_col,__valid_from,__valid_to
        1,123.456,aaaa,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
    """,
        table_config.name,
        table_configs,
    )

    assert_frame_equal(df_expected, df_read)
    assert df_read_history.is_empty()

    # delete row
    ts_2 = datetime.fromisoformat("1986-01-01T00:00:01Z")
    df_2 = df_1.clear()

    df_read, df_read_history = modify_and_read(
        engine, df_2, table_schema, table_config, ts_2, "upload"
    )

    df_expected = from_test_result(
        """
        id,double_col,varchar_col,__valid_from,__valid_to
    """,
        table_config.name,
        table_configs,
    )

    df_expected_history = from_test_result(
        """
        id,double_col,varchar_col,__valid_from,__valid_to
        1,123.456,aaaa,1985-01-01T00:00:01,1986-01-01T00:00:01
    """,
        table_config.name,
        table_configs,
    )

    assert_frame_equal(df_expected, df_read)
    assert_frame_equal(df_expected_history, df_read_history)

    # reinsert row
    ts_3 = datetime.fromisoformat("1987-01-01T00:00:01Z")
    df_3 = from_test_result(
        """
        id,double_col,varchar_col
        1,234.567,bbbb
    """,
        table_config.name,
        table_configs,
    )

    df_read, df_read_history = modify_and_read(
        engine, df_3, table_schema, table_config, ts_3, "upload"
    )

    df_expected = from_test_result(
        """
        id,double_col,varchar_col,__valid_from,__valid_to
        1,234.567,bbbb,1987-01-01T00:00:01,2106-02-07T06:28:15.999999
    """,
        table_config.name,
        table_configs,
    )

    df_expected_history = from_test_result(
        """
        id,double_col,varchar_col,__valid_from,__valid_to
        1,123.456,aaaa,1985-01-01T00:00:01,1986-01-01T00:00:01
    """,
        table_config.name,
        table_configs,
    )

    assert_frame_equal(df_expected, df_read)
    assert_frame_equal(df_expected_history, df_read_history)


def test_dataframe_upsert(fixture_with_simple_table):
    engine, config = fixture_with_simple_table
    table_schema = config.tables.schemas()[0]
    table_configs = config.tables
    table_config = config.tables.items[0]

    # upload then test initial df
    ts_1 = datetime.fromisoformat("1985-01-01T00:00:01Z")
    df_1 = pl.from_dict(
        {
            "id": [1, 2, 3, 4, 5, 6, 7, 8],
            "double_col": [
                100.100,
                -200.200,
                None,
                400.400,
                -500.500,
                600.600,
                700.700,
                -800.800,
            ],
        }
    )

    df_read, df_read_history = modify_and_read(
        engine, df_1, table_schema, table_config, ts_1, "upload"
    )

    df_expected = from_test_result(
        """
        id,double_col,varchar_col,__valid_from,__valid_to
        1, 100.100,None,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
        2,-200.200,None,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
        3,        ,None,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
        4, 400.400,None,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
        5,-500.500,None,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
        6, 600.600,None,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
        7, 700.700,None,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
        8,-800.800,None,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
    """,
        table_config.name,
        table_configs,
    )

    assert_frame_equal(df_expected, df_read)
    assert df_read_history.is_empty()

    # upload the same df, same asof
    df_read, df_read_history = modify_and_read(
        engine, df_1, table_schema, table_config, ts_1, "upload"
    )
    assert_frame_equal(df_expected, df_read)
    assert df_read_history.is_empty()

    # upload the same df, different asof
    ts_2 = datetime.fromisoformat("1986-01-01T00:00:01Z")
    df_read, df_read_history = modify_and_read(
        engine, df_1, table_schema, table_config, ts_2, "upload"
    )
    assert_frame_equal(df_expected, df_read)
    assert df_read_history.is_empty()

    # update a single row
    ts_3 = datetime.fromisoformat("1987-01-01T00:00:01Z")
    df_3 = pl.from_dict(
        {
            "id": [1, 2, 3, 4, 5, 6, 7, 8],
            "double_col": [
                100.100,
                -200.200,
                None,
                None,
                -500.500,
                600.600,
                700.700,
                -800.800,
            ],
        }
    )

    df_read, df_read_history = modify_and_read(
        engine, df_3, table_schema, table_config, ts_3, "upload"
    )

    df_expected = from_test_result(
        """
        id,double_col,varchar_col,__valid_from,__valid_to
        1, 100.100,None,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
        2,-200.200,None,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
        3,        ,None,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
        4,        ,None,1987-01-01T00:00:01,2106-02-07T06:28:15.999999
        5,-500.500,None,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
        6, 600.600,None,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
        7, 700.700,None,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
        8,-800.800,None,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
    """,
        table_config.name,
        table_configs,
    )

    df_expected_history = from_test_result(
        """
        id,double_col,varchar_col,__valid_from,__valid_to
        4, 400.400,None,1985-01-01T00:00:01,1987-01-01T00:00:01
    """,
        table_config.name,
        table_configs,
    )

    assert_frame_equal(df_expected, df_read)
    assert_frame_equal(df_expected_history, df_read_history)

    # delete a single non-null row
    ts_4 = datetime.fromisoformat("1988-01-01T00:00:01Z")
    df_4 = pl.from_dict(
        {
            "id": [1, 2, 3, 4, 6, 7, 8],
            "double_col": [100.100, -200.200, None, None, 600.600, 700.700, -800.800],
        }
    )

    df_read, df_read_history = modify_and_read(
        engine, df_4, table_schema, table_config, ts_4, "upload"
    )

    df_expected = from_test_result(
        """
        id,double_col,varchar_col,__valid_from,__valid_to
        1, 100.100,None,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
        2,-200.200,None,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
        3,        ,None,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
        4,        ,None,1987-01-01T00:00:01,2106-02-07T06:28:15.999999
        6, 600.600,None,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
        7, 700.700,None,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
        8,-800.800,None,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
    """,
        table_config.name,
        table_configs,
    )

    df_expected_history = from_test_result(
        """
        id,double_col,varchar_col,__valid_from,__valid_to
        4, 400.400,None,1985-01-01T00:00:01,1987-01-01T00:00:01
        5,-500.500,None,1985-01-01T00:00:01,1988-01-01T00:00:01
    """,
        table_config.name,
        table_configs,
    )

    assert_frame_equal(df_expected, df_read)
    assert_frame_equal(df_expected_history, df_read_history)

    # delete a single non-null row, and update an existing row
    ts_5 = datetime.fromisoformat("1989-01-01T00:00:01Z")
    df_5 = pl.from_dict(
        {
            "id": [1, 2, 3, 4, 7, 8],
            "double_col": [100.100, -200.200, None, 444.444, 700.700, -800.800],
        }
    )

    df_read, df_read_history = modify_and_read(
        engine, df_5, table_schema, table_config, ts_5, "upload"
    )

    df_expected = from_test_result(
        """
        id,double_col,varchar_col,__valid_from,__valid_to
        1, 100.100,,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
        2,-200.200,,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
        3,      ,,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
        4, 444.444,,1989-01-01T00:00:01,2106-02-07T06:28:15.999999
        7, 700.700,,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
        8,-800.800,,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
    """,
        table_config.name,
        table_configs,
    )

    df_expected_history = from_test_result(
        """
        id,double_col,varchar_col,__valid_from,__valid_to
        4, 400.400,None,1985-01-01T00:00:01,1987-01-01T00:00:01
        4,        ,None,1987-01-01T00:00:01,1989-01-01T00:00:01
        5,-500.500,None,1985-01-01T00:00:01,1988-01-01T00:00:01
        6, 600.600,None,1985-01-01T00:00:01,1989-01-01T00:00:01
    """,
        table_config.name,
        table_configs,
    )

    assert_frame_equal(df_expected, df_read)
    assert_frame_equal(df_expected_history, df_read_history)

    # insert a new non-null row
    ts_6 = datetime.fromisoformat("1990-01-01T00:00:01Z")
    df_6 = pl.from_dict(
        {
            "id": [1, 2, 3, 4, 7, 8, 9],
            "double_col": [
                100.100,
                -200.200,
                None,
                444.444,
                700.700,
                -800.800,
                -999.999,
            ],
        }
    )

    df_read, df_read_history = modify_and_read(
        engine, df_6, table_schema, table_config, ts_6, "upload"
    )

    df_expected = from_test_result(
        """
        id,double_col,varchar_col,__valid_from,__valid_to
        1, 100.100,None,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
        2,-200.200,None,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
        3,        ,None,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
        4, 444.444,None,1989-01-01T00:00:01,2106-02-07T06:28:15.999999
        7, 700.700,None,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
        8,-800.800,None,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
        9,-999.999,None,1990-01-01T00:00:01,2106-02-07T06:28:15.999999
    """,
        table_config.name,
        table_configs,
    )

    df_expected_history = from_test_result(
        """
        id,double_col,varchar_col,__valid_from,__valid_to
        4, 400.400,None,1985-01-01T00:00:01,1987-01-01T00:00:01
        4,        ,None,1987-01-01T00:00:01,1989-01-01T00:00:01
        5,-500.500,None,1985-01-01T00:00:01,1988-01-01T00:00:01
        6, 600.600,None,1985-01-01T00:00:01,1989-01-01T00:00:01
    """,
        table_config.name,
        table_configs,
    )

    assert_frame_equal(df_expected, df_read)
    assert_frame_equal(df_expected_history, df_read_history)

    # insert a new non-null row and delete a non-null row
    ts_7 = datetime.fromisoformat("1991-01-01T00:00:01Z")
    df_7 = pl.from_dict(
        {
            "id": [2, 3, 4, 7, 8, 9, 10],
            "double_col": [
                -200.200,
                None,
                444.444,
                700.700,
                -800.800,
                -999.999,
                111.111,
            ],
        }
    )

    df_read, df_read_history = modify_and_read(
        engine, df_7, table_schema, table_config, ts_7, "upload"
    )

    df_expected = from_test_result(
        """
        id,double_col,varchar_col,__valid_from,__valid_to
        2,-200.200,None,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
        3,        ,None,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
        4, 444.444,None,1989-01-01T00:00:01,2106-02-07T06:28:15.999999
        7, 700.700,None,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
        8,-800.800,None,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
        9,-999.999,None,1990-01-01T00:00:01,2106-02-07T06:28:15.999999
       10, 111.111,None,1991-01-01T00:00:01,2106-02-07T06:28:15.999999
    """,
        table_config.name,
        table_configs,
    )

    df_expected_history = from_test_result(
        """
        id,double_col,varchar_col,__valid_from,__valid_to
        1, 100.100,None,1985-01-01T00:00:01,1991-01-01T00:00:01
        4, 400.400,None,1985-01-01T00:00:01,1987-01-01T00:00:01
        4,        ,None,1987-01-01T00:00:01,1989-01-01T00:00:01
        5,-500.500,None,1985-01-01T00:00:01,1988-01-01T00:00:01
        6, 600.600,None,1985-01-01T00:00:01,1989-01-01T00:00:01
    """,
        table_config.name,
        table_configs,
    )

    assert_frame_equal(df_expected, df_read)
    assert_frame_equal(df_expected_history, df_read_history)

    # insert r1, delete r8, and update r2 operations together
    ts_8 = datetime.fromisoformat("1992-01-01T00:00:01Z")
    df_8 = pl.from_dict(
        {
            "id": [1, 2, 3, 4, 7, 9, 10],
            "double_col": [
                100.123,
                -200.234,
                None,
                444.444,
                700.700,
                -999.999,
                111.111,
            ],
        }
    )

    df_read, df_read_history = modify_and_read(
        engine, df_8, table_schema, table_config, ts_8, "upload"
    )

    df_expected = from_test_result(
        """
        id,double_col,varchar_col,__valid_from,__valid_to
        1, 100.123,None,1992-01-01T00:00:01,2106-02-07T06:28:15.999999
        2,-200.234,None,1992-01-01T00:00:01,2106-02-07T06:28:15.999999
        3,        ,None,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
        4, 444.444,None,1989-01-01T00:00:01,2106-02-07T06:28:15.999999
        7, 700.700,None,1985-01-01T00:00:01,2106-02-07T06:28:15.999999
        9,-999.999,None,1990-01-01T00:00:01,2106-02-07T06:28:15.999999
       10, 111.111,None,1991-01-01T00:00:01,2106-02-07T06:28:15.999999
    """,
        table_config.name,
        table_configs,
    )

    df_expected_history = from_test_result(
        """
        id,double_col,varchar_col,__valid_from,__valid_to
        1, 100.100,None,1985-01-01T00:00:01,1991-01-01T00:00:01
        2,-200.200,None,1985-01-01T00:00:01,1992-01-01T00:00:01
        4, 400.400,None,1985-01-01T00:00:01,1987-01-01T00:00:01
        4,       ,None,1987-01-01T00:00:01,1989-01-01T00:00:01
        5,-500.500,None,1985-01-01T00:00:01,1988-01-01T00:00:01
        6, 600.600,None,1985-01-01T00:00:01,1989-01-01T00:00:01
        8,-800.800,None,1985-01-01T00:00:01,1992-01-01T00:00:01
    """,
        table_config.name,
        table_configs,
    )

    assert_frame_equal(df_expected, df_read)
    assert_frame_equal(df_expected_history, df_read_history)
