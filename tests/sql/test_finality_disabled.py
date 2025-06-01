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


def test_dataframe_upsert(fixture_with_simple_table):
    engine, config = fixture_with_simple_table
    table_schema = config.tables.schemas()[0]
    table_configs = config.tables
    table_config = config.tables.items[0]

    assert table_config.delta_config is not None
    table_config.delta_config.row_finality = "disabled"

    # upload then test initial df
    ts_1 = datetime.fromisoformat("1985-01-01T00:00:01Z")
    df_1 = pl.from_dict(
        {
            "id": [1, 2, 3],
            "double_col": [100.100, -200.200, None],
            "varchar_col": [None, None, None],
        },
        schema_overrides={"varchar_col": pl.Utf8},
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
    """,
        table_config.name,
        table_configs,
    )

    assert_frame_equal(df_expected, df_read)
    assert df_read_history.is_empty()

    # update all rows setting upload time
    ts_2 = datetime.fromisoformat("1986-01-01T00:00:01Z")
    df_2 = pl.from_dict(
        {
            "id": [1, 2, 3],
            "double_col": [110.110, -220.220, 330.330],
            "varchar_col": [None, None, None],
        }
    )

    df_read, df_read_history = modify_and_read(
        engine, df_2, table_schema, table_config, ts_2, "upload"
    )

    df_expected = from_test_result(
        """
        id,double_col,varchar_col,__valid_from,__valid_to
        1, 110.110,None,1986-01-01T00:00:01,2106-02-07T06:28:15.999999
        2,-220.220,None,1986-01-01T00:00:01,2106-02-07T06:28:15.999999
        3, 330.330,None,1986-01-01T00:00:01,2106-02-07T06:28:15.999999
    """,
        table_config.name,
        table_configs,
    )

    df_expected_history = from_test_result(
        """
        id,double_col,varchar_col,__valid_from,__valid_to
        1, 100.100,None,1985-01-01T00:00:01,1986-01-01T00:00:01
        2,-200.200,None,1985-01-01T00:00:01,1986-01-01T00:00:01
        3,        ,None,1985-01-01T00:00:01,1986-01-01T00:00:01
    """,
        table_config.name,
        table_configs,
    )

    assert_frame_equal(df_expected, df_read)
    assert_frame_equal(df_expected_history, df_read_history)

    # update all rows setting upload time
    ts_3 = datetime.fromisoformat("1987-01-01T00:00:01Z")
    df_3 = pl.from_dict(
        {
            "id": [1, 2, 3],
            "double_col": [111.111, -222.222, 333.333],
            "varchar_col": [None, None, None],
        }
    )

    df_read, df_read_history = modify_and_read(
        engine, df_3, table_schema, table_config, ts_3, "upload"
    )

    df_expected = from_test_result(
        """
        id,double_col,varchar_col,__valid_from,__valid_to
        1, 111.111,None,1987-01-01T00:00:01,2106-02-07T06:28:15.999999
        2,-222.222,None,1987-01-01T00:00:01,2106-02-07T06:28:15.999999
        3, 333.333,None,1987-01-01T00:00:01,2106-02-07T06:28:15.999999
    """,
        table_config.name,
        table_configs,
    )

    df_expected_history = from_test_result(
        """
        id,double_col,varchar_col,__valid_from,__valid_to
        1, 100.100,None,1985-01-01T00:00:01,1986-01-01T00:00:01
        1, 110.110,None,1986-01-01T00:00:01,1987-01-01T00:00:01
        2,-200.200,None,1985-01-01T00:00:01,1986-01-01T00:00:01
        2,-220.220,None,1986-01-01T00:00:01,1987-01-01T00:00:01
        3,        ,None,1985-01-01T00:00:01,1986-01-01T00:00:01
        3, 330.330,None,1986-01-01T00:00:01,1987-01-01T00:00:01
    """,
        table_config.name,
        table_configs,
    )

    assert_frame_equal(df_expected, df_read)
    assert_frame_equal(df_expected_history, df_read_history)

    # update all rows setting upload time
    ts_4 = datetime.fromisoformat("1988-01-01T00:00:01Z")
    df_4 = pl.from_dict(
        {
            "id": [1, 2, 3],
            "double_col": [100.001, None, 300.003],
            "varchar_col": [None, None, None],
        }
    )

    df_read, df_read_history = modify_and_read(
        engine, df_4, table_schema, table_config, ts_4, "upload"
    )

    df_expected = from_test_result(
        """
        id,double_col,varchar_col,__valid_from,__valid_to
        1, 100.001,None,1988-01-01T00:00:01,2106-02-07T06:28:15.999999
        2,        ,None,1988-01-01T00:00:01,2106-02-07T06:28:15.999999
        3, 300.003,None,1988-01-01T00:00:01,2106-02-07T06:28:15.999999
    """,
        table_config.name,
        table_configs,
    )

    df_expected_history = from_test_result(
        """
        id,double_col,varchar_col,__valid_from,__valid_to
        1, 100.100,None,1985-01-01T00:00:01,1986-01-01T00:00:01
        1, 110.110,None,1986-01-01T00:00:01,1987-01-01T00:00:01
        1, 111.111,None,1987-01-01T00:00:01,1988-01-01T00:00:01
        2,-200.200,None,1985-01-01T00:00:01,1986-01-01T00:00:01
        2,-220.220,None,1986-01-01T00:00:01,1987-01-01T00:00:01
        2,-222.222,None,1987-01-01T00:00:01,1988-01-01T00:00:01
        3,        ,None,1985-01-01T00:00:01,1986-01-01T00:00:01
        3, 330.330,None,1986-01-01T00:00:01,1987-01-01T00:00:01
        3, 333.333,None,1987-01-01T00:00:01,1988-01-01T00:00:01
    """,
        table_config.name,
        table_configs,
    )

    assert_frame_equal(df_expected, df_read)
    assert_frame_equal(df_expected_history, df_read_history)

    # upload dataframe with no changes and same time
    df_read, df_read_history = modify_and_read(
        engine, df_4, table_schema, table_config, ts_4, "upload"
    )
    assert_frame_equal(df_expected, df_read)
    assert_frame_equal(df_expected_history, df_read_history)

    # update single row
    ts_5 = datetime.fromisoformat("1989-01-01T00:00:01Z")
    df_5 = pl.from_dict(
        {
            "id": [2],
            "double_col": [202.202],
            "varchar_col": [None],
        }
    )

    df_read, df_read_history = modify_and_read(
        engine, df_5, table_schema, table_config, ts_5, "upload"
    )

    df_expected = from_test_result(
        """
        id,double_col,varchar_col,__valid_from,__valid_to
        1, 100.001,None,1988-01-01T00:00:01,2106-02-07T06:28:15.999999
        2, 202.202,None,1989-01-01T00:00:01,2106-02-07T06:28:15.999999
        3, 300.003,None,1988-01-01T00:00:01,2106-02-07T06:28:15.999999
    """,
        table_config.name,
        table_configs,
    )

    df_expected_history = from_test_result(
        """
        id,double_col,varchar_col,__valid_from,__valid_to
        1, 100.100,None,1985-01-01T00:00:01,1986-01-01T00:00:01
        1, 110.110,None,1986-01-01T00:00:01,1987-01-01T00:00:01
        1, 111.111,None,1987-01-01T00:00:01,1988-01-01T00:00:01
        2,-200.200,None,1985-01-01T00:00:01,1986-01-01T00:00:01
        2,-220.220,None,1986-01-01T00:00:01,1987-01-01T00:00:01
        2,-222.222,None,1987-01-01T00:00:01,1988-01-01T00:00:01
        2,        ,None,1988-01-01T00:00:01,1989-01-01T00:00:01
        3,        ,None,1985-01-01T00:00:01,1986-01-01T00:00:01
        3, 330.330,None,1986-01-01T00:00:01,1987-01-01T00:00:01
        3, 333.333,None,1987-01-01T00:00:01,1988-01-01T00:00:01
    """,
        table_config.name,
        table_configs,
    )

    assert_frame_equal(df_expected, df_read)
    assert_frame_equal(df_expected_history, df_read_history)

    # update single row
    ts_6 = datetime.fromisoformat("1990-01-01T00:00:01Z")
    df_6 = pl.from_dict(
        {
            "id": [2, 3],
            "double_col": [234.234, 300.003],
            "varchar_col": [None, None],
        }
    )

    df_read, df_read_history = modify_and_read(
        engine, df_6, table_schema, table_config, ts_6, "upload"
    )

    df_expected = from_test_result(
        """
        id,double_col,varchar_col,__valid_from,__valid_to
        1, 100.001,None,1988-01-01T00:00:01,2106-02-07T06:28:15.999999
        2, 234.234,None,1990-01-01T00:00:01,2106-02-07T06:28:15.999999
        3, 300.003,None,1988-01-01T00:00:01,2106-02-07T06:28:15.999999
    """,
        table_config.name,
        table_configs,
    )

    df_expected_history = from_test_result(
        """
        id,double_col,varchar_col,__valid_from,__valid_to
        1, 100.100,None,1985-01-01T00:00:01,1986-01-01T00:00:01
        1, 110.110,None,1986-01-01T00:00:01,1987-01-01T00:00:01
        1, 111.111,None,1987-01-01T00:00:01,1988-01-01T00:00:01
        2,-200.200,None,1985-01-01T00:00:01,1986-01-01T00:00:01
        2,-220.220,None,1986-01-01T00:00:01,1987-01-01T00:00:01
        2,-222.222,None,1987-01-01T00:00:01,1988-01-01T00:00:01
        2,        ,None,1988-01-01T00:00:01,1989-01-01T00:00:01
        2, 202.202,None,1989-01-01T00:00:01,1990-01-01T00:00:01
        3,        ,None,1985-01-01T00:00:01,1986-01-01T00:00:01
        3, 330.330,None,1986-01-01T00:00:01,1987-01-01T00:00:01
        3, 333.333,None,1987-01-01T00:00:01,1988-01-01T00:00:01
    """,
        table_config.name,
        table_configs,
    )

    assert_frame_equal(df_expected, df_read)
    assert_frame_equal(df_expected_history, df_read_history)
