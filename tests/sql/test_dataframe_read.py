import pytest
import polars as pl
from sqlalchemy import select

from polars_hist_db.core.dataframe import DataframeOps

from polars_hist_db.core.table import TableOps
from tests.utils import (
    from_test_result,
    modify_and_read,
    setup_fixture_tableconfigs,
)


@pytest.fixture
def fixutre_with_simple_table():
    yield from setup_fixture_tableconfigs("simple_nontemporal_table.yaml")


def test_select_sql(fixutre_with_simple_table):
    engine, table_configs, table_schema = fixutre_with_simple_table
    table_config = table_configs.table_configs[0]

    def _upload_df(df):
        df, _ = modify_and_read(engine, df, table_schema, table_config, None, "upload")

        return df

    # upload then test initial df
    df_1 = pl.from_dict({"id": [1], "col_double": [123.4567], "col_varchar": ["abc"]})

    df_read = _upload_df(df_1)
    df_expected = from_test_result(
        """
        id, double_col, varchar_col
        1, 123.4567, abc
    """,
        table_config,
    )

    assert df_expected.equals(df_read)

    # update from dataframe
    df_2 = pl.from_dict({"id": [1], "col_double": [234.5678], "col_varchar": ["def"]})

    df_read = _upload_df(df_2)
    df_expected = from_test_result(
        """
        id, double_col, varchar_col
        1, 234.5678, def
    """,
        table_config,
    )

    assert df_expected.equals(df_read)

    # read using raw sql
    _sql = f"select * from {table_schema}.{table_config.name}"
    with engine.begin() as connection:
        df_read = DataframeOps(connection).from_raw_sql(_sql)

    df_expected = from_test_result(
        """
        id, double_col, varchar_col
        1, 234.5678, def
    """,
        table_config,
    )

    assert df_expected.equals(df_read)

    # incremental update nulls in database
    df_3 = pl.from_dict({"id": [1], "col_double": [None], "col_varchar": [None]})

    df_read = _upload_df(df_3)
    df_expected = from_test_result(
        """
        id, double_col, varchar_col
        1,,
    """,
        table_config,
    )

    assert df_expected.equals(df_read)

    df_4 = pl.from_dict(
        {"id": [1], "col_double": [345.67890001], "col_varchar": ["ghi"]}
    )

    df_read = _upload_df(df_4)
    df_expected = from_test_result(
        """
        id, double_col, varchar_col
        1, 345.67890001, ghi
    """,
        table_config,
    )

    assert df_expected.equals(df_read)

    # read empty dataframe using raw sql
    _sql = f"select * from {table_schema}.{table_config.name} where 1=0"
    with engine.begin() as connection:
        df_read = DataframeOps(connection).from_raw_sql(_sql)

    assert df_read.is_empty()

    # read empty dataframe using selectable
    with engine.begin() as connection:
        tbo = TableOps(table_schema, table_config.name, connection)
        tbl = tbo.get_table_metadata()
        select_sql = select(tbl).where("1" == "0")

        df_read = DataframeOps(connection).from_selectable(select_sql)

    assert df_read.is_empty()
