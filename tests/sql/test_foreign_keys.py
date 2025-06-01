import pytest

from polars_hist_db.core.table_config import TableConfigOps
from polars_hist_db.config import TableConfig
from polars_hist_db.core.table import TableOps
from tests.utils.dsv_helper import setup_fixture_dataset


@pytest.fixture
def fixture_with_column_selection():
    yield from setup_fixture_dataset("trading_pairs.yaml")


@pytest.fixture
def fixture_with_linked_tables():
    yield from setup_fixture_dataset("trading_pairs.yaml")


def test_foreign_keys(fixture_with_linked_tables):
    engine, config = fixture_with_linked_tables
    table_schema = config.tables.schemas()[0]
    table_configs = config.tables

    expected_exchange_config = table_configs["exchanges"]
    expected_cryptocurrency_config = table_configs["cryptocurrencies"]
    expected_trading_pair_configs = table_configs["trading_pairs"]

    with engine.begin() as connection:
        read_exchange_config = TableConfigOps(connection).from_table(
            table_schema, "exchanges"
        )
        read_cryptocurrencies_config = TableConfigOps(connection).from_table(
            table_schema, "cryptocurrencies"
        )
        read_trading_pair_config = TableConfigOps(connection).from_table(
            table_schema, "trading_pairs"
        )

    assert isinstance(read_exchange_config, TableConfig)
    assert isinstance(read_cryptocurrencies_config, TableConfig)
    assert isinstance(read_trading_pair_config, TableConfig)

    assert (
        read_exchange_config.foreign_keys == expected_exchange_config.foreign_keys == []
    )
    assert (
        read_cryptocurrencies_config.foreign_keys
        == expected_cryptocurrency_config.foreign_keys
        == []
    )

    expected_fks = {fk.name: fk for fk in expected_trading_pair_configs.foreign_keys}
    read_fks = {fk.name: fk for fk in read_trading_pair_config.foreign_keys}

    for ex_fk_name, ex_fk in expected_fks.items():
        assert ex_fk_name in read_fks
        read_fk = read_fks[ex_fk_name]

        assert read_fk.name == ex_fk_name
        assert read_fk.references.table == ex_fk.references.table.name
        assert read_fk.references.column == ex_fk.references.column


def test_column_selection(fixture_with_column_selection):
    engine, config = fixture_with_column_selection
    table_schema = config.tables.schemas()[0]
    table_configs = config.tables
    table_config = table_configs["exchanges"]

    all_column_defs = table_config.columns

    with engine.begin() as connection:
        tbo = TableOps(table_schema, table_config.name, connection)
        tbl = tbo.get_table_metadata()
        read_cols = tbl.columns

    assert len(all_column_defs) == len(read_cols) == 5
