from polars_hist_db.config import (
    TableColumnConfig,
    DeltaConfig,
    ForeignKeyConfig,
    TableConfig,
    TableConfigs,
)
from ..utils.dsv_helper import get_test_config


def assert_table_config_object(table_config: TableConfig):
    assert isinstance(table_config, TableConfig)
    if table_config.delta_config:
        assert isinstance(table_config.delta_config, DeltaConfig)

    for column in table_config.columns:
        assert isinstance(column, TableColumnConfig)

    for primary_key in table_config.primary_keys:
        assert isinstance(primary_key, str)

    for foreign_key in table_config.foreign_keys:
        assert isinstance(foreign_key, ForeignKeyConfig)
        ref = foreign_key.references
        assert isinstance(ref, ForeignKeyConfig.References)


def test_cryptocurrency_config_deserialization():
    file_path = get_test_config("trading_pairs.yaml")
    config = TableConfigs.from_yamls(file_path)["cryptocurrencies"]
    assert_table_config_object(config)

    assert config.name == "cryptocurrencies"
    assert len(config.columns) == 6
    assert config.columns[0].table == config.name
    assert config.columns[0].name == "id"
    assert config.columns[0].data_type == "INT"
    assert config.columns[0].autoincrement
    assert not config.columns[0].nullable
    assert config.primary_keys == ["id"]
    assert config.columns[3].name == "current_price_usd"
    assert config.columns[3].data_type == "DECIMAL(18,8)"


def test_exchange_config_deserialization():
    file_path = get_test_config("trading_pairs.yaml")
    config = TableConfigs.from_yamls(file_path)["exchanges"]
    assert_table_config_object(config)

    assert config.name == "exchanges"
    assert len(config.columns) == 5
    assert config.columns[1].name == "name"
    assert config.columns[1].data_type == "VARCHAR(100)"
    assert not config.columns[1].nullable


def test_trading_pairs_config_deserialization():
    file_path = get_test_config("trading_pairs.yaml")
    configs = TableConfigs.from_yamls(file_path)

    exchange_config = configs["exchanges"]
    cryptocurrency_config = configs["cryptocurrencies"]
    config = configs["trading_pairs"]

    assert_table_config_object(config)

    assert config.name == "trading_pairs"
    assert len(config.columns) == 7
    assert len(config.foreign_keys) == 3

    assert config.foreign_keys[0].references.table == cryptocurrency_config
    assert config.foreign_keys[0].references.column == "id"
    assert config.foreign_keys[0].name == "base_currency_id"

    assert config.foreign_keys[1].references.table == cryptocurrency_config
    assert config.foreign_keys[1].references.column == "id"
    assert config.foreign_keys[1].name == "quote_currency_id"

    assert config.foreign_keys[2].references.table == exchange_config
    assert config.foreign_keys[2].references.column == "id"
    assert config.foreign_keys[2].name == "exchange_id"

    assert isinstance(config.delta_config, DeltaConfig)
    assert config.is_temporal


# def test_trading_pairs_config_serialization_deserialization():
# file_path = get_test_config("table_cryptocurrencies.yaml")
# original_config = TableConfig.from_yaml(file_path)

# Serialize to YAML
# serialized = yaml.dump(original_config)

# Deserialize from YAML
# deserialized_config = yaml.safe_load(serialized)

#     assert isinstance(deserialized_config, TableConfig)
#     assert deserialized_config.name == original_config.name
#     assert len(deserialized_config.columns) == len(original_config.columns)
#     assert deserialized_config.columns[0].name == original_config.columns[0].name
#     assert deserialized_config.columns[0].data_type == original_config.columns[0].data_type
#     assert len(deserialized_config.foreign_keys) == len(original_config.foreign_keys)
#     assert deserialized_config.foreign_keys[0].column == original_config.foreign_keys[0].column
#     assert deserialized_config.foreign_keys[0].references.table == original_config.foreign_keys[0].references.table
#     assert deserialized_config.delta_config.drop_unchanged_rows == original_config.delta_config.drop_unchanged_rows
#     assert deserialized_config.is_temporal == original_config.is_temporal

# def test_config_roundtrip():
#     for filename in ['table_cryptocurrencies.yaml', 'table_exchanges.yaml', 'table_trading_pairs.yaml']:
#         original_config_dict = get_test_path(filename)

#         # Serialize to YAML
#         serialized = yaml.dump(original_config)

#         # Deserialize from YAML
#         deserialized_config = yaml.safe_load(serialized)

#         # Serialize again
#         reserialized = yaml.dump(deserialized_config)

#         # Compare the two serialized strings
#         assert serialized == reserialized, f"Roundtrip failed for {filename}"
