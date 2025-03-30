from functools import reduce
import os
from typing import Any, Iterable, Mapping, Optional
import yaml

from .dataset import DatasetsConfig
from .engine import DbEngineConfig
from .table import ColumnDefinitions, TableConfigs


class Config:
    def __init__(
        self,
        cfg_dict: Mapping[str, Any],
        column_definitions_path: Iterable[str],
        table_configs_path: Iterable[str],
        datasets_path: Iterable[str],
        db_config_path: Iterable[str],
        config_filename: Optional[str] = None,
    ):
        if db_config_path:
            self.db_config = DbEngineConfig(
                **Config.get_nested_key(cfg_dict, db_config_path)
            )

        if column_definitions_path and table_configs_path:
            column_defintions = ColumnDefinitions(
                column_definitions=Config.get_nested_key(
                    cfg_dict, column_definitions_path
                )
            )
            self.tables = TableConfigs(
                table_configs=Config.get_nested_key(cfg_dict, table_configs_path),
                column_definitions=column_defintions,
            )

        if datasets_path:
            config_dir = (
                None if config_filename is None else os.path.dirname(config_filename)
            )
            self.datasets = DatasetsConfig(
                datasets=Config.get_nested_key(cfg_dict, datasets_path),
                base_dir=config_dir,
            )

    @staticmethod
    def get_nested_key(my_dict: Mapping[str, Any], keys: Iterable[str]):
        try:
            return reduce(lambda d, key: d[key], keys, my_dict)
        except (KeyError, TypeError):
            return None

    @classmethod
    def yaml_to_dict(cls, filename: str) -> Any:
        def _expand_env_vars(value: Any) -> Any:
            if isinstance(value, str):
                return os.path.expandvars(value)
            elif isinstance(value, dict):
                return {k: _expand_env_vars(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [_expand_env_vars(v) for v in value]
            else:
                return value

        print(f"loading Config from: {filename}")
        with open(filename, "r") as file:
            yaml_dict = yaml.safe_load(file)
            yaml_dict = _expand_env_vars(yaml_dict)

        return yaml_dict

    @classmethod
    def from_yaml(
        cls,
        filename: str,
        column_definitions_path: str = "column_definitions",
        table_configs_path: str = "table_configs",
        datasets_path: str = "datasets",
        db_config_path: str = "db",
    ) -> "Config":
        yaml_dict: Mapping[str, Any] = cls.yaml_to_dict(filename)
        config = Config(
            yaml_dict,
            db_config_path=db_config_path.split("."),
            column_definitions_path=column_definitions_path.split("."),
            table_configs_path=table_configs_path.split("."),
            datasets_path=datasets_path.split("."),
            config_filename=filename,
        )

        return config
