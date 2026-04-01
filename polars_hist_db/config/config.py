from typing import Any, Iterable, Mapping, Optional

from .helpers import get_nested_key, load_yaml

from .dataset import DatasetsConfig
from .table import TableConfigs


class PolarsHistDbConfig:
    def __init__(
        self,
        cfg_dict: Mapping[str, Any],
        table_configs_path: Iterable[str],
        datasets_path: Iterable[str],
        config_file_path: Optional[str] = None,
    ):
        self.config_file_path = config_file_path
        dataset_params = get_nested_key(cfg_dict, datasets_path)
        table_params = get_nested_key(cfg_dict, table_configs_path)

        self.tables = TableConfigs(items=table_params)

        if dataset_params:
            self.datasets = DatasetsConfig(
                datasets=dataset_params, config_file_path=config_file_path
            )

    @classmethod
    def from_yaml(
        cls,
        filename: str,
        table_configs_path: str = "table_configs",
        datasets_path: str = "datasets",
    ) -> "PolarsHistDbConfig":
        yaml_dict: Mapping[str, Any] = load_yaml(filename)
        config = PolarsHistDbConfig(
            yaml_dict,
            table_configs_path=table_configs_path.split("."),
            datasets_path=datasets_path.split("."),
            config_file_path=filename,
        )

        return config
