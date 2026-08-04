from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from ..backends.config import DbEngineConfig
from .dataset import DatasetsConfig
from .helpers import get_nested_key, load_yaml
from .table import TableConfigs


@dataclass(frozen=True)
class IngestionConfig:
    max_workers: int = 1
    queue_size: int = 1

    def __post_init__(self):
        if self.max_workers < 1:
            raise ValueError("ingestion.max_workers must be at least 1")
        if self.queue_size < 1:
            raise ValueError("ingestion.queue_size must be at least 1")


@dataclass(frozen=True)
class ParitySemanticForeignKeyConfig:
    source: str
    target: str
    columns: tuple[str, ...]

    def __init__(self, source: str, target: str, columns: Iterable[str]):
        object.__setattr__(self, "source", source)
        object.__setattr__(self, "target", target)
        object.__setattr__(self, "columns", tuple(columns))


@dataclass(frozen=True)
class ParityConfig:
    ignore_columns: tuple[str, ...] = field(default_factory=tuple)
    semantic_foreign_keys: tuple[ParitySemanticForeignKeyConfig, ...] = field(
        default_factory=tuple
    )

    def __init__(
        self,
        ignore_columns: Iterable[str] = (),
        semantic_foreign_keys: Iterable[
            ParitySemanticForeignKeyConfig | Mapping[str, Any]
        ] = (),
    ):
        object.__setattr__(self, "ignore_columns", tuple(ignore_columns))
        object.__setattr__(
            self,
            "semantic_foreign_keys",
            tuple(
                item
                if isinstance(item, ParitySemanticForeignKeyConfig)
                else ParitySemanticForeignKeyConfig(
                    source=str(item["source"]),
                    target=str(item["target"]),
                    columns=item["columns"],
                )
                for item in semantic_foreign_keys
            ),
        )


class PolarsHistDbConfig:
    def __init__(
        self,
        cfg_dict: Mapping[str, Any],
        table_configs_path: Iterable[str],
        datasets_path: Iterable[str],
        config_file_path: str | None = None,
    ):
        self.config_file_path = config_file_path
        dataset_params = get_nested_key(cfg_dict, datasets_path)
        table_params = get_nested_key(cfg_dict, table_configs_path)
        db_params = get_nested_key(cfg_dict, ["db"]) or {}
        parity_params = get_nested_key(cfg_dict, ["parity"]) or {}
        ingestion_params = get_nested_key(cfg_dict, ["ingestion"]) or {}

        self.db_config = DbEngineConfig.from_mapping(db_params)
        self.parity = ParityConfig(**parity_params)
        self.ingestion = IngestionConfig(**ingestion_params)
        self.tables = TableConfigs(items=table_params)

        if dataset_params:
            self.datasets = DatasetsConfig(
                datasets=dataset_params, config_file_path=config_file_path
            )

    def create_engine(self):
        from ..backends import backend_from_config

        backend = backend_from_config(self.db_config)
        return backend.create_engine(self.db_config)

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
