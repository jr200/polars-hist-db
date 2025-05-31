from abc import ABC
from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional, Union
import os
import logging

import polars as pl

LOGGER = logging.getLogger(__name__)


@dataclass
class InputConfig(ABC):
    type: Literal["dsv", "dsv-text", "jetstream"]

    @staticmethod
    def from_dict(config: Dict[str, Any]) -> "InputConfig":
        input_type = config["type"]
        if input_type == "dsv":
            return DsvCrawlerInputConfig(**config)
        elif input_type == "dsv-text":
            return DsvTextInputConfig(**config)
        elif input_type == "jetstream":
            return JetStreamInputConfig(**config)
        else:
            raise ValueError(f"Unsupported input type: {input_type}")


@dataclass
class DsvTextInputConfig(InputConfig): ...


@dataclass
class DsvCrawlerInputConfig(InputConfig):
    search_paths: Union[pl.DataFrame, List[Dict[str, Any]]]

    def __post_init__(self):
        if not isinstance(self.search_paths, pl.DataFrame):
            for search_path in self.search_paths:
                if "root_path" in search_path:
                    path = search_path["root_path"]
                    if not os.path.isabs(path):
                        abs_path = os.path.normpath(os.path.abspath(path))
                        search_path["root_path"] = abs_path

            self.search_paths = pl.from_records(self.search_paths)


@dataclass
class NatsConfig:
    host: str
    port: int
    options: Optional[Dict[str, Any]]


@dataclass
class JetStreamArgs:
    durable_consumer_name: str
    name: str
    subjects: List[str]


@dataclass
class JetStreamInputConfig(InputConfig):
    nats: NatsConfig
    js_args: JetStreamArgs

    def __post_init__(self):
        if isinstance(self.nats, dict):
            self.nats = NatsConfig(**self.nats)
            self.js_args = JetStreamArgs(**self.js_args)
