from abc import ABC
import csv
from dataclasses import dataclass
from io import StringIO
from typing import Any, Dict, List, Literal, Optional, Union
import os
import logging
from datetime import datetime

import polars as pl

LOGGER = logging.getLogger(__name__)


@dataclass
class InputConfig(ABC):
    type: Literal["dsv", "jetstream"]

    @staticmethod
    def from_dict(config: Dict[str, Any]) -> "InputConfig":
        input_type = config["type"]
        if input_type == "dsv":
            return DsvCrawlerInputConfig(**config)
        elif input_type == "jetstream":
            return JetStreamInputConfig(**config)
        else:
            raise ValueError(f"Unsupported input type: {input_type}")


@dataclass
class DsvCrawlerInputConfig(InputConfig):
    type: Literal["dsv", "jetstream"] = "dsv"
    search_paths: Optional[Union[pl.DataFrame, List[Dict[str, Any]]]] = None
    payload: Optional[str] = None
    payload_time: Optional[datetime] = None
    config_file_path: Optional[str] = None

    @staticmethod
    def clean_dsv_string(data: str) -> str:
        input_io = StringIO(data.strip())
        output_io = StringIO()

        reader = csv.reader(input_io)
        writer = csv.writer(output_io, quoting=csv.QUOTE_MINIMAL)

        for row in reader:
            cleaned_row = [field.strip() if field is not None else "" for field in row]
            writer.writerow(cleaned_row)

        csv_output = output_io.getvalue().strip() + "\n"
        return csv_output

    def set_payload(self, payload: str, payload_time: datetime):
        self.payload = DsvCrawlerInputConfig.clean_dsv_string(payload)
        self.payload_time = payload_time

    def has_payload(self) -> bool:
        return self.payload is not None and self.payload_time is not None

    def __post_init__(self):
        if self.search_paths and not isinstance(self.search_paths, pl.DataFrame):
            for search_path in self.search_paths:
                if "root_path" in search_path:
                    path = search_path["root_path"]
                    if not os.path.isabs(path):
                        if self.config_file_path is None:
                            LOGGER.warning("No config_file_path provided, using current working directory as base for relative path")
                            base_path = os.getcwd()
                        else:
                            base_path = os.path.dirname(os.path.abspath(self.config_file_path))
                        abs_path = os.path.normpath(os.path.join(base_path, path))
                        search_path["root_path"] = abs_path

            self.search_paths = pl.from_records(self.search_paths)


@dataclass
class NatsConfig:
    host: str
    port: int
    options: Dict[str, Any]

    def __post_init__(self):
        if self.options is None:
            self.options = dict()


@dataclass
class JetStreamSubscriptionConfig:
    subject: str
    stream: str
    durable: Optional[str]
    options: Dict[str, Any]
    consumer_args: Dict[str, Any]

    def __post_init__(self):
        if self.options is None:
            self.options = dict()

        if self.consumer_args is None:
            self.consumer_args = dict()


@dataclass
class JetStreamFetchConfig:
    # number of messages to fetch in a single call
    batch_size: int = 1000

    # timeout for a single fetch call in seconds
    batch_timeout: float = 5.0


@dataclass
class JetStreamConfig:
    subscription: JetStreamSubscriptionConfig
    fetch: JetStreamFetchConfig
    context: Dict[str, Any]

    def __post_init__(self):
        if isinstance(self.subscription, dict):
            self.subscription = JetStreamSubscriptionConfig(**self.subscription)

        if isinstance(self.fetch, dict):
            self.fetch = JetStreamFetchConfig(**self.fetch)

        if self.context is None:
            self.context = dict()


@dataclass
class JetStreamInputConfig(InputConfig):
    nats: NatsConfig
    jetstream: JetStreamConfig

    def __post_init__(self):
        if isinstance(self.nats, dict):
            self.nats = NatsConfig(**self.nats)

        if isinstance(self.jetstream, dict):
            self.jetstream = JetStreamConfig(**self.jetstream)
