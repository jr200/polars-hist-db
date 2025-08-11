from abc import ABC
from dataclasses import dataclass
from typing import Any, Dict, Optional
import logging

from .types import InputDataSourceType

LOGGER = logging.getLogger(__name__)


@dataclass
class InputConfig(ABC):
    type: InputDataSourceType
    config_file_path: str
    filter_past_events: Optional[bool]

    @staticmethod
    def from_dict(config: Dict[str, Any]) -> "InputConfig":
        input_type = config["type"]
        config.setdefault("filter_past_events", False)

        if input_type == "dsv":
            from .dsv_crawler import DsvCrawlerInputConfig

            return DsvCrawlerInputConfig(**config)
        elif input_type == "nats-jetstream":
            from .jetstream_config import JetStreamInputConfig

            return JetStreamInputConfig(**config)
        else:
            raise ValueError(f"Unsupported input type: {input_type}")
