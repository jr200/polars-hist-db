from typing import Optional

from nats.js.client import JetStreamContext

from ..config.dataset import DatasetConfig
from ..config.input.input_source import InputConfig
from ..config.input.dsv_crawler import DsvCrawlerInputConfig
from ..config.input.jetstream_config import JetStreamInputConfig

from ..config.table import TableConfigs
from .input_source import InputSource
from .dsv_input_source import DsvCrawlerInputSource
from .jetstream_input_source import JetStreamInputSource


class InputSourceFactory:
    """Factory for creating input sources."""

    @staticmethod
    def create_input_source(
        tables: TableConfigs,
        dataset: DatasetConfig,
        config: InputConfig,
        js: Optional[JetStreamContext] = None,
    ) -> InputSource:
        if config.type == "dsv":
            assert isinstance(config, DsvCrawlerInputConfig)
            return DsvCrawlerInputSource(tables, dataset, config)
        elif config.type == "nats-jetstream":
            assert isinstance(config, JetStreamInputConfig)
            if js is None:
                raise ValueError(
                    "JetStreamContext is required for nats-jetstream input sources"
                )
            return JetStreamInputSource(tables, dataset, config, js)
        else:
            raise ValueError(f"Unsupported input type: {config.type}")
