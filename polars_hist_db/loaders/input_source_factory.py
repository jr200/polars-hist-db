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
        tables: TableConfigs, dataset: DatasetConfig, config: InputConfig
    ) -> InputSource:
        if config.type == "dsv":
            assert isinstance(config, DsvCrawlerInputConfig)
            return DsvCrawlerInputSource(tables, dataset, config)
        elif config.type == "nats-jetstream":
            assert isinstance(config, JetStreamInputConfig)
            return JetStreamInputSource(tables, dataset, config)
        else:
            raise ValueError(f"Unsupported input type: {config.type}")
