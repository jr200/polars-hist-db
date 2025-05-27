from ..config.input_source import DSVInputConfig, InputConfig, JetStreamInputConfig
from .input_source import InputSource
from .dsv_input_source import DsvInputSource
from .jetstream_input_source import JetStreamInputSource


class InputSourceFactory:
    """Factory for creating input sources."""

    @staticmethod
    def create_input_source(config: InputConfig) -> InputSource:
        if config.type == "dsv":
            assert isinstance(config, DSVInputConfig)
            return DsvInputSource(config)
        elif config.type == "jetstream":
            assert isinstance(config, JetStreamInputConfig)
            return JetStreamInputSource(config)
        else:
            raise ValueError(f"Unsupported input type: {config.type}")
