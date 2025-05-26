from ..config.input_source import DSVInputConfig, InputConfig
from .input_source import InputSource
from .dsv_input_source import DsvInputSource


class InputSourceFactory:
    """Factory for creating input sources."""

    @staticmethod
    def create_input_source(config: InputConfig) -> InputSource:
        if config.type == "dsv":
            assert isinstance(config, DSVInputConfig)
            return DsvInputSource(config)
        elif config.type == "stream":
            raise NotImplementedError("Stream input source not implemented yet")
        else:
            raise ValueError(f"Unsupported input type: {config.type}")
