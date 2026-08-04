from dataclasses import dataclass
from typing import Any, Literal

from .input_source import InputConfig


@dataclass
class JetStreamSubscriptionConfig:
    subject: str
    stream: str
    durable: str | None
    options: dict[str, Any]
    consumer_args: dict[str, Any]

    def __post_init__(self):
        if self.options is None:
            self.options = {}

        if self.consumer_args is None:
            self.consumer_args = {}


@dataclass
class JetStreamFetchConfig:
    # number of messages to fetch in a single call
    batch_size: int = 1000

    # timeout for a single fetch call in seconds
    batch_timeout: float = 5.0

    # interval between in-progress heartbeats; 0 disables them
    heartbeat_interval: float = 30.0

    def __post_init__(self):
        if self.heartbeat_interval < 0:
            raise ValueError("heartbeat_interval cannot be negative")


@dataclass
class JetStreamConfig:
    subscription: JetStreamSubscriptionConfig
    fetch: JetStreamFetchConfig

    def __post_init__(self):
        if isinstance(self.subscription, dict):
            self.subscription = JetStreamSubscriptionConfig(**self.subscription)

        if isinstance(self.fetch, dict):
            self.fetch = JetStreamFetchConfig(**self.fetch)


@dataclass
class JetstreamIngestConfig:
    fn_name: str
    fn_args: dict[str, Any] | None = None


@dataclass
class JetStreamInputConfig(InputConfig):
    jetstream: JetStreamConfig
    payload_ingest: JetstreamIngestConfig
    run_until: Literal["empty", "forever"]

    def __post_init__(self):
        if isinstance(self.jetstream, dict):
            self.jetstream = JetStreamConfig(**self.jetstream)

        if isinstance(self.payload_ingest, dict):
            self.payload_ingest = JetstreamIngestConfig(**self.payload_ingest)
