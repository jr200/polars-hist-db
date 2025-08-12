from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .input_source import InputConfig
from .nats_config import NatsConfig


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
class JetstreamIngestConfig:
    fn_name: str
    fn_args: Optional[List[str]] = None


@dataclass
class JetStreamInputConfig(InputConfig):
    jetstream: JetStreamConfig
    nats: NatsConfig
    payload_ingest: JetstreamIngestConfig

    def __post_init__(self):
        if isinstance(self.nats, dict):
            self.nats = NatsConfig(**self.nats)

        if isinstance(self.jetstream, dict):
            self.jetstream = JetStreamConfig(**self.jetstream)

        if isinstance(self.payload_ingest, dict):
            self.payload_ingest = JetstreamIngestConfig(**self.payload_ingest)
