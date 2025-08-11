from datetime import datetime
import json
import logging
from pathlib import Path
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    List,
    Mapping,
    Tuple,
    Optional,
)

import nats
from nats.aio.msg import Msg

import polars as pl
from sqlalchemy import Connection, Engine

from .jetstream.nats_client import make_nats_client

from ..config.dataset import DatasetConfig
from ..config.input.jetstream_config import JetStreamInputConfig, JetStreamPayloadType
from ..config.table import TableConfigs
from .input_source import InputSource
from .transform import apply_transformations

LOGGER = logging.getLogger(__name__)


class JetStreamInputSource(InputSource[JetStreamInputConfig]):
    def __init__(
        self,
        tables: TableConfigs,
        dataset: DatasetConfig,
        config: JetStreamInputConfig,
    ):
        super().__init__(tables, dataset, config)
        self._nats_client: Optional[nats.NATS] = None

    async def _get_nats_client(self) -> nats.NATS:
        if self._nats_client is None:
            nats_servers = self.config.nats.servers
            options = self.config.nats.options or dict()
            self._nats_client = await make_nats_client(nats_servers, options)
        return self._nats_client

    @staticmethod
    def _validate_auth(auth_config: Mapping[str, Any]):
        creds_file = auth_config.get("user_credentials", None)
        if not creds_file:
            LOGGER.info("No auth provided")
        else:
            creds_path = Path(creds_file).resolve(strict=True)
            LOGGER.info(f"Validating auth from {creds_path}")
            if not creds_path.exists():
                raise FileNotFoundError(f"Missing creds file: {creds_file}")

            file_size = creds_path.stat().st_size
            if file_size < 512:
                raise ValueError(f"Invalid credentials file: {creds_file}. Check file.")

    @staticmethod
    def _load_df_from_msg(msg: Msg, default_payload_type: Optional[JetStreamPayloadType]) -> pl.DataFrame:
        data = json.loads(msg.data.decode())
        payload_type = data.get("payload_type", default_payload_type)
        if payload_type == "s3":
            return pl.from_dict(data)
        elif payload_type == "json":
            return pl.from_dict(data)
        else:
            raise ValueError("Unable to determine payload_type, and no default set.")
        return pl.from_dict(data)

    async def cleanup(self) -> None:
        if self._nats_client is not None:
            await self._nats_client.close()
            self._nats_client = None

    async def next_df(
        self, _engine: Engine
    ) -> AsyncGenerator[
        Tuple[
            List[Tuple[datetime, pl.DataFrame]], Callable[[Connection], Awaitable[bool]]
        ],
        None,
    ]:
        async def _generator() -> AsyncGenerator[
            Tuple[
                List[Tuple[datetime, pl.DataFrame]],
                Callable[[Connection], Awaitable[bool]],
            ],
            None,
        ]:
            nc = await self._get_nats_client()
            js = nc.jetstream(**self.config.jetstream.context)
            remaining_msgs = self.dataset.scrape_limit

            js_sub_cfg = self.config.jetstream.subscription

            sub = await js.pull_subscribe(
                subject=js_sub_cfg.subject,
                durable=js_sub_cfg.durable,
                stream=js_sub_cfg.stream,
                config=nats.js.api.ConsumerConfig(
                    **js_sub_cfg.consumer_args,
                ),
                **js_sub_cfg.options,
            )

            total_msgs = 0
            while remaining_msgs != 0:
                try:
                    msgs = await sub.fetch(
                        self.config.jetstream.fetch.batch_size,
                        self.config.jetstream.fetch.batch_timeout,
                    )

                    if len(msgs) == 0:
                        continue

                    total_msgs += len(msgs)

                    all_dfs = [self._load_df_from_msg(msg, self.config.default_payload_type) for msg in msgs]
                    df = pl.concat(all_dfs)

                    ts: datetime = msgs[-1].metadata.timestamp

                    df = apply_transformations(df, self.column_definitions)
                    partitions = self._apply_time_partitioning(df, ts)

                    async def commit_fn(_: Connection) -> bool:
                        for msg in msgs:
                            await msg.ack()
                        return True

                    yield partitions, commit_fn

                except TimeoutError:
                    break

            LOGGER.info("Processed %d msgs", total_msgs)

        return _generator()
