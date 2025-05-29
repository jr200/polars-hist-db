from datetime import datetime
import json
import logging
from pathlib import Path
from typing import Any, AsyncGenerator, Awaitable, Callable, Dict, Mapping, Tuple

import nats
from nats.aio.msg import Msg

import polars as pl
from sqlalchemy import Connection, Engine

from ..config.dataset import DatasetConfig
from ..config.input_source import JetStreamInputConfig
from ..config.table import TableConfigs
from .input_source import InputSource

LOGGER = logging.getLogger(__name__)


class JetStreamInputSource(InputSource):
    def __init__(self, config: JetStreamInputConfig):
        self.config = config

    async def _get_nats_client(self) -> nats.NATS:
        nats_url = f"nats://{self.config.nats.host}:{self.config.nats.port}"
        options = self.config.nats.options or {}
        nats_client = await nats.connect(nats_url, **options)
        return nats_client

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
    def _load_df_from_msg(msg: Msg) -> pl.DataFrame:
        data = json.loads(msg.data.decode())
        return pl.from_dict(data)

    async def next_df(
        self,
        dataset: DatasetConfig,
        _tables: TableConfigs,
        _engine: Engine,
        scrape_limit: int = -1,
        load_df_from_msg: Callable[[Msg], pl.DataFrame] = _load_df_from_msg,
    ) -> AsyncGenerator[
        Tuple[
            Dict[Tuple[datetime], pl.DataFrame], Callable[[Connection], Awaitable[bool]]
        ],
        None,
    ]:
        async def _generator() -> AsyncGenerator[
            Tuple[
                Dict[Tuple[datetime], pl.DataFrame],
                Callable[[Connection], Awaitable[bool]],
            ],
            None,
        ]:
            nc = None
            try:
                nc = await self._get_nats_client()
                js = nc.jetstream()
                remaining_msgs = scrape_limit

                sub = await js.pull_subscribe(
                    self.config.js_args.subjects[0],
                    stream=self.config.js_args.name,
                    durable=self.config.js_args.durable_consumer_name,
                )

                while remaining_msgs != 0:
                    remaining_msgs -= 1
                    try:
                        msgs = await sub.fetch()

                        for msg in msgs:
                            partitions = load_df_from_msg(msg)
                            ts: datetime = msg.metadata.timestamp

                            async def commit_fn(_: Connection) -> bool:
                                await msg.ack()
                                return True

                            yield {(ts,): partitions}, commit_fn

                    except TimeoutError:
                        break
            finally:
                if nc is not None:
                    await nc.close()

        return _generator()
