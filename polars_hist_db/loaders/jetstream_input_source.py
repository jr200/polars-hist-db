from datetime import datetime
import json
import logging
from pathlib import Path
from typing import Any, AsyncGenerator, Callable, Dict, Mapping, Tuple

import nats
from nats.aio.msg import Msg

import polars as pl
from sqlalchemy import Connection, Engine

from polars_hist_db.core.audit import AuditOps

from ..config.dataset import DatasetConfig
from ..config.input_source import JetStreamInputConfig
from ..config.table import TableConfigs
from .input_source import InputSource

LOGGER = logging.getLogger(__name__)


class JetStreamInputSource(InputSource):
    def __init__(self, config: JetStreamInputConfig):
        self.config = config

    async def get_nats_client(self) -> nats.NATS:
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

    async def next_df(
        self,
        dataset: DatasetConfig,
        tables: TableConfigs,
        engine: Engine,
        scrape_limit: int = -1,
    ) -> AsyncGenerator[
        Tuple[Dict[Tuple[datetime], pl.DataFrame], Callable[[Connection], bool]], None
    ]:
        async def _generator() -> AsyncGenerator[
            Tuple[Dict[Tuple[datetime], pl.DataFrame], Callable[[Connection], bool]],
            None,
        ]:
            table_name = dataset.pipeline.get_main_table_name()
            table_config = tables[table_name]
            table_schema = table_config.schema

            aops = AuditOps(table_schema)

            with engine.begin() as connection:
                latest_entry = aops.get_latest_entry(
                    table_name, "jetstream", connection
                )
                if latest_entry.is_empty():
                    latest_ts: datetime = datetime.fromtimestamp(0)
                else:
                    latest_ts = latest_entry.item(0, "data_source_ts")

            nc = await self.get_nats_client()
            js = nc.jetstream()

            osub = await js.subscribe(
                self.config.js_args.name,
                durable=self.config.js_args.durable_consumer_name,
                ordered_consumer=True,
            )

            while scrape_limit != 0:
                try:
                    msg: Msg = await osub.next_msg()
                    data = json.loads(msg.data.decode())
                    partitions = pl.read_json(data)
                    ts: datetime = msg.metadata.timestamp

                    if ts <= latest_ts:
                        continue

                    def commit_fn(connection: Connection) -> bool:
                        result: bool = aops.add_entry(
                            "jetstream",
                            self.config.js_args.name,
                            table_name,
                            connection,
                            ts,
                        )
                        return result

                    yield {(ts,): partitions}, commit_fn

                except TimeoutError:
                    break

        return _generator()
