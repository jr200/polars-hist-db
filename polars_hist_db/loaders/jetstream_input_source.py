from datetime import datetime
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

import polars as pl
from sqlalchemy import Connection, Engine

from ..core.audit import AuditOps
from ..utils.exceptions import NonRetryableException

from .ingest_payload import load_df_from_msg
from .jetstream.nats_client import make_nats_client

from ..config.dataset import DatasetConfig
from ..config.input.jetstream_config import JetStreamInputConfig
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

    async def cleanup(self) -> None:
        if self._nats_client is not None:
            await self._nats_client.close()
            self._nats_client = None

    async def next_df(
        self, engine: Engine
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

            LOGGER.info(
                f"Consumer[{js_sub_cfg.durable}] subscribing to {js_sub_cfg.subject} on {js_sub_cfg.stream}"
            )

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

            run_until = self.config.run_until
            pipeline = self.dataset.pipeline
            table_schema, table_name = pipeline.get_main_table_name()
            aops = AuditOps(table_schema)

            while (run_until == "empty" and remaining_msgs != 0) or (
                run_until == "forever"
            ):
                try:
                    msgs = await sub.fetch(
                        self.config.jetstream.fetch.batch_size,
                        self.config.jetstream.fetch.batch_timeout,
                    )

                    if len(msgs) == 0:
                        continue

                    total_msgs += len(msgs)
                    msg_ts: datetime = msgs[-1].metadata.timestamp

                    all_dfs = []
                    msg_audits = []
                    for msg in msgs:
                        df = load_df_from_msg(msg, msg_ts, self.config.payload_ingest)
                        msg_audits.extend(
                            list(
                                df.select("__path", "__created_at").unique().iter_rows()
                            )
                        )
                        all_dfs.append(df)

                    df = pl.concat(all_dfs)

                    num_items_received = len(df)
                    received_items_ts = (
                        df.select(
                            pl.concat_list(
                                pl.min("__created_at"), pl.max("__created_at")
                            )
                            .explode()
                            .sort()
                            .unique()
                            .dt.strftime("%Y-%m-%d %H:%M:%S")
                        )
                        .get_column("__created_at")
                        .to_list()
                    )

                    df = self._search_and_filter_files(
                        df, table_schema, table_name, engine
                    )

                    audit_entries = df["__path"].unique().to_list()

                    df = df.drop("__path", "__created_at").pipe(
                        apply_transformations, self.column_definitions
                    )
                    LOGGER.info(
                        f"got [{len(df)}/{num_items_received}] {js_sub_cfg.subject}@t={received_items_ts}..."
                    )

                    partitions = self._apply_time_partitioning(df, msg_ts)

                    async def commit_fn(
                        audit_entries: List[str], connection: Connection
                    ) -> bool:
                        for msg, (audit_log_id, created_at) in zip(msgs, msg_audits):
                            if audit_log_id in audit_entries:
                                result: bool = aops.add_entry(
                                    "nats-jetstream",
                                    audit_log_id,
                                    table_name,
                                    connection,
                                    created_at,
                                )
                            else:
                                result = True

                            if result:
                                await msg.ack()
                            else:
                                await msg.nak()
                                LOGGER.error(
                                    "audit for [%s.%s - %s]: FAILED",
                                    table_schema,
                                    table_name,
                                    audit_log_id,
                                )
                                raise NonRetryableException(
                                    "Failed to update audit log"
                                )

                        return True

                    yield (
                        partitions,
                        lambda connection: commit_fn(audit_entries, connection),
                    )

                except TimeoutError:
                    if run_until == "empty":
                        LOGGER.info("No more messages, exiting...")
                        break
                    else:
                        LOGGER.info(
                            f"{js_sub_cfg.stream}: polling {self.config.jetstream.fetch.batch_timeout}s..."
                        )

            LOGGER.info("Processed %d msgs", total_msgs)

        return _generator()
