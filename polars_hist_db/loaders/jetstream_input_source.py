import asyncio
from datetime import datetime
import logging
from typing import (
    AsyncGenerator,
    Awaitable,
    Callable,
    List,
    Tuple,
)

from nats.js.api import ConsumerConfig
from nats.js.client import JetStreamContext
from nats.js.errors import NotFoundError

import polars as pl
from sqlalchemy import Connection, Engine

from ..core.audit import AuditOps
from ..utils.exceptions import NonRetryableException

from .ingest_payload import load_df_from_msg

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
        js: JetStreamContext,
    ):
        super().__init__(tables, dataset, config)
        self._js = js

    async def cleanup(self) -> None:
        # Caller owns the NATS client — do not close it here
        pass

    async def next_df(
        self, engine: Engine
    ) -> AsyncGenerator[
        Tuple[
            List[Tuple[datetime, pl.DataFrame]],
            Callable[[Connection, List[Tuple[str, str]]], Awaitable[bool]],
        ],
        None,
    ]:
        async def _generator() -> AsyncGenerator[
            Tuple[
                List[Tuple[datetime, pl.DataFrame]],
                Callable[[Connection, List[Tuple[str, str]]], Awaitable[bool]],
            ],
            None,
        ]:
            js = self._js
            remaining_msgs = self.dataset.scrape_limit

            js_sub_cfg = self.config.jetstream.subscription

            LOGGER.info(
                "Consumer[%s] subscribing to %s on %s",
                js_sub_cfg.durable,
                js_sub_cfg.subject,
                js_sub_cfg.stream,
            )

            retry_delay = 5
            max_delay = 60
            while True:
                try:
                    sub = await js.pull_subscribe(
                        subject=js_sub_cfg.subject,
                        durable=js_sub_cfg.durable,
                        stream=js_sub_cfg.stream,
                        config=ConsumerConfig(
                            **js_sub_cfg.consumer_args,
                        ),
                        **js_sub_cfg.options,
                    )
                    break
                except NotFoundError:
                    LOGGER.info(
                        "Stream %s not found, retrying in %ds...",
                        js_sub_cfg.stream,
                        retry_delay,
                    )
                    await asyncio.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, max_delay)

            total_msgs = 0

            run_until = self.config.run_until
            pipeline = self.dataset.pipeline
            table_schema, table_name = pipeline.get_main_table_name()

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
                        "got [%d/%d] %s@t=%s...",
                        len(df),
                        num_items_received,
                        js_sub_cfg.subject,
                        received_items_ts,
                    )

                    partitions = self._apply_time_partitioning(df, msg_ts)

                    async def commit_fn(
                        audit_entries: List[str],
                        connection: Connection,
                        modified_tables: List[Tuple[str, str]],
                    ) -> bool:
                        for msg, (audit_log_id, created_at) in zip(msgs, msg_audits):
                            result = True
                            if audit_log_id in audit_entries:
                                for modified_schema, modified_table in modified_tables:
                                    aops = AuditOps(modified_schema)
                                    result = result and aops.add_entry(
                                        "nats-jetstream",
                                        audit_log_id,
                                        modified_table,
                                        connection,
                                        created_at,
                                    )

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
                        lambda connection, modified_tables: commit_fn(
                            audit_entries, connection, modified_tables
                        ),
                    )

                except TimeoutError:
                    if run_until == "empty":
                        LOGGER.info("No more messages, exiting...")
                        break
                    else:
                        LOGGER.info(
                            "%s: polling %ss...",
                            js_sub_cfg.stream,
                            self.config.jetstream.fetch.batch_timeout,
                        )

            LOGGER.info("Processed %d msgs", total_msgs)

        return _generator()
