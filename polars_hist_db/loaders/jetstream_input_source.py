import asyncio
import logging
from collections.abc import AsyncGenerator
from datetime import datetime
from typing import Any

import polars as pl
from nats.js.api import ConsumerConfig
from nats.js.client import JetStreamContext
from nats.js.errors import NotFoundError
from sqlalchemy import Connection, Engine

from ..config.dataset import DatasetConfig
from ..config.input.jetstream_config import JetStreamInputConfig
from ..config.table import TableConfigs
from ..core.audit import AuditOps
from ..observability import record_uploader_batch
from ..utils.exceptions import NonRetryableException
from .ingest_payload import load_df_from_msg
from .input_source import BatchFinalizer, InputSource
from .transform import apply_transformations, enforce_input_schema

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

    def _prepare_batch(
        self,
        msgs: list[Any],
        msg_ts: datetime,
        engine: Engine,
        table_schema: str,
        table_name: str,
        subject: str,
    ) -> tuple[list[tuple[datetime, pl.DataFrame]], BatchFinalizer]:
        all_dfs = []
        msg_audits = []
        for msg in msgs:
            raw_df = load_df_from_msg(msg, msg_ts, self.config.payload_ingest)
            msg_audits.extend(
                list(raw_df.select("__path", "__created_at").unique().iter_rows())
            )
            metadata = raw_df.select("__path", "__created_at")
            df = raw_df.drop("__path", "__created_at").pipe(
                enforce_input_schema, self.column_definitions
            )
            df = df.with_columns(
                metadata.get_column("__path"),
                metadata.get_column("__created_at"),
            )
            all_dfs.append(df)

        df = pl.concat(all_dfs)
        num_items_received = len(df)
        received_items_ts = (
            df.select(
                pl.concat_list(pl.min("__created_at"), pl.max("__created_at"))
                .explode(empty_as_null=True)
                .sort()
                .unique()
                .dt.strftime("%Y-%m-%d %H:%M:%S")
            )
            .get_column("__created_at")
            .to_list()
        )
        df = self._search_and_filter_files(df, table_schema, table_name, engine)
        audit_entries = df["__path"].unique().to_list()
        df = df.drop("__path", "__created_at").pipe(
            apply_transformations, self.column_definitions
        )
        LOGGER.info(
            "got [%d/%d] %s@t=%s...",
            len(df),
            num_items_received,
            subject,
            received_items_ts,
        )
        record_uploader_batch(
            table=f"{table_schema}.{table_name}",
            subject=subject,
            received=num_items_received,
            written=len(df),
        )
        partitions = self._apply_time_partitioning(df, msg_ts)

        def write_audit_before_commit(
            connection: Connection,
            modified_tables: list[tuple[str, str]],
        ) -> bool:
            for audit_log_id, created_at in msg_audits:
                result = True
                if audit_log_id in audit_entries:
                    for modified_schema, modified_table in modified_tables:
                        result = result and AuditOps(modified_schema).add_entry(
                            "nats-jetstream",
                            audit_log_id,
                            modified_table,
                            connection,
                            created_at,
                        )
                if not result:
                    LOGGER.error(
                        "audit for [%s.%s - %s]: FAILED",
                        table_schema,
                        table_name,
                        audit_log_id,
                    )
                    raise NonRetryableException("Failed to update audit log")
            return True

        async def ack_after_commit() -> None:
            for msg in msgs:
                await msg.ack()

        return partitions, BatchFinalizer(write_audit_before_commit, ack_after_commit)

    async def _heartbeat(self, msgs: list[Any]) -> None:
        while True:
            await asyncio.sleep(self.config.jetstream.fetch.heartbeat_interval)
            results = await asyncio.gather(
                *(msg.in_progress() for msg in msgs),
                return_exceptions=True,
            )
            for result in results:
                if isinstance(result, Exception):
                    LOGGER.warning("JetStream heartbeat failed: %s", result)

    async def next_df(
        self, engine: Engine
    ) -> AsyncGenerator[
        tuple[
            list[tuple[datetime, pl.DataFrame]],
            BatchFinalizer,
        ],
        None,
    ]:
        async def _generator() -> AsyncGenerator[
            tuple[
                list[tuple[datetime, pl.DataFrame]],
                BatchFinalizer,
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
                    fetch_size = self.config.jetstream.fetch.batch_size
                    if remaining_msgs > 0:
                        fetch_size = min(fetch_size, remaining_msgs)
                    msgs = await sub.fetch(
                        fetch_size,
                        self.config.jetstream.fetch.batch_timeout,
                    )

                    if len(msgs) == 0:
                        continue

                    total_msgs += len(msgs)
                    if remaining_msgs > 0:
                        remaining_msgs -= len(msgs)
                    msg_ts: datetime = msgs[-1].metadata.timestamp

                    heartbeat_task = (
                        asyncio.create_task(self._heartbeat(msgs))
                        if self.config.jetstream.fetch.heartbeat_interval > 0
                        else None
                    )
                    try:
                        yield await asyncio.to_thread(
                            self._prepare_batch,
                            msgs,
                            msg_ts,
                            engine,
                            table_schema,
                            table_name,
                            js_sub_cfg.subject,
                        )
                    finally:
                        if heartbeat_task is not None:
                            heartbeat_task.cancel()
                            await asyncio.gather(heartbeat_task, return_exceptions=True)

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
