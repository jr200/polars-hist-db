import asyncio
import logging
from contextlib import ExitStack
from datetime import datetime
from random import uniform
from typing import Any
from uuid import uuid4

import polars as pl
from sqlalchemy import Connection, Engine

from ..config import DatasetConfig, TableConfig, TableConfigs
from ..core import DataframeOps, TableConfigOps
from ..loaders.input_source import BatchFinalizer
from ..utils import NonRetryableException
from .extract_item import scrape_extract_item
from .primary_item import scrape_primary_item

LOGGER = logging.getLogger(__name__)


async def _to_thread_joined(function, /, *args):
    task = asyncio.create_task(asyncio.to_thread(function, *args))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        await task
        raise


def _scrape_pipeline_item(
    pipeline_id: int,
    dataset: DatasetConfig,
    target_schema: str,
    target_table: str,
    tables: TableConfigs,
    upload_time: datetime,
    connection: Connection,
    partition_df: pl.DataFrame | None = None,
    stage_run_id: str | None = None,
    staging: Any = None,
    backend: Any = None,
) -> bool:
    item_type = dataset.pipeline.item_type(target_table)
    if item_type == "primary":
        return scrape_primary_item(
            pipeline_id,
            dataset,
            tables,
            upload_time,
            connection,
            partition_df=partition_df,
            stage_run_id=stage_run_id,
            staging=staging,
            backend=backend,
        )
    elif item_type == "extract":
        return scrape_extract_item(
            pipeline_id,
            dataset,
            target_table,
            tables,
            upload_time,
            connection,
            partition_df=partition_df,
            stage_run_id=stage_run_id,
            staging=staging,
            backend=backend,
        )
    else:
        raise ValueError(f"unknown item type: {item_type}")


def _ensure_delta_table(
    connection: Connection,
    delta_table_config: TableConfig,
    is_temporary_table: bool,
):
    """Ensure the delta table exists in the given connection.

    For temporary tables this must run in the same connection that will
    use the table, since TEMPORARY TABLEs are session-scoped.
    """
    TableConfigOps(connection).create(
        delta_table_config,
        is_delta_table=True,
        is_temporary_table=is_temporary_table,
    )


def _run_pipeline_as_transaction(
    partitions: list[tuple[datetime, pl.DataFrame]],
    dataset: DatasetConfig,
    tables: TableConfigs,
    engine: Engine,
    finalizer: BatchFinalizer,
    delta_table_config: TableConfig | None = None,
    backend: Any = None,
    ingest_connection: Any = None,
):
    system_times = {timestamp for timestamp, _ in partitions}
    if backend.name == "xtdb" and len(system_times) > 1:
        raise NonRetryableException(
            "XTDB atomic ingestion requires one system-time per batch"
        )
    main_table_config: TableConfig = tables[dataset.pipeline.get_main_table_name()[1]]
    tbl_to_header_map = dataset.pipeline.get_header_map(main_table_config.name)
    header_keys = [tbl_to_header_map.get(k, k) for k in main_table_config.primary_keys]

    with backend.connection_scope(engine) as connection, ExitStack() as stack:
        ingest_transaction = getattr(backend, "ingest_transaction", None)
        if ingest_transaction is not None:
            stack.enter_context(
                ingest_transaction(
                    connection,
                    next(iter(system_times)) if system_times else None,
                )
            )
        staging = backend.staging(
            connection,
            ingest_connection=ingest_connection,
        )
        if delta_table_config is not None and staging is None:
            _ensure_delta_table(
                connection,
                delta_table_config,
                dataset.delta_config.is_temporary_table,
            )
        modified_tables: set[tuple[str, str]] = set()
        for i, (ts, partition_df) in enumerate(partitions):
            stage_run_id = None
            assert isinstance(ts, datetime), f"timestamp is not a datetime [{type(ts)}]"
            LOGGER.info(
                "-- (%d/%d) time_partition[%s] %d rows",
                i + 1,
                len(partitions),
                ts.isoformat(),
                len(partition_df),
            )

            if staging is None:
                DataframeOps(connection).table_insert(
                    partition_df,
                    dataset.delta_table_schema,
                    dataset.name,
                    uniqueness_col_set=header_keys,
                    prefill_nulls_with_default=True,
                    clear_table_first=True,
                )
            else:
                if delta_table_config is None:
                    raise ValueError("XTDB ingest requires delta table config")
                stage_run_id = f"{dataset.name}:{uuid4()}"
                staging.insert_partition(
                    partition_df,
                    delta_table_config,
                    stage_run_id,
                    ts,
                    uniqueness_col_set=header_keys,
                    prefill_nulls_with_default=True,
                )

            try:
                for pipeline_id, (
                    target_schema,
                    target_table,
                ) in dataset.pipeline.get_pipeline_items().items():
                    did_modify = _scrape_pipeline_item(
                        pipeline_id,
                        dataset,
                        target_schema,
                        target_table,
                        tables,
                        ts,
                        connection,
                        partition_df=partition_df,
                        stage_run_id=stage_run_id,
                        staging=staging,
                        backend=backend,
                    )

                    if did_modify:
                        modified_item = (target_schema, target_table)
                        modified_tables.add(modified_item)
            finally:
                if stage_run_id is not None:
                    staging.cleanup_run(stage_run_id)

        if delta_table_config is not None:
            backend.finalize_ingest_run(connection, delta_table_config)

        if not finalizer.write_audit_before_commit(connection, sorted(modified_tables)):
            raise NonRetryableException("Failed to update audit log")


async def try_run_pipeline_as_transaction(
    partitions: list[tuple[datetime, pl.DataFrame]],
    dataset: DatasetConfig,
    tables: TableConfigs,
    engine: Engine,
    finalizer: BatchFinalizer,
    num_retries: int = 3,
    seconds_between_retries: float = 60,
    max_retry_delay: float = 300,
    retry_jitter: float = 0.1,
    delta_table_config: TableConfig | None = None,
    backend: Any = None,
    ingest_connection: Any = None,
):
    if num_retries < 1:
        raise ValueError("num_retries must be at least 1")
    if not 0 <= retry_jitter <= 1:
        raise ValueError("retry_jitter must be between 0 and 1")
    for attempt in range(num_retries):
        try:
            await _to_thread_joined(
                _run_pipeline_as_transaction,
                partitions,
                dataset,
                tables,
                engine,
                finalizer,
                delta_table_config,
                backend,
                ingest_connection,
            )
            break
        except NonRetryableException:
            raise
        except Exception as e:
            LOGGER.error("error in scrape_pipeline_as_transaction", exc_info=e)
            if attempt + 1 == num_retries:
                raise
            delay = min(seconds_between_retries * 2**attempt, max_retry_delay)
            delay *= uniform(1 - retry_jitter, 1 + retry_jitter)
            LOGGER.info("retries remaining: %d", num_retries - attempt - 1)
            await asyncio.sleep(delay)
    await finalizer.ack_after_commit()
