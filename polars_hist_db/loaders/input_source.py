import logging
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TypeVar

import polars as pl
from sqlalchemy import Connection, Engine

from ..config.dataset import DatasetConfig
from ..config.input.input_source import InputConfig
from ..config.table import TableConfig, TableConfigs
from ..core.audit import AuditOps

LOGGER = logging.getLogger(__name__)

TConfig = TypeVar("TConfig", bound=InputConfig)


async def _noop_after_commit() -> None:
    pass


@dataclass
class BatchFinalizer:
    write_audit_before_commit: Callable[[Connection, list[tuple[str, str]]], bool] = (
        lambda connection, modified_tables: True
    )
    ack_after_commit: Callable[[], Awaitable[None]] = _noop_after_commit


def _file_filter_connection_context(engine: Engine):
    if getattr(getattr(engine, "dialect", None), "name", None) == "postgresql":
        return engine.connect()
    return engine.begin()


class InputSource[TConfig: InputConfig](ABC):
    def __init__(
        self,
        tables: TableConfigs,
        dataset: DatasetConfig,
        config: TConfig,
    ):
        self.tables: TableConfigs = tables
        self.dataset: DatasetConfig = dataset
        self.config: TConfig = config
        self.column_definitions = (
            self.dataset.pipeline.build_ingestion_column_definitions(self.tables)
        )
        self.previous_payload_time: datetime = datetime.min.replace(tzinfo=UTC)

    @abstractmethod
    async def next_df(
        self,
        engine: Engine,
    ) -> AsyncGenerator[
        tuple[
            list[tuple[datetime, pl.DataFrame]],
            BatchFinalizer,
        ],
        None,
    ]:
        """Async generator that yields the next dataframe to process"""
        raise NotImplementedError("InputSource is an abstract class")

    @abstractmethod
    async def cleanup(self) -> None:
        """Clean up any resources used by the input source"""
        raise NotImplementedError("InputSource is an abstract class")

    def _filter_past_events(
        self, df: pl.DataFrame, time_col: str, bucket_col: str, bucket_offset: str
    ) -> pl.DataFrame:
        previous_row_count = len(df)

        # only keep rows that are after the previous bucket's timestamp
        df = df.filter(
            pl.col(bucket_col)
            > pl.lit(self.previous_payload_time).cast(pl.dtype_of(bucket_col))
        )
        stale_row_count = previous_row_count - len(df)
        if stale_row_count > 0:
            LOGGER.warning(
                "Removed %d/%d stale rows <= %s",
                stale_row_count,
                previous_row_count,
                self.previous_payload_time.isoformat(),
            )

        if len(df) == 0:
            LOGGER.warning("Empty dataframe after time partitioning")
        else:
            self.previous_payload_time = (
                df.select(pl.col(bucket_col).dt.offset_by(f"-{bucket_offset}").max())
                .to_series()
                .item()
            )
            df = df.filter(
                pl.col(time_col)
                > pl.lit(self.previous_payload_time).cast(pl.dtype_of(time_col))
            )

        return df

    def _apply_time_partitioning(
        self, df: pl.DataFrame, payload_time: datetime
    ) -> list[tuple[datetime, pl.DataFrame]]:
        pipeline = self.dataset.pipeline
        main_table_config: TableConfig = self.tables[pipeline.get_main_table_name()[1]]
        tbl_to_header_map = pipeline.get_header_map(main_table_config.name)
        header_keys = [
            tbl_to_header_map.get(k, k) for k in main_table_config.primary_keys
        ]

        if self.dataset.time_partition:
            tp = self.dataset.time_partition
            time_col = tp.column
            interval = tp.bucket_interval
            bucket_strategy = tp.bucket_strategy
            bucket_offset = interval if bucket_strategy == "round_up" else "0s"
            unique_strategy = tp.unique_strategy

            prepared_df = (
                df.with_columns(
                    __bucket=pl.col(time_col)
                    .dt.truncate(interval)
                    .dt.offset_by(bucket_offset)
                    .cast(pl.dtype_of(time_col))
                )
                .sort(time_col)
                .unique(
                    [*header_keys, "__bucket"],
                    keep=unique_strategy,
                    maintain_order=True,
                )
            )

            if self.config.filter_past_events:
                prepared_df = self._filter_past_events(
                    prepared_df, time_col, "__bucket", bucket_offset
                )

            partition_sizes = (
                prepared_df.group_by("__bucket", maintain_order=True).len().rows()
            )
            result = []
            offset = 0
            for bucket, size in partition_sizes:
                result.append(
                    (bucket, prepared_df.slice(offset, size).drop("__bucket"))
                )
                offset += size

        else:
            result = [(payload_time, df)]
            self.previous_payload_time = payload_time

        return result  # type: ignore[return-value]

    def _search_and_filter_files(
        self,
        upload_candidates_df: pl.DataFrame,
        table_schema: str,
        table_name: str,
        engine: Engine,
    ) -> pl.DataFrame:
        assert "__path" in upload_candidates_df.columns
        assert "__created_at" in upload_candidates_df.columns

        aops = AuditOps(table_schema)
        with _file_filter_connection_context(engine) as connection:
            filtered_items_df = aops.filter_items(
                upload_candidates_df, "__path", "__created_at", table_name, connection
            ).sort("__created_at")

            if self.dataset.scrape_limit > 0:
                filtered_items_df = filtered_items_df.head(self.dataset.scrape_limit)

            aops.prevalidate_new_items(table_name, filtered_items_df, connection)

        LOGGER.debug("found %d items to process", len(filtered_items_df))
        LOGGER.debug("filtered_items_df: %s", filtered_items_df)

        return filtered_items_df
