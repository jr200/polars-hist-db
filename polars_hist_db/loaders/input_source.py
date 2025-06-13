from abc import ABC, abstractmethod
from typing import AsyncGenerator, Awaitable, Callable, List, Tuple, TypeVar, Generic
from datetime import datetime
import logging

import polars as pl
from sqlalchemy import Connection, Engine

from ..config.dataset import DatasetConfig
from ..config.table import TableConfig, TableConfigs
from ..config.input_source import InputConfig

LOGGER = logging.getLogger(__name__)

TConfig = TypeVar("TConfig", bound=InputConfig)


class InputSource(ABC, Generic[TConfig]):
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
        self.previous_payload_time: datetime = datetime.min

    @abstractmethod
    async def next_df(
        self,
        engine: Engine,
    ) -> AsyncGenerator[
        Tuple[
            List[Tuple[datetime, pl.DataFrame]], Callable[[Connection], Awaitable[bool]]
        ],
        None,
    ]:
        """Async generator that yields the next dataframe to process"""
        raise NotImplementedError("InputSource is an abstract class")

    @abstractmethod
    async def cleanup(self) -> None:
        """Clean up any resources used by the input source"""
        raise NotImplementedError("InputSource is an abstract class")

    def _filter_past_events(self, df: pl.DataFrame, time_col: str) -> pl.DataFrame:
        previous_row_count = len(df)
        df = df.filter(pl.col("__interval") > self.previous_payload_time)
        stale_row_count = previous_row_count - len(df)
        if stale_row_count > 0:
            LOGGER.warn(
                f"Removed {stale_row_count} stale rows <= {self.previous_payload_time.isoformat()}"
            )

        if len(df) == 0:
            LOGGER.warn("Empty dataframe after time partitioning")
        else:
            self.previous_payload_time = (
                df.select(pl.col("__interval").max()).to_series().item()
            )
            df = df.filter(pl.col(time_col) > self.previous_payload_time)

        return df

    def _apply_time_partitioning(
        self, df: pl.DataFrame, payload_time: datetime
    ) -> List[Tuple[datetime, pl.DataFrame]]:
        pipeline = self.dataset.pipeline
        main_table_config: TableConfig = self.tables[pipeline.get_main_table_name()]
        tbl_to_header_map = pipeline.get_header_map(main_table_config.name)
        header_keys = [
            tbl_to_header_map.get(k, k) for k in main_table_config.primary_keys
        ]

        if self.dataset.time_partition:
            tp = self.dataset.time_partition
            time_col = tp.column
            interval = tp.truncate
            unique_strategy = tp.unique_strategy

            prepared_df = (
                df.with_columns(
                    __interval=pl.col(time_col).dt.truncate(interval).cast(pl.Datetime)
                )
                .sort(time_col)
                .unique(
                    [*header_keys, "__interval"],
                    keep=unique_strategy,
                    maintain_order=True,
                )
            )

            if self.config.filter_past_events:
                prepared_df = self._filter_past_events(prepared_df, time_col)

            partitions = prepared_df.partition_by(
                "__interval", include_key=False, as_dict=True, maintain_order=True
            )

            result = [(k[0], v) for k, v in partitions.items()]

        else:
            result = [(payload_time, df)]
            self.previous_payload_time = payload_time

        return result  # type: ignore[return-value]
