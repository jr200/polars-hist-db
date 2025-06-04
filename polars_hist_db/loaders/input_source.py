from abc import ABC, abstractmethod
from typing import AsyncGenerator, Awaitable, Callable, List, Tuple
from datetime import datetime

import polars as pl
from sqlalchemy import Connection, Engine

from ..config.dataset import DatasetConfig
from ..config.table import TableConfig, TableConfigs


class InputSource(ABC):
    def __init__(self, tables: TableConfigs, dataset: DatasetConfig):
        self.tables: TableConfigs = tables
        self.dataset: DatasetConfig = dataset
        self.column_definitions = (
            self.dataset.pipeline.build_ingestion_column_definitions(self.tables)
        )

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

            partitions = (
                df.with_columns(
                    __interval=pl.col(time_col).dt.truncate(interval).cast(pl.Datetime)
                )
                .sort(time_col)
                .unique(
                    [*header_keys, "__interval"],
                    keep=unique_strategy,
                    maintain_order=True,
                )
                .partition_by(
                    "__interval", include_key=False, as_dict=True, maintain_order=True
                )
            )

            result = [(k[0], v) for k, v in partitions.items()]

        else:
            result = [(payload_time, df)]

        return result  # type: ignore[return-value]
