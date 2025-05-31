from abc import ABC, abstractmethod
from typing import AsyncGenerator, Awaitable, Callable, Dict, Tuple
from datetime import datetime

import polars as pl
from sqlalchemy import Connection, Engine

from ..config.dataset import DatasetConfig
from ..config.table import TableConfigs


class InputSource(ABC):
    def __init__(self, tables: TableConfigs, dataset: DatasetConfig):
        self.tables: TableConfigs = tables
        self.dataset: DatasetConfig = dataset

    @abstractmethod
    async def next_df(
        self,
        engine: Engine,
    ) -> AsyncGenerator[
        Tuple[
            Dict[Tuple[datetime], pl.DataFrame], Callable[[Connection], Awaitable[bool]]
        ],
        None,
    ]:
        """Async generator that yields the next dataframe to process"""
        raise NotImplementedError("InputSource is an abstract class")

    @abstractmethod
    async def cleanup(self) -> None:
        """Clean up any resources used by the input source"""
        raise NotImplementedError("InputSource is an abstract class")
