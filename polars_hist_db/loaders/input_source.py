from abc import ABC, abstractmethod
from typing import AsyncGenerator, Awaitable, Callable, Dict, Tuple
from datetime import datetime

import polars as pl
from sqlalchemy import Connection, Engine

from ..config.dataset import DatasetConfig
from ..config.table import TableConfigs


class InputSource(ABC):
    @abstractmethod
    async def next_df(
        self,
        dataset: DatasetConfig,
        tables: TableConfigs,
        engine: Engine,
        scrape_limit: int = -1,
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
