from abc import ABC, abstractmethod
from typing import AsyncGenerator, Callable, Dict, Tuple
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
        Tuple[Dict[Tuple[datetime], pl.DataFrame], Callable[[Connection], bool]], None
    ]:
        """Async generator that yields the next dataframe to process"""
        raise NotImplementedError("InputSource is an abstract class")
