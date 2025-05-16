from abc import ABC, abstractmethod
from typing import Any, List
import polars as pl

class DataFrameTransform(ABC):
    @classmethod
    @abstractmethod
    def name(cls) -> str:
        pass
    
    @abstractmethod
    def __init__(self, args: List[Any]) -> None:
        pass
    
    @abstractmethod
    def dependencies(self) -> List[str]:
        pass

    @abstractmethod
    def apply(self, df: pl.DataFrame) -> pl.DataFrame:
        pass
