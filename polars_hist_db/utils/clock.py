from datetime import timedelta
from typing import Any, Dict
import polars as pl


class Clock:
    _borg: Dict[str, Any] = {"_df": None}

    def __init__(self) -> None:
        self.__dict__ = self._borg
        self._df: pl.DataFrame = self._getdf()

    def _getdf(self) -> pl.DataFrame:
        if self._df is None:
            self._df = pl.DataFrame(schema={"name": pl.Utf8, "time": pl.Float64})

        return self._df

    def add_timing(self, name: str, timing: float) -> None:
        self._df = pl.concat(
            [self._getdf(), pl.from_dict({"name": [name], "time": [timing]})]
        )

    def get_avg(self, name: str, window_size: int = 5) -> float:
        avg_seconds: float = (
            self._getdf()
            .filter(pl.col("name") == name)
            .get_column("time")
            .rolling_mean(window_size=window_size, min_samples=1)
            .tail(1)[0]
        )

        return avg_seconds

    def eta(self, name: str, count_remaining: int, window_size: int = 5) -> timedelta:
        avg_seconds = self.get_avg(name, window_size)
        seconds_remaining = int(avg_seconds * count_remaining)
        return timedelta(seconds=seconds_remaining)
