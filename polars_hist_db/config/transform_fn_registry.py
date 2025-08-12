import logging
from typing import Any, Callable, List, Dict
import polars as pl

from .fn_builtins import (
    apply_type_casts,
    combine_columns,
    map_to_true,
    null_if_gte,
    parse_date,
)

LOGGER = logging.getLogger(__name__)

TransformFnSignature = Callable[[pl.DataFrame, str, List[Any]], pl.DataFrame]
TransformRegistryStore = Dict[str, TransformFnSignature]


class TransformFnRegistry:
    _borg: Dict[str, Any] = {"_registry": None}

    def __init__(self) -> None:
        self.__dict__ = self._borg
        self._registry: TransformRegistryStore = self._one_time_init()

    def _one_time_init(self) -> TransformRegistryStore:
        if self._registry is None:
            self._registry = dict()
            self.register_function("null_if_gte", null_if_gte)
            self.register_function("apply_type_casts", apply_type_casts)
            self.register_function("combine_columns", combine_columns)
            self.register_function("map_to_true", map_to_true)
            self.register_function("parse_date", parse_date)

        return self._registry

    def delete_function(self, name: str) -> None:
        if name in self._registry:
            del self._registry[name]

    def register_function(
        self, name: str, fn: TransformFnSignature, allow_overwrite: bool = False
    ) -> None:
        if name in self._registry and not allow_overwrite:
            raise ValueError(
                f"A transform function with the name '{name}' is already registered."
            )

        LOGGER.debug("added transform function %s to registry", name)
        self._registry[name] = fn

    def call_function(
        self,
        name: str,
        df: pl.DataFrame,
        result_col: str,
        args: List[Any],
    ) -> pl.DataFrame:
        if name not in self._registry:
            raise ValueError(
                f"No transform function registered with the name '{name}'."
            )

        LOGGER.debug(
            "applying transform fn %s to dataframe %s => %s", name, df.shape, result_col
        )
        fn = self._registry[name]
        result_df = fn(df, result_col, args)

        if result_df is None:
            raise ValueError(f"transform function {name} returned None")

        return result_df

    def list_functions(self) -> List[str]:
        return list(self._registry.keys())
