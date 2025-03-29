import logging
from typing import Any, Callable, List, Dict
import polars as pl

from .fn_builtins import apply_type_casts, combine_columns, null_if_gte

LOGGER = logging.getLogger(__name__)

FnSignature = Callable[[pl.DataFrame, List[Any]], pl.DataFrame]
RegistryStore = Dict[str, FnSignature]


class FunctionRegistry:
    _borg: Dict[str, Any] = {"_registry": None}

    def __init__(self) -> None:
        self.__dict__ = self._borg
        self._registry: RegistryStore = self._one_time_init()

    def _one_time_init(self) -> RegistryStore:
        if self._registry is None:
            self._registry = dict()
            self.register_function("null_if_gte", null_if_gte)
            self.register_function("apply_type_casts", apply_type_casts)
            self.register_function("combine_columns", combine_columns)

        return self._registry

    def delete_function(self, name: str) -> None:
        if name in self._registry:
            del self._registry[name]

    def register_function(self, name: str, fn: FnSignature) -> None:
        if name in self._registry:
            raise ValueError(
                f"A function with the name '{name}' is already registered."
            )
        self._registry[name] = fn

    def call_function(
        self, name: str, df: pl.DataFrame, args: List[Any]
    ) -> pl.DataFrame:
        if name not in self._registry:
            raise ValueError(f"No function registered with the name '{name}'.")

        LOGGER.info("applying fn %s to dataframe %s", name, df.shape)
        fn = self._registry[name]
        result_df = fn(df, args)

        if result_df is None:
            raise ValueError(f"function {name} returned None")

        return result_df

    def list_functions(self) -> List[str]:
        return list(self._registry.keys())
