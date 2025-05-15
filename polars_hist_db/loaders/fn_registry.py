import logging
from typing import Any, List, Dict
import polars as pl

from .fn_transform import DataFrameTransform
from .fn_builtins import ApplyTypeCasts, CombineColumns, NullIfGte

LOGGER = logging.getLogger(__name__)

RegistryStore = Dict[str, DataFrameTransform]


class FunctionRegistry:
    _borg: Dict[str, Any] = {"_registry": None}

    def __init__(self) -> None:
        self.__dict__ = self._borg
        self._registry: RegistryStore = self._one_time_init()

    def _one_time_init(self) -> RegistryStore:
        if self._registry is None:
            self._registry = dict()
            self.register_function(NullIfGte)
            self.register_function(ApplyTypeCasts)
            self.register_function(CombineColumns)

        return self._registry

    def delete_function(self, name: str) -> None:
        if name in self._registry:
            del self._registry[name]

    def register_function(self, transform: DataFrameTransform) -> None:
        if transform.name() in self._registry:
            raise ValueError(
                f"A function with the name '{transform.name()}' is already registered."
            )
        self._registry[transform.name()] = transform

    def call_function(
        self, name: str, df: pl.DataFrame, args: List[Any]
    ) -> pl.DataFrame:
        if name not in self._registry:
            raise ValueError(f"No function registered with the name '{name}'.")

        LOGGER.info("applying fn %s to dataframe %s", name, df.shape)
        class_obj = self._registry[name]
        fn = class_obj(args)
        result_df = fn.apply(df)

        if result_df is None:
            raise ValueError(f"function {name} returned None")

        return result_df

    def list_functions(self) -> List[str]:
        return list(self._registry.keys())
