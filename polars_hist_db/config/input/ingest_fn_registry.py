from datetime import datetime
import logging
from typing import Any, Callable, List, Dict
import polars as pl

LOGGER = logging.getLogger(__name__)

IngestFnSignature = Callable[[Any, datetime, Dict[str, Any]], pl.DataFrame]
IngestRegistryStore = Dict[str, IngestFnSignature]


class IngestFnRegistry:
    _borg: Dict[str, Any] = {"_registry": None}

    def __init__(self) -> None:
        self.__dict__ = self._borg
        self._registry: IngestRegistryStore = self._one_time_init()

    def _one_time_init(self) -> IngestRegistryStore:
        if self._registry is None:
            self._registry = dict()

        return self._registry

    def delete_function(self, name: str) -> None:
        if name in self._registry:
            del self._registry[name]

    def register_function(
        self, name: str, fn: IngestFnSignature, allow_overwrite: bool = False
    ) -> None:
        if name in self._registry and not allow_overwrite:
            raise ValueError(
                f"An ingest function with the name '{name}' is already registered."
            )

        LOGGER.debug("added ingest function %s to registry", name)
        self._registry[name] = fn

    def call_function(
        self,
        payload: Any,
        ts: datetime,
        name: str,
        args: Dict[str, Any],
    ) -> pl.DataFrame:
        if name not in self._registry:
            raise ValueError(f"No ingest function registered with the name '{name}'.")

        LOGGER.debug("applying ingest fn %s to payload", name)
        fn = self._registry[name]
        result_df = fn(payload, ts, args)

        if result_df is None:
            raise ValueError(f"ingest function {name} returned None")

        return result_df

    def list_functions(self) -> List[str]:
        return list(self._registry.keys())
