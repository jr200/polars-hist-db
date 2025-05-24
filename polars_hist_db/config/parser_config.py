from dataclasses import dataclass, field
from typing import Any, Dict, Literal, Optional

import polars as pl
import logging

from .fn_registry import FunctionRegistry


LOGGER = logging.getLogger(__name__)


@dataclass
class IngestionColumnConfig:
    column_type: Literal["data", "computed", "dsv_only", "time_partition_only"]
    table: str
    ingestion_data_type: str
    target_data_type: str
    source: Optional[str] = None
    target: Optional[str] = None
    transforms: Dict[str, Any] = field(default_factory=dict)
    aggregation: Optional[str] = None
    deduce_foreign_key: bool = False
    value_if_missing: Optional[str] = None
    nullable: bool = True
    required: bool = False

    def __post_init__(self): ...

    @classmethod
    def df_schema(cls) -> pl.Schema:
        schema: Dict[str, pl.DataClassType] = {
            "table": pl.Utf8,
            "source": pl.Utf8,
            "target": pl.Utf8,
            "target_data_type": pl.Utf8,
            "ingestion_data_type": pl.Utf8,
            "column_type": pl.Utf8,
            "required": pl.Boolean,
            "transforms": pl.Struct(
                {k: pl.List(pl.Utf8) for k in FunctionRegistry().list_functions()}
            ),
            "aggregation": pl.Utf8,
            "deduce_foreign_key": pl.Boolean,
            "value_if_missing": pl.Utf8,
            "nullable": pl.Boolean,
        }

        return pl.Schema(schema)

    def df(self) -> pl.DataFrame:
        result = pl.DataFrame(
            [list(self.__dict__.values())],
            schema=list(self.__dict__.keys()),
            schema_overrides=self.df_schema(),
            orient="row",
        )

        return result

    def __repr__(self) -> str:
        return f"ParserColumnConfig({', '.join(f'{k}={v!r}' for k, v in self.__dict__.items())})"
