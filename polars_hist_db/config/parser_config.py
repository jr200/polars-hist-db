import logging
from dataclasses import dataclass, field
from typing import Any, Literal

import polars as pl

from .transform_fn_registry import TransformFnRegistry

LOGGER = logging.getLogger(__name__)


@dataclass
class IngestionColumnConfig:
    column_type: Literal[
        "data", "computed", "input_only", "dsv_only", "time_partition_only"
    ]
    schema: str
    table: str | None
    ingestion_data_type: str
    target_data_type: str
    source: str | None = None
    target: str | None = None
    transforms: dict[str, Any] = field(default_factory=dict)
    aggregation: str | None = None
    deduce_foreign_key: bool = False
    value_if_missing: str | None = None
    nullable: bool | None = True
    required: bool = False

    def __post_init__(self):
        if self.schema is None:
            raise ValueError("schema is required for IngestionColumnConfig")

    @classmethod
    def df_schema(cls) -> pl.Schema:
        schema: dict[str, pl.DataType] = {
            "schema": pl.Utf8(),
            "table": pl.Utf8(),
            "source": pl.Utf8(),
            "target": pl.Utf8(),
            "target_data_type": pl.Utf8(),
            "ingestion_data_type": pl.Utf8(),
            "column_type": pl.Utf8(),
            "required": pl.Boolean(),
            "transforms": pl.Struct(
                {k: pl.List(pl.Utf8()) for k in TransformFnRegistry().list_functions()}
            ),
            "aggregation": pl.Utf8(),
            "deduce_foreign_key": pl.Boolean(),
            "value_if_missing": pl.Utf8(),
            "nullable": pl.Boolean(),
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
