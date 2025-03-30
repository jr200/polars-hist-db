from dataclasses import dataclass
from typing import Optional, Sequence
import os

import polars as pl

from .parser import parse_col_exprs


@dataclass
class Pipeline:
    items: pl.DataFrame
    _extract_spec: Optional[pl.DataFrame] = None

    def __post_init__(self):
        schema = {
            "table": pl.Utf8,
            "type": pl.Utf8,
            "columns": pl.List(pl.List(pl.Utf8)),
        }

        for pipeline_item in self.items:
            columns = []
            for col_name, col_def_name, col_is_required in parse_col_exprs(
                pipeline_item.get("columns", [])
            ):
                columns.append(
                    [
                        col_name,
                        col_def_name,
                        "required" if col_is_required else "optional",
                    ]
                )

            pipeline_item["columns"] = columns

        items = pl.from_records(self.items, schema_overrides=schema).with_columns(
            pl.col("type").fill_null("extract")
        )

        if len(items.filter(type="primary")) != 1:
            raise ValueError("invalid pipeline, required exactly one primary table")

        self.items = items

        self._extract_spec = (
            self.items.select("table", "columns")
            .explode("columns")
            .select(
                "table",
                source=pl.col("columns").list.get(1),
                target=pl.col("columns").list.get(0),
                required=pl.col("columns").list.get(2) == "required",
            )
        )

    def item_type(self, table: str) -> str:
        df = self.items.filter(table=table).select("type").unique()
        if len(df) != 1:
            raise ValueError("invalid pipeline")

        result: str = df[0, "type"]
        return result

    def extract_items(self, table: str) -> pl.DataFrame:
        if self._extract_spec is None:
            raise ValueError("missing _extract_spec")

        df = self._extract_spec.filter(table=table).drop("table")
        return df

    def get_main_table_name(self) -> str:
        if self.items.is_empty():
            raise ValueError("missing pipeline")

        table_name: str = self.items.filter(type="primary")[0, "table"]
        return table_name


@dataclass
class DatasetConfig:
    name: str
    delta_table_schema: str
    search_paths: pl.DataFrame
    pipeline: Pipeline
    scrape_limit: Optional[int] = None
    base_dir: Optional[str] = None

    def __post_init__(self):
        if not isinstance(self.pipeline, Pipeline):
            self.pipeline = Pipeline(items=self.pipeline)

        if not isinstance(self.search_paths, pl.DataFrame):
            for search_path in self.search_paths:
                if "root_path" in search_path:
                    path = search_path["root_path"]
                    if not os.path.isabs(path):
                        abs_path = os.path.normpath(os.path.join(self.base_dir, path))
                        search_path["root_path"] = abs_path

            self.search_paths = pl.from_records(self.search_paths)


@dataclass
class DatasetsConfig:
    datasets: Sequence[DatasetConfig]
    base_dir: Optional[str]

    def __post_init__(self):
        self.datasets = [
            DatasetConfig(**ds_dict, base_dir=self.base_dir)
            for ds_dict in self.datasets
        ]

    def __getitem__(self, name: str) -> Optional[DatasetConfig]:
        try:
            ds = next((ds for ds in self.datasets if ds.name == name), None)
            return ds
        except StopIteration:
            return None
