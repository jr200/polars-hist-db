from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence
import os

import polars as pl
import logging

from .delta_config import DeltaColumnConfig
from .table import TableColumnConfig, TableConfigs

LOGGER = logging.getLogger(__name__)


@dataclass
class Pipeline:
    items: pl.DataFrame

    def __post_init__(self):
        item_schema = DeltaColumnConfig.df_schema()
        items = (
            pl
            .from_records(self.items)
            .explode("columns")
            .unnest("columns")
        )

        items = (
            items
            .with_columns(
                pl.lit(None).cast(t).alias(c)
                for c, t in item_schema.items()
                if c not in items.columns
            )
            .with_columns(
                pl.col("type").fill_null("extract"),
                pl.col("required").fill_null(False),
                pl.col("nullable").fill_null(True),
                pl.when(pl.col("column_type").is_null())
                .then(
                    pl
                    .when(pl.col("target").is_null())
                    .then(pl.lit("dsv_only"))
                    .when(pl.col("source").is_null())
                    .then(pl.lit("computed"))
                    .otherwise(pl.lit("data"))
                )
                .otherwise(pl.col("column_type"))
                .alias("column_type")
            )
        )

        if len(items.filter(type="primary").select("table").unique()) != 1:
            raise ValueError("invalid pipeline, required exactly one primary table")

        requires_dtype = (
            items
            .filter(pl.col("target").is_null() & pl.col("data_type").is_null())
            .unique(subset=["table", "source"])
        )

        if not requires_dtype.is_empty():
            LOGGER.error("invalid pipeline, required dtype for columns: %s", requires_dtype)
            raise ValueError("invalid pipeline, required dtype for temporary columns without 'name'")

        self.items = items


    def build_input_column_definitions(self, all_tables: TableConfigs) -> List[DeltaColumnConfig]:

        tmp_cols = self.items.filter(pl.col("column_type").is_in(["dsv_only", "time_partition"]))
        pipeline_cols = self.items.filter(pl.col("column_type").is_in(["dsv_only", "time_partition"]).not_())
        all_dfs = self._merge_with_table_config(pipeline_cols, all_tables)
        all_dfs.append(tmp_cols)

        
        schema_keys = DeltaColumnConfig.df_schema().keys()
        result = []
        for df in all_dfs:
            df = df.with_columns(name=pl.coalesce("target", "source"))
            for row in df.iter_rows(named=True):
                row_dict = {c: row[c] for c in schema_keys if c in row}
                cc = DeltaColumnConfig(**row_dict)
                result.append(cc)

        return result


    def _merge_with_table_config(self, pipeline_cols: pl.DataFrame, all_tables: TableConfigs) -> List[pl.DataFrame]:

        all_dfs = []
        for tbl_cfg in all_tables.items:
            tbl_cols = tbl_cfg.columns_df()
            pipeline_tbl = pipeline_cols.filter(table=tbl_cfg.name)
            merged_tbl = (
                pipeline_tbl
                .drop([c for c in pipeline_tbl.columns if c in tbl_cols.columns])
                .join(tbl_cols, left_on=["target"], right_on=["name"], how="left")
            )
            
            all_dfs.append(merged_tbl)

        all_dfs = [df for df in all_dfs if not df.is_empty()]

        return all_dfs


    def build_delta_table_column_configs(self, all_tables: TableConfigs) -> List[TableColumnConfig]:

        pipeline_cols = self.items.filter(pl.col("column_type").is_in(["data", "computed"]))
        all_dfs = self._merge_with_table_config(pipeline_cols, all_tables)

        candidate_cols = (
            pl.concat(all_dfs)
            .sort("type")
            .unique(subset=["source"], keep="last", maintain_order=True)
            .with_columns(name=pl.coalesce("source", "target"))
            .drop("target", "source")
        )

        columns = TableColumnConfig.from_dataframe(candidate_cols)

        return columns


    def get_header_map(self, table: str) -> Dict[str, str]:
        will_copy = self.items.filter(table=table).select("source", "target").drop_nulls()
        return {row["target"]: row["source"] for row in will_copy.iter_rows(named=True)}


    def item_type(self, table: str) -> str:
        df = self.items.filter(table=table).select("type").unique()
        if len(df) != 1:
            raise ValueError("invalid pipeline")

        result: str = df[0, "type"]
        return result

    def extract_items(self, table: str) -> pl.DataFrame:
        df = (
            self.items
            .filter(table=table)
            .drop("table")
            .filter(pl.col("column_type").is_in(["data", "computed"]))
            .with_columns(source=pl.coalesce("source", "target"))
        )

        return df

    def get_main_table_name(self) -> str:
        if self.items.is_empty():
            raise ValueError("missing pipeline")

        table_name: str = self.items.filter(type="primary")[0, "table"]
        return table_name
    
    def get_table_names(self) -> List[str]:
        return self.items["table"].unique(maintain_order=True).to_list()




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
