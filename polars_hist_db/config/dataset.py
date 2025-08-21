from dataclasses import dataclass, field
from typing import Dict, List, Literal, Optional, Sequence, Tuple

import polars as pl
import logging

from .parser_config import IngestionColumnConfig
from .table import TableColumnConfig, TableConfigs
from .input.input_source import InputConfig

LOGGER = logging.getLogger(__name__)


@dataclass
class DeltaConfig:
    drop_unchanged_rows: bool = False
    on_duplicate_key: Literal["error", "take_last", "take_first"] = "error"
    prefill_nulls_with_default: bool = False

    # tracks the finality of rows in the target (temporal) table
    # disabled: no tracking, rows are not deleted from the target table
    # dropout: rows are deleted from the target table if they are not present in the source table
    # manual: a separate column tracks the finality of rows in the target table
    row_finality: Literal["disabled", "dropout", "manual"] = "disabled"

    # for debugging purposes, we can set this to False to keep the delta table
    is_temporary_table: bool = True

    def tmp_table_name(self, table_name: str) -> str:
        return f"__{table_name}_tmp"


@dataclass
class Pipeline:
    items: pl.DataFrame

    def __post_init__(self):
        item_schema = IngestionColumnConfig.df_schema()
        items = (
            pl.from_records(self.items)
            .with_row_index(name="id")
            .explode("columns")
            .unnest("columns")
        )

        items = items.with_columns(
            pl.lit(None).cast(t).alias(c)
            for c, t in item_schema.items()
            if c not in items.columns
        ).with_columns(
            pl.col("type").fill_null("extract"),
            pl.col("required").fill_null(False),
            pl.col("nullable").fill_null(True),
            pl.col("deduce_foreign_key").fill_null(False),
            pl.when(pl.col("column_type").is_null())
            .then(
                pl.when(pl.col("target").is_null())
                .then(pl.lit("dsv_only"))
                .when(pl.col("source").is_null())
                .then(pl.lit("computed"))
                .otherwise(pl.lit("data"))
            )
            .otherwise(pl.col("column_type"))
            .alias("column_type"),
        )

        if len(items.filter(type="primary").select("table").unique()) != 1:
            raise ValueError("invalid pipeline, required exactly one primary table")

        self.items = items

    def build_ingestion_column_definitions(
        self, all_tables: TableConfigs
    ) -> List[IngestionColumnConfig]:
        tmp_cols = self.items.filter(
            pl.col("column_type").is_in(["dsv_only", "time_partition_only"])
        )
        pipeline_cols = self.items.filter(
            pl.col("column_type").is_in(["dsv_only", "time_partition_only"]).not_()
        )
        merged_cols = pl.concat([pipeline_cols, tmp_cols])
        all_dfs = self._merge_with_table_config(
            merged_cols, ["schema", "table", "source", "target"], all_tables
        )

        schema_keys = IngestionColumnConfig.df_schema().keys()
        result = []
        for df in all_dfs:
            df = df.with_columns(
                name=pl.coalesce("target", "source"),
            )
            for row in df.iter_rows(named=True):
                row_dict = {c: row[c] for c in schema_keys if c in row}
                cc = IngestionColumnConfig(**row_dict)
                result.append(cc)

        return result

    def _merge_with_table_config(
        self,
        pipeline_cols: pl.DataFrame,
        unique_key: List[str],
        all_tables: TableConfigs,
    ) -> List[pl.DataFrame]:
        all_dfs = []
        for tbl_cfg in all_tables.items:
            tbl_cols = tbl_cfg.columns_df().rename({"data_type": "tbl_data_type"})
            pipeline_tbl = pipeline_cols.filter(table=tbl_cfg.name)
            merged_tbl = (
                pipeline_tbl.unique(subset=unique_key, maintain_order=True)
                .drop([c for c in pipeline_tbl.columns if c in tbl_cols.columns])
                .join(tbl_cols, left_on=["target"], right_on=["name"], how="left")
                .with_columns(
                    pl.coalesce(
                        "target_data_type", "tbl_data_type", "ingestion_data_type"
                    ).alias("target_data_type")
                )
                .with_columns(
                    pl.coalesce(
                        "ingestion_data_type", "tbl_data_type", "target_data_type"
                    ).alias("ingestion_data_type")
                )
            )

            missing_types = merged_tbl.select(
                "id",
                "table",
                "source",
                "target",
                "ingestion_data_type",
                "target_data_type",
                "tbl_data_type",
                is_missing=pl.col("ingestion_data_type").is_null()
                | pl.col("target_data_type").is_null(),
            ).filter(pl.col("is_missing"))
            if not missing_types.is_empty():
                LOGGER.error(f"Missing types for dataframe {missing_types}")
                raise ValueError(f"Missing types in {tbl_cfg.schema}.{tbl_cfg.name}")

            all_dfs.append(merged_tbl)

        all_dfs = [df for df in all_dfs if not df.is_empty()]

        return all_dfs

    def build_delta_table_column_configs(
        self, all_tables: TableConfigs, table_name: str
    ) -> List[TableColumnConfig]:
        pipeline_cols = self.items.filter(
            pl.col("column_type").is_in(["data", "computed"])
        )
        all_dfs = self._merge_with_table_config(
            pipeline_cols, ["table", "source", "target"], all_tables
        )

        candidate_cols = (
            pl.concat(all_dfs)
            .sort("type")
            .with_columns(
                name=pl.coalesce("source", "target"),
                data_type=pl.col("target_data_type"),
            )
            .unique(subset=["name"], keep="last", maintain_order=True)
            .drop("target", "source")
        )

        columns = TableColumnConfig.from_dataframe(
            candidate_cols, table_name_override=table_name
        )

        return columns

    def get_header_map(self, table: str) -> Dict[str, str]:
        will_copy = (
            self.items.filter(table=table).select("source", "target").drop_nulls()
        )
        return {row["target"]: row["source"] for row in will_copy.iter_rows(named=True)}

    def item_type(self, table: str) -> str:
        df = self.items.filter(table=table).select("type").unique()
        if len(df) != 1:
            raise ValueError("invalid pipeline")

        result: str = df[0, "type"]
        return result

    def extract_items(self, pipeline_id: int) -> pl.DataFrame:
        df = (
            self.items.filter(id=pipeline_id)
            # .drop("table")
            .filter(pl.col("column_type").is_in(["data", "computed"]))
            .with_columns(source=pl.coalesce("source", "target"))
            .select("table", "source", "target", "required", "deduce_foreign_key")
        )

        return df

    def get_main_table_name(self) -> Tuple[str, str]:
        if self.items.is_empty():
            raise ValueError("missing pipeline")

        primary_item = (
            self.items.filter(type="primary").select("table", "schema").unique()
        )
        if len(primary_item) != 1:
            raise ValueError("invalid pipeline, required exactly one primary table")

        table_name: str = primary_item[0, "table"]
        table_schema: str = primary_item[0, "schema"]
        return table_schema, table_name

    def get_table_names(self) -> List[str]:
        return self.items["table"].unique(maintain_order=True).to_list()

    def get_pipeline_items(self) -> Dict[int, str]:
        pipeline_items = self.items.select("id", "table").unique(maintain_order=True)
        result: Dict[int, str] = dict(pipeline_items.iter_rows())
        return result


@dataclass
class TimePartition:
    column: str
    bucket_interval: str
    bucket_strategy: Literal["round_up", "round_down"] = "round_up"
    unique_strategy: Literal["first", "last"] = "last"


@dataclass
class DatasetConfig:
    name: str
    delta_table_schema: str
    input_config: InputConfig
    pipeline: Pipeline
    scrape_limit: int = -1
    time_partition: Optional[TimePartition] = None
    null_values: Optional[Sequence[str]] = None
    delta_config: DeltaConfig = field(default_factory=DeltaConfig)
    config_file_path: Optional[str] = None

    def __post_init__(self):
        if not isinstance(self.delta_config, DeltaConfig):
            self.delta_config = DeltaConfig(**self.delta_config)

        if not self.scrape_limit:
            self.scrape_limit = -1

        if not isinstance(self.pipeline, Pipeline):
            self.pipeline = Pipeline(items=self.pipeline)

        if not isinstance(self.input_config, InputConfig):
            if isinstance(self.input_config, dict):
                self.input_config["config_file_path"] = self.config_file_path
            self.input_config = InputConfig.from_dict(self.input_config)

        if self.time_partition is not None and not isinstance(
            self.time_partition, TimePartition
        ):
            self.time_partition = TimePartition(**self.time_partition)


@dataclass
class DatasetsConfig:
    datasets: Sequence[DatasetConfig]
    config_file_path: Optional[str] = None

    def __post_init__(self):
        self.datasets = [
            DatasetConfig(**ds_dict, config_file_path=self.config_file_path)
            for ds_dict in self.datasets
        ]

    def __getitem__(self, key: str) -> Optional[DatasetConfig]:
        try:
            if isinstance(key, int):
                return self.datasets[key]

            ds = next((ds for ds in self.datasets if ds.name == key), None)
            return ds
        except StopIteration:
            return None
