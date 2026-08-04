import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal, cast

from .input.input_source import InputConfig
from .parser_config import IngestionColumnConfig
from .table import TableColumnConfig, TableConfigs

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


PipelineColumnType = Literal[
    "data",
    "computed",
    "input_only",
    "dsv_only",
    "time_partition_only",
]


@dataclass(frozen=True)
class PipelineColumn:
    pipeline_id: int
    schema: str
    table: str
    item_type: str
    column_type: PipelineColumnType
    source: str | None = None
    target: str | None = None
    target_data_type: str | None = None
    ingestion_data_type: str | None = None
    required: bool = False
    transforms: Mapping[str, Any] = field(default_factory=dict)
    aggregation: str | None = None
    deduce_foreign_key: bool = False
    value_if_missing: str | None = None
    nullable: bool | None = None
    omit_row_if_null: bool = False


@dataclass(frozen=True)
class PipelineExtractColumn:
    table: str
    source: str
    target: str
    required: bool = False
    deduce_foreign_key: bool = False
    omit_row_if_null: bool = False


@dataclass(init=False)
class Pipeline:
    items: tuple[PipelineColumn, ...]
    _header_maps: dict[str, dict[str, str]] = field(
        init=False, repr=False, compare=False
    )
    _item_types: dict[str, tuple[str, ...]] = field(
        init=False, repr=False, compare=False
    )
    _extract_items: dict[int, tuple[PipelineExtractColumn, ...]] = field(
        init=False, repr=False, compare=False
    )
    _main_table_name: tuple[str, str] = field(init=False, repr=False, compare=False)
    _table_names: tuple[str, ...] = field(init=False, repr=False, compare=False)
    _pipeline_items: dict[int, tuple[str, str]] = field(
        init=False, repr=False, compare=False
    )

    def __init__(self, items: Sequence[Mapping[str, Any] | PipelineColumn]) -> None:
        if all(isinstance(item, PipelineColumn) for item in items):
            columns = tuple(cast(PipelineColumn, item) for item in items)
        else:
            raw_items = cast(Sequence[Mapping[str, Any]], items)
            parsed_columns: list[PipelineColumn] = []
            for pipeline_id, item in enumerate(raw_items):
                item_columns: Sequence[Mapping[str, Any]] = item.get("columns") or ({},)
                parsed_columns.extend(
                    self._parse_column(pipeline_id, item, column)
                    for column in item_columns
                )
            columns = tuple(parsed_columns)

        primary_items = {
            (column.schema, column.table)
            for column in columns
            if column.item_type == "primary"
        }
        if len(primary_items) != 1:
            raise ValueError("invalid pipeline, required exactly one primary table")

        self.items = columns
        self._main_table_name = next(iter(primary_items))
        self._table_names = tuple(dict.fromkeys(column.table for column in columns))
        self._pipeline_items = {}
        for column in columns:
            self._pipeline_items.setdefault(
                column.pipeline_id, (column.schema, column.table)
            )

        self._header_maps = {table: {} for table in self._table_names}
        for column in columns:
            if column.source is not None and column.target is not None:
                self._header_maps[column.table][column.target] = column.source

        item_types: dict[str, list[str]] = {table: [] for table in self._table_names}
        for column in columns:
            if column.item_type not in item_types[column.table]:
                item_types[column.table].append(column.item_type)
        self._item_types = {table: tuple(types) for table, types in item_types.items()}

        self._extract_items = {pipeline_id: () for pipeline_id in self._pipeline_items}
        for pipeline_id in self._pipeline_items:
            self._extract_items[pipeline_id] = tuple(
                PipelineExtractColumn(
                    table=column.table,
                    source=column.source or column.target or "",
                    target=column.target or "",
                    required=column.required,
                    deduce_foreign_key=column.deduce_foreign_key,
                    omit_row_if_null=column.omit_row_if_null,
                )
                for column in columns
                if column.pipeline_id == pipeline_id
                and column.column_type in {"data", "computed"}
            )

        if any(
            not column.source or not column.target
            for extract_columns in self._extract_items.values()
            for column in extract_columns
        ):
            raise ValueError("data and computed pipeline columns require a target")

    @staticmethod
    def _parse_column(
        pipeline_id: int,
        item: Mapping[str, Any],
        column: Mapping[str, Any],
    ) -> PipelineColumn:
        source = column.get("source")
        target = column.get("target")
        column_type = column.get("column_type")
        if column_type is None:
            column_type = (
                "dsv_only"
                if target is None
                else "computed"
                if source is None
                else "data"
            )

        return PipelineColumn(
            pipeline_id=pipeline_id,
            schema=item["schema"],
            table=item["table"],
            item_type=item.get("type") or "extract",
            column_type=cast(PipelineColumnType, column_type),
            source=source,
            target=target,
            target_data_type=column.get("target_data_type"),
            ingestion_data_type=column.get("ingestion_data_type"),
            required=bool(column.get("required")),
            transforms=column.get("transforms") or {},
            aggregation=column.get("aggregation"),
            deduce_foreign_key=bool(column.get("deduce_foreign_key")),
            value_if_missing=column.get("value_if_missing"),
            nullable=column.get("nullable"),
            omit_row_if_null=bool(column.get("omit_row_if_null")),
        )

    @staticmethod
    def _resolved_types(
        column: PipelineColumn,
        table_column: TableColumnConfig | None,
        table_schema: str,
        table_name: str,
    ) -> tuple[str, str]:
        table_data_type = table_column.data_type if table_column is not None else None
        target_data_type = (
            column.target_data_type or table_data_type or column.ingestion_data_type
        )
        ingestion_data_type = (
            column.ingestion_data_type or table_data_type or target_data_type
        )
        if target_data_type is None or ingestion_data_type is None:
            LOGGER.error("Missing types for pipeline column %s", column)
            raise ValueError(f"Missing types in {table_schema}.{table_name}")
        return ingestion_data_type, target_data_type

    def build_ingestion_column_definitions(
        self, all_tables: TableConfigs
    ) -> list[IngestionColumnConfig]:
        temporary_types = {"input_only", "dsv_only", "time_partition_only"}
        ordered_columns = [
            column for column in self.items if column.column_type not in temporary_types
        ] + [column for column in self.items if column.column_type in temporary_types]
        result = []
        for table in all_tables.items:
            table_columns = {column.name: column for column in table.columns}
            seen = set()
            for column in ordered_columns:
                if column.table != table.name:
                    continue
                key = (column.schema, column.table, column.source, column.target)
                if key in seen:
                    continue
                seen.add(key)
                table_column = table_columns.get(column.target or "")
                ingestion_type, target_type = self._resolved_types(
                    column, table_column, table.schema, table.name
                )
                result.append(
                    IngestionColumnConfig(
                        column_type=column.column_type,
                        schema=column.schema,
                        table=(column.table if table_column is not None else None),
                        ingestion_data_type=ingestion_type,
                        target_data_type=target_type,
                        source=column.source,
                        target=column.target,
                        transforms=dict(column.transforms),
                        aggregation=column.aggregation,
                        deduce_foreign_key=column.deduce_foreign_key,
                        value_if_missing=column.value_if_missing,
                        nullable=(
                            column.nullable
                            if column.nullable is not None
                            else table_column.nullable
                            if table_column
                            else None
                        ),
                        required=column.required,
                    )
                )
        return result

    def build_delta_table_column_configs(
        self, all_tables: TableConfigs, table_name: str
    ) -> list[TableColumnConfig]:
        candidates = []
        for table in all_tables.items:
            table_columns = {column.name: column for column in table.columns}
            seen = set()
            for column in self.items:
                if column.table != table.name or column.column_type not in {
                    "data",
                    "computed",
                    "time_partition_only",
                }:
                    continue
                key = (column.table, column.source, column.target)
                if key in seen:
                    continue
                seen.add(key)
                table_column = table_columns.get(column.target or "")
                _, target_type = self._resolved_types(
                    column, table_column, table.schema, table.name
                )
                candidates.append(
                    (column, table_column, column.source or column.target, target_type)
                )

        candidates.sort(key=lambda candidate: candidate[0].item_type)
        last_index = {name: index for index, (_, _, name, _) in enumerate(candidates)}
        return [
            TableColumnConfig(
                table=table_name,
                name=name,
                data_type=data_type,
                default_value=(
                    table_column.default_value if table_column is not None else None
                ),
                autoincrement=(
                    table_column.autoincrement if table_column is not None else False
                ),
                nullable=(table_column.nullable if table_column is not None else True),
                unique_constraint=(
                    list(table_column.unique_constraint)
                    if table_column is not None
                    else []
                ),
            )
            for index, (_, table_column, name, data_type) in enumerate(candidates)
            if name is not None and last_index[name] == index
        ]

    def get_header_map(self, table: str) -> dict[str, str]:
        return dict(self._header_maps.get(table, {}))

    def item_type(self, table: str) -> str:
        item_types = self._item_types.get(table, ())
        if len(item_types) != 1:
            raise ValueError("invalid pipeline")

        return item_types[0]

    def extract_items(self, pipeline_id: int) -> tuple[PipelineExtractColumn, ...]:
        return self._extract_items.get(pipeline_id, ())

    def get_main_table_name(self) -> tuple[str, str]:
        return self._main_table_name

    def get_table_names(self) -> list[str]:
        return list(self._table_names)

    def get_pipeline_items(self) -> dict[int, tuple[str, str]]:
        return dict(self._pipeline_items)


@dataclass
class TimePartition:
    column: str
    bucket_interval: str
    bucket_strategy: Literal["round_up", "round_down"] = "round_up"
    unique_strategy: Literal["first", "last"] = "last"


@dataclass(frozen=True)
class ValidTimeConfig:
    table: str
    from_column: str
    schema: str | None = None
    to_column: str | None = None

    def matches(self, table_schema: str, table_name: str) -> bool:
        if self.table != table_name:
            return False
        return self.schema is None or self.schema == table_schema


@dataclass
class DatasetConfig:
    name: str
    delta_table_schema: str
    input_config: InputConfig
    pipeline: Pipeline
    scrape_limit: int = -1
    time_partition: TimePartition | None = None
    null_values: Sequence[str] | None = None
    delta_config: DeltaConfig = field(default_factory=DeltaConfig)
    valid_time: Sequence[ValidTimeConfig] = field(default_factory=tuple)
    config_file_path: str | None = None

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

        self.valid_time = [
            item if isinstance(item, ValidTimeConfig) else ValidTimeConfig(**item)
            for item in self.valid_time
        ]

    def valid_time_for_table(
        self, table_schema: str, table_name: str
    ) -> ValidTimeConfig | None:
        return next(
            (
                valid_time
                for valid_time in self.valid_time
                if valid_time.matches(table_schema, table_name)
            ),
            None,
        )


@dataclass
class DatasetsConfig:
    datasets: Sequence[DatasetConfig]
    config_file_path: str | None = None

    def __post_init__(self):
        self.datasets = [
            DatasetConfig(**ds_dict, config_file_path=self.config_file_path)
            for ds_dict in self.datasets
        ]

    def __getitem__(self, key: str) -> DatasetConfig | None:
        try:
            if isinstance(key, int):
                return self.datasets[key]

            ds = next((ds for ds in self.datasets if ds.name == key), None)
            return ds
        except StopIteration:
            return None
