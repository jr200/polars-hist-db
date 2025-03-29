import copy
from dataclasses import dataclass, field
from typing import Any, Iterable, List, Literal, Mapping, Optional

import polars as pl
from sqlalchemy import Column, Identity
import yaml

from .parser import flatten_list, parse_col_spec
from ..types import PolarsType, SQLType, SQLAlchemyType


@dataclass
class TimePartition:
    column: str = ""
    truncate: str = ""
    unique_strategy: Literal["first", "last"] = "last"


@dataclass
class DeltaConfig:
    drop_unchanged_rows: bool = False
    on_duplicate_key: Literal["error", "take_last", "take_first"] = "error"
    prefill_nulls_with_default: bool = False
    time_partition: Optional[TimePartition] = None

    # tracks the finality of rows in the target (temporal) table
    # disabled: no tracking, rows are not deleted from the target table
    # dropout: rows are deleted from the target table if they are not present in the source table
    # manual: a separate column tracks the finality of rows in the target table
    row_finality: Literal["disabled", "dropout", "manual"] = "disabled"

    def __post_init__(self):
        if self.time_partition is not None and not isinstance(
            self.time_partition, TimePartition
        ):
            self.time_partition = TimePartition(**self.time_partition)

    def tmp_table_name(self, table_name: str) -> str:
        return f"__{table_name}_tmp"


@dataclass
class ColumnConfig:
    name: str
    data_type: str
    transforms: Mapping[str, Any] = field(default_factory=dict)
    aggregation: Optional[str] = None
    deduce_foreign_key: bool = False
    default_value: Optional[str] = None
    header: str = ""
    autoincrement: bool = False
    nullable: bool = True
    unique_constraint: Iterable[str] = field(default_factory=tuple)

    def __post_init__(self):
        self.header = self.header or self.name

    def df(self) -> pl.DataFrame:
        result = pl.DataFrame(
            [list(self.__dict__.values())],
            schema=list(self.__dict__.keys()),
            schema_overrides={
                "aggregation": pl.Utf8,
                "default_value": pl.Utf8,
                "header": pl.Utf8,
                "transforms": pl.Struct,
                "unique_constraint": pl.List(pl.Utf8),
            },
            orient="row",
        )

        return result


def __repr__(self) -> str:
    return f"ColumnConfig({', '.join(f'{k}={v!r}' for k, v in self.__dict__.items())})"


@dataclass
class ColumnDefinitions:
    column_definitions: List[ColumnConfig] = field(default_factory=list)

    def __post_init__(self):
        self.column_definitions = [
            col if isinstance(col, ColumnConfig) else ColumnConfig(**col)
            for col in self.column_definitions
        ]

    def __getitem__(self, name: str) -> ColumnConfig:
        col_name, col_def_name, _ = parse_col_spec(name)
        col_defs_copy = self.clone().column_definitions

        col_def = next(
            (c for c in col_defs_copy if c.name == col_def_name),
            None,
        )

        if col_def is None:
            col_def = next(
                (c for c in col_defs_copy if c.name == col_name),
                None,
            )
        else:
            col_def.name = col_name

        if col_def:
            return col_def

        raise ValueError(f"ColumnConfig {name} not found")

    def clone(self) -> "ColumnDefinitions":
        return ColumnDefinitions(copy.deepcopy(self.column_definitions))


@dataclass
class TableConfig:
    name: str
    schema: str
    columns: List[str] = field(default_factory=list)
    forbid_drop_table: bool = False
    foreign_keys: Iterable["ForeignKeyConfig"] = field(default_factory=tuple)
    is_temporal: bool = False
    primary_keys: Iterable[str] = field(default_factory=tuple)
    delta_config: DeltaConfig = field(default_factory=DeltaConfig)
    column_definitions: ColumnDefinitions = field(default_factory=ColumnDefinitions)

    def __post_init__(self):
        if not isinstance(self.delta_config, DeltaConfig):
            self.delta_config = DeltaConfig(**self.delta_config)

        parsed_cols = []
        found_col_defs = []
        column_definitions = self.column_definitions.clone()
        for col_expr in flatten_list(self.columns):
            _, col_def_name, _ = parse_col_spec(col_expr)

            col_def = column_definitions[col_def_name]
            found_col_defs.append(col_def)
            parsed_cols.append(col_expr)

        self.columns = parsed_cols
        self.column_definitions = ColumnDefinitions(column_definitions=found_col_defs)

        self.foreign_keys = [
            fk if isinstance(fk, ForeignKeyConfig) else ForeignKeyConfig(**fk)
            for fk in self.foreign_keys
        ]

    def table_dependencies(self) -> Iterable[str]:
        deps = [self.name]

        for fk in self.foreign_keys:
            deps.append(fk.references.table.name)

        return deps

    @classmethod
    def from_yaml(cls, file_path: str) -> "TableConfig":
        with open(file_path, "r") as file:
            config_dict = yaml.safe_load(file)

        result = TableConfig(**config_dict["table"])
        return result

    def _resolve_foreign_keys(self, *ref_configs: "TableConfig") -> "TableConfig":
        for foreign_key in self.foreign_keys:
            ref = foreign_key.references
            assert isinstance(ref, ForeignKeyConfig.References)

            found = False

            assert isinstance(ref.table, str)
            search_table_name = ref.table
            for ref_config in ref_configs:
                if ref_config.name == search_table_name:
                    foreign_key.references = ForeignKeyConfig.References(
                        table=ref_config, column=ref.column
                    )
                    found = True
                    break

            if not found:
                raise ValueError(
                    f"foreign key references unknown table: {search_table_name}"
                )

        return self

    def columns_df(self) -> pl.DataFrame:
        result = pl.concat(
            [col.df() for col in self.column_definitions.column_definitions]
        )

        return result

    def table_names(self) -> List[str]:
        result = [self.name]

        return result

    @classmethod
    def from_dataframe(
        cls,
        df: pl.DataFrame,
        table_schema: str,
        table_name: str,
        primary_keys: List[str],
        default_categorical_length: int,
    ) -> "TableConfig":
        columns = [
            ColumnConfig(name=col_name, data_type=SQLType.from_polars(col_type))
            for col_name, col_type in zip(df.columns, df.dtypes)
        ]

        result = TableConfig(
            name=table_name,
            schema=table_schema,
            primary_keys=primary_keys,
            columns=df.columns,
            column_definitions=ColumnDefinitions(column_definitions=columns),
        )

        return result

    def to_df(self) -> pl.DataFrame:
        schema = {
            col.name: PolarsType.from_sql(col.data_type)
            for col in sorted(
                self.column_definitions.column_definitions, key=lambda k: k.name
            )
        }

        return pl.DataFrame(schema=schema)

    def build_sqlalchemy_columns(self, is_delta_table: bool) -> List[Column]:
        columns: List[Column] = []

        for col_name in self.columns:
            col_cfg = self.column_definitions[col_name]
            default_value = (
                str(col_cfg.default_value)
                if col_cfg.default_value is not None
                else None
            )
            autoincrement_spec = (
                [Identity(start=1, increment=1)] if col_cfg.autoincrement else []
            )

            col: Column = Column(
                col_cfg.name,
                SQLAlchemyType.from_sql(col_cfg.data_type),
                *autoincrement_spec,
                autoincrement=col_cfg.autoincrement,
                primary_key=col_cfg.name in self.primary_keys,
                nullable=col_cfg.nullable
                or (is_delta_table and col_cfg.deduce_foreign_key),
                server_default=default_value,
            )

            columns.append(col)

        return columns

    def dtypes(self) -> Mapping[str, pl.DataType]:
        result = {
            row["name"]: PolarsType.from_sql(row["data_type"])
            for row in self.columns_df().iter_rows(named=True)
        }

        return result


@dataclass
class ForeignKeyConfig:
    @dataclass
    class References:
        table: TableConfig
        column: str

    name: str
    references: References

    def __post_init__(self):
        self.references = ForeignKeyConfig.References(**self.references)


@dataclass
class TableConfigs:
    table_configs: List[TableConfig]
    column_definitions: ColumnDefinitions

    def __post_init__(self):
        self.table_configs = [
            TableConfig(column_definitions=self.column_definitions, **tc_dict)
            for tc_dict in self.table_configs
        ]
        for tc in self.table_configs:
            tc._resolve_foreign_keys(*self.table_configs)

    def __getitem__(self, name: str) -> TableConfig:
        tc = next((tc for tc in self.table_configs if tc.name == name), None)
        if tc:
            return tc

        raise ValueError(f"TableConfig {name} not found")

    def names(self) -> List[str]:
        return [tc.name for tc in self.table_configs]

    def schemas(self) -> List[str]:
        schemas = {tc.schema for tc in self.table_configs}
        return sorted(schemas)

    @classmethod
    def from_yamls(cls, *file_path: str):
        all_tcs = []
        all_col_defs = []
        for yf in file_path:
            with open(yf, "r") as fp:
                cfg_i = yaml.safe_load(fp)
                all_tcs.extend(cfg_i["table_configs"])
                all_col_defs.extend(cfg_i["column_definitions"])

        result = TableConfigs(
            table_configs=all_tcs,
            column_definitions=ColumnDefinitions(column_definitions=all_col_defs),
        )
        return result
