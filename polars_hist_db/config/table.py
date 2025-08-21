from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Mapping, Optional

import polars as pl
from sqlalchemy import Column, Identity
import yaml

from ..types import PolarsType, SQLType, SQLAlchemyType


@dataclass
class TableColumnConfig:
    table: str
    name: str
    data_type: str
    default_value: Optional[str] = None
    autoincrement: bool = False
    nullable: bool = True
    unique_constraint: List[str] = field(default_factory=list)

    def __post_init__(self):
        if self.unique_constraint is None:
            self.unique_constraint = []

    @classmethod
    def from_dataframe(
        cls, df: pl.DataFrame, table_name_override: Optional[str] = None
    ) -> List["TableColumnConfig"]:
        schema = TableColumnConfig.df_schema()
        df = df.select([c for c in schema.keys() if c in df.columns])

        result = []
        for row in df.iter_rows(named=True):
            row_dict = {c: row[c] for c in schema.keys() if c in row}
            if table_name_override is not None:
                row_dict["table"] = table_name_override
            cc = TableColumnConfig(**row_dict)
            result.append(cc)

        return result

    @classmethod
    def df_schema(cls) -> pl.Schema:
        schema: Dict[str, pl.DataTypeClass] = {
            "table": pl.Utf8,
            "name": pl.Utf8,
            "data_type": pl.Utf8,
            "default_value": pl.Utf8,
            "autoincrement": pl.Boolean,
            "nullable": pl.Boolean,
            "unique_constraint": pl.List(pl.Utf8),
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
        return f"TableColumnConfig({', '.join(f'{k}={v!r}' for k, v in self.__dict__.items())})"


@dataclass
class TableConfig:
    name: str
    schema: str
    columns: List[TableColumnConfig]
    forbid_drop_table: bool = False
    foreign_keys: Iterable["ForeignKeyConfig"] = field(default_factory=tuple)
    is_temporal: bool = False
    primary_keys: Iterable[str] = field(default_factory=tuple)

    def __post_init__(self):
        columns = []
        for col in self.columns:
            if not isinstance(col, TableColumnConfig):
                col = TableColumnConfig(**col, table=self.name)

            columns.append(col)

        self.columns = columns

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
            assert isinstance(ref.schema, str)
            search_table_schema = ref.schema
            search_table_name = ref.table
            for ref_config in ref_configs:
                if (
                    ref_config.name == search_table_name
                    and ref_config.schema == search_table_schema
                ):
                    foreign_key.references = ForeignKeyConfig.References(
                        schema=ref_config.schema, table=ref_config, column=ref.column
                    )
                    found = True
                    break

            if not found:
                raise ValueError(
                    f"foreign key references unknown table: {search_table_name}"
                )

        return self

    def columns_df(self) -> pl.DataFrame:
        result = pl.concat([col.df() for col in self.columns]).with_columns(
            table=pl.lit(self.name)
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
            TableColumnConfig(
                name=col_name, data_type=SQLType.from_polars(col_type), table=table_name
            )
            for col_name, col_type in zip(df.columns, df.dtypes)
        ]

        result = TableConfig(
            name=table_name,
            schema=table_schema,
            primary_keys=primary_keys,
            columns=columns,
        )

        return result

    def to_df(self) -> pl.DataFrame:
        schema = {
            col.name: PolarsType.from_sql(col.data_type)
            for col in sorted(self.columns, key=lambda k: k.name)
        }

        return pl.DataFrame(schema=schema)

    def build_sqlalchemy_columns(self, is_delta_table: bool) -> List[Column]:
        columns: List[Column] = []

        for col_cfg in self.columns:
            try:
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
                    nullable=col_cfg.nullable or is_delta_table,
                    server_default=default_value,
                )

                columns.append(col)
            except Exception as e:
                raise ValueError(
                    f"Error building column {col_cfg.table}.{col_cfg.name} : {col_cfg.data_type}",
                    e,
                )

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
        schema: str
        table: TableConfig
        column: str

    name: str
    references: References

    def __post_init__(self):
        self.references = ForeignKeyConfig.References(**self.references)


@dataclass
class TableConfigs:
    items: List[TableConfig]

    def __post_init__(self):
        self.items = [TableConfig(**tc_dict) for tc_dict in self.items]
        for tc in self.items:
            tc._resolve_foreign_keys(*self.items)

    def __getitem__(self, name: str) -> TableConfig:
        tc = next((tc for tc in self.items if tc.name == name), None)
        if tc:
            return tc

        raise ValueError(f"TableConfig {name} not found")

    def names(self) -> List[str]:
        return [tc.name for tc in self.items]

    def schemas(self) -> List[str]:
        schemas = {tc.schema for tc in self.items}
        return sorted(schemas)

    @classmethod
    def from_yamls(cls, *file_path: str):
        all_tcs = []
        for yf in file_path:
            with open(yf, "r") as fp:
                cfg_i = yaml.safe_load(fp)
                all_tcs.extend(cfg_i["table_configs"])

        result = TableConfigs(items=all_tcs)
        return result
