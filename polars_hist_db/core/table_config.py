from collections import defaultdict
import logging
from typing import List, Mapping, Optional

from sqlalchemy import (
    Column,
    Connection,
    DefaultClause,
    ForeignKeyConstraint,
    inspect,
    MetaData,
    Table,
    text,
    UniqueConstraint,
)
from sqlalchemy.schema import CreateTable

from .db import DbOps
from .table import TableOps

from ..config.table import TableConfig, TableConfigs, TableColumnConfig
from ..types import SQLAlchemyType
from ..utils.db_utils import strip_outer_quotes


LOGGER = logging.getLogger(__name__)


class TableConfigOps:
    def __init__(self, connection: Connection):
        self.connection = connection

    def create_all(self, tcs: TableConfigs):
        for tc in tcs.items:
            if tc.is_temporal:
                self._create_temporal(tc.name, tc)
            else:
                self._create_nontemporal(tc.name, tc)

    def create(
        self,
        table_config: TableConfig,
        column_selection: Optional[List[str]] = None,
        is_delta_table: bool = False,
        is_temporary_table: bool = False,
    ) -> Table:
        inspector = inspect(self.connection, raiseerr=False)
        table_schema = table_config.schema

        if table_config.is_temporal:
            tbl = self._create_temporal(
                table_config.name, table_config, column_selection
            )
        else:
            tbl = self._create_nontemporal(
                table_config.name,
                table_config,
                column_selection,
                is_delta_table=is_delta_table,
                is_temporary_table=is_temporary_table,
            )

        assert inspector.has_table(table_config.name, schema=table_schema)
        return tbl

    def from_table(self, table_schema: str, table_name: str) -> TableConfig:
        tbl = TableOps(table_schema, table_name, self.connection).get_table_metadata()
        # types = get_sql_types_from_table(tbl)
        col_defs = []
        col_names = []
        fks = []
        pks = []
        for col in tbl.columns:
            assert isinstance(col, Column)
            if col.name in TableOps.system_versioning_columns():
                continue

            col_name = col.name
            col_names.append(col_name)
            if col.primary_key:
                pks.append(col_name)

            for fk in col.foreign_keys:
                ref_column = fk.column
                fk_spec = {
                    "name": col_name,
                    "references": {
                        "schema": fk.column.table.schema,
                        "table": fk.column.table.name,
                        "column": ref_column.name,
                    },
                }

                fks.append(fk_spec)

            sql_type = repr(col.type)
            sa_type = str(SQLAlchemyType.from_sql(sql_type))
            if col.server_default and col.server_default.has_argument:
                assert isinstance(col.server_default, DefaultClause)
                default_value = strip_outer_quotes(str(col.server_default.arg))
            else:
                default_value = None

            col_config = TableColumnConfig(
                table_name,
                col_name,
                data_type=sa_type,
                default_value=default_value,
                nullable=bool(col.nullable),
                autoincrement=col.identity is not None,
            )

            col_defs.append(col_config)

        return TableConfig(
            name=table_name,
            schema=table_schema,
            columns=col_defs,
            forbid_drop_table=False,
            foreign_keys=fks,  # type: ignore[arg-type]
            is_temporal=False,
            primary_keys=pks,
        )

    def drop_all(self, table_configs: TableConfigs):
        for tc in reversed(table_configs.items):
            self.drop(tc)

    def drop(self, table_config: TableConfig):
        if table_config.forbid_drop_table:
            # (protect production tables from misconfiguration)
            raise ValueError("dropping this table from code is forbidden.")

        self._drop(table_config.schema, table_config.name)

    def _drop(self, table_schema: str, table_name: str):
        tbo = TableOps(table_schema, table_name, self.connection)
        if tbo.table_exists():
            tbl = tbo.get_table_metadata()
            tbl.drop(self.connection, checkfirst=True)
            LOGGER.info("dropped table %s", table_name)

    def _create_temporal(
        self,
        table_name: str,
        table_config: TableConfig,
        column_selection: Optional[List[str]] = None,
    ) -> Table:
        tbo = TableOps(table_config.schema, table_name, self.connection)
        if tbo.table_exists():
            return tbo.get_table_metadata()

        self._create_nontemporal(table_name, table_config, column_selection)
        tbo.enable_system_versioning()
        tbl = tbo.get_table_metadata()
        return tbl

    def _create_nontemporal(
        self,
        table_name: str,
        table_config: TableConfig,
        column_selection: Optional[List[str]] = None,
        additional_columns: Optional[List[Column]] = None,
        is_delta_table: bool = False,
        is_temporary_table: bool = False,
    ) -> Table:
        tbo = TableOps(table_config.schema, table_name, self.connection)
        if tbo.table_exists():
            return tbo.get_table_metadata()

        LOGGER.info("creating table %s.%s", tbo.table_schema, tbo.table_name)

        columns = table_config.build_sqlalchemy_columns(is_delta_table)

        if additional_columns is not None:
            columns.extend(additional_columns)

        unique_constraint_items: Mapping[str, List[str]] = defaultdict(list)
        if not is_delta_table:
            for col_def in table_config.columns:
                for uc_name in col_def.unique_constraint:
                    uc_guid = f"{uc_name}_{table_name}"
                    unique_constraint_items[uc_guid].append(col_def.name)

        unique_constraints = [
            UniqueConstraint(*uc_cols, name=uc_guid)
            for uc_guid, uc_cols in unique_constraint_items.items()
        ]

        metadata = MetaData(schema=tbo.table_schema)

        # build foreign key constraints
        foreign_key_constraints = []
        if not is_delta_table:
            for fk in table_config.foreign_keys:
                fk_tbl = Table(
                    fk.references.table.name,
                    metadata,
                    autoload_with=self.connection,
                    schema=fk.references.schema,
                )

                fkc = ForeignKeyConstraint(
                    columns=[f"{fk.name}"],
                    refcolumns=[fk_tbl.c[fk.references.column]],
                )

                foreign_key_constraints.append(fkc)

        prefixes = ["TEMPORARY"] if is_temporary_table else []

        tbl = Table(
            table_name,
            metadata,
            *columns,
            *unique_constraints,
            *foreign_key_constraints,
            schema=tbo.table_schema,
            prefixes=prefixes,
        )

        DbOps(self.connection).db_create(tbo.table_schema)
        create_stmt = text(str(CreateTable(tbl).compile(self.connection)))
        # LOGGER.debug("table_create.sql: %s", str(create_stmt))
        DbOps(self.connection).execute_sqlalchemy(
            f"sql.base.table_create.{table_name}", create_stmt
        )

        LOGGER.debug("created table %s.%s", tbo.table_schema, tbo.table_name)

        tbl = tbo.get_table_metadata()
        return tbl
