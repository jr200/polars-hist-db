import logging
from types import MappingProxyType
from typing import Mapping, Optional, Sequence
import warnings
from sqlalchemy import (
    Column,
    ColumnCollection,
    Connection,
    func,
    inspect,
    MetaData,
    select,
    Table,
    text,
)

from sqlalchemy.exc import SAWarning

from .db import DbOps


LOGGER = logging.getLogger(__name__)


class TableOps:
    def __init__(self, table_schema: str, table_name: str, connection: Connection):
        self.connection = connection
        self.table_schema = table_schema
        self.table_name = table_name

    def enable_system_versioning(
        self,
        partition_interval: str = "1 YEAR",
    ):
        sql = f"""
        ALTER TABLE {self.table_schema}.{self.table_name}
            ADD COLUMN __valid_from TIMESTAMP(6) GENERATED ALWAYS AS ROW START INVISIBLE,
            ADD COLUMN __valid_to TIMESTAMP(6) GENERATED ALWAYS AS ROW END INVISIBLE,
            ADD PERIOD FOR SYSTEM_TIME(__valid_from, __valid_to),
            ADD SYSTEM VERSIONING
            ;
        """
        # disable partitioning if table has foreign keys: https://jira.mariadb.org/browse/MDEV-12483
        # -- PARTITION BY SYSTEM_TIME INTERVAL {partition_interval} AUTO

        result = DbOps(self.connection).execute_sqlalchemy(
            "sql.op.enable_system_versioning", text(sql)
        )
        LOGGER.debug("enabled system versioning %s", result)

    def get_table_metadata(
        self,
        autoload_metadata: bool = True,
    ) -> Table:
        metadata = MetaData(schema=self.table_schema)
        if not autoload_metadata:
            return Table(self.table_name, metadata)

        with warnings.catch_warnings():
            # skip this annoying SQLAlchemy warning:
            # SAWarning: Unknown schema content: '  PERIOD FOR SYSTEM_TIME (`__valid_from`, `__valid_to`),'
            warning_regex = r"Unknown schema content:.*PERIOD FOR SYSTEM_TIME [()].*"
            warnings.filterwarnings("ignore", message=warning_regex, category=SAWarning)

            tbl = Table(self.table_name, metadata, autoload_with=self.connection)

        return tbl

    def row_count(self) -> int:
        tbl = self.get_table_metadata()
        count_sql = select(func.count().label("nrow")).select_from(tbl)
        row_count = DbOps(self.connection).execute_sqlalchemy(
            "sql.op.row_count", count_sql
        )
        assert isinstance(row_count, int), (
            f"expected row count to be int, got {type(row_count)}"
        )
        return row_count

    def get_column_intersection(
        self,
        column_selection: Optional[Sequence[str]],
    ) -> ColumnCollection:
        tbl = self.get_table_metadata()

        result: ColumnCollection = ColumnCollection(
            [
                (c.name, Column(c.name, c.type))
                for c in tbl.columns
                if column_selection is None or c.name in column_selection
            ]
        )

        return result

    def has_all_columns(
        self,
        search_col_names: Sequence[str],
    ) -> bool:
        tbl = self.get_table_metadata()
        for col_name in search_col_names:
            if col_name not in tbl.columns:
                return False

        return True

    def get_primary_keys(
        self,
        tbl: Table,
        remap: Mapping[str, str] = MappingProxyType({}),
        include_temporal: bool = False,
    ) -> Sequence[Column]:
        exclude_cols: list[str] = []

        if not include_temporal:
            exclude_cols.extend(self.system_versioning_columns())

        primary_keys = [
            c
            for c in tbl.primary_key.columns
            if remap.get(c.name, c.name) not in exclude_cols
        ]

        return primary_keys

    def is_temporal_table(
        self,
    ) -> bool:
        return self.has_all_columns(self.system_versioning_columns())

    @staticmethod
    def system_versioning_columns() -> Sequence[str]:
        return ["__valid_from", "__valid_to"]

    @staticmethod
    def finality_column() -> Sequence[str]:
        return ["__finality"]

    def table_exists(self) -> bool:
        inspector = inspect(self.connection, raiseerr=True)
        result = inspector.has_table(self.table_name, schema=self.table_schema)

        return result
