from datetime import datetime, timezone
import logging
from typing import Literal, Optional

import polars as pl
from sqlalchemy import and_, Connection, delete, select, Table

from ..config.input.types import InputDataSourceType
from ..config.table import TableColumnConfig, TableConfig
from .dataframe import DataframeOps
from .db import DbOps
from .table import TableOps
from .table_config import TableConfigOps

from ..utils.db_utils import smallest_datetime

LOGGER = logging.getLogger(__name__)


class AuditOps:
    def __init__(self, schema: str):
        self.schema = schema
        self._table_name = "__audit_log"

    def fqtn(self) -> str:
        return f"{self.schema}.{self._table_name}"

    def drop(self, connection: Connection):
        TableConfigOps(connection).drop(self._table_config())

    def _table_config(self) -> TableConfig:
        columns = [
            TableColumnConfig(
                name="audit_id",
                data_type="INT",
                autoincrement=True,
                table=self._table_name,
            ),
            TableColumnConfig(
                name="table_name",
                data_type="VARCHAR(LENGTH=64)",
                nullable=False,
                unique_constraint=["unique_audit_item"],
                table=self._table_name,
            ),
            TableColumnConfig(
                name="data_source_type",
                data_type="VARCHAR(LENGTH=32)",
                nullable=False,
                unique_constraint=["unique_audit_item"],
                table=self._table_name,
            ),
            TableColumnConfig(
                name="data_source",
                data_type="VARCHAR(LENGTH=1023)",
                nullable=False,
                unique_constraint=["unique_audit_item"],
                table=self._table_name,
            ),
            TableColumnConfig(
                name="data_source_ts",
                data_type="DATETIME",
                nullable=False,
                unique_constraint=["unique_audit_item"],
                table=self._table_name,
            ),
            TableColumnConfig(
                name="upload_ts",
                data_type="DATETIME",
                nullable=False,
                table=self._table_name,
            ),
        ]

        table_config = TableConfig(
            name=str(self._table_name),
            schema=self.schema,
            primary_keys=["audit_id"],
            columns=columns,
        )
        return table_config

    def create(self, connection: Connection) -> Table:
        audit_table_name = str(self._table_name)
        tbo = TableOps(self.schema, audit_table_name, connection)

        if tbo.table_exists():
            tbl = tbo.get_table_metadata()
            return tbl

        table_config = self._table_config()
        tbl = TableConfigOps(connection).create(table_config)
        assert tbl is not None
        return tbl

    def purge(self, target_table_name: str, connection: Connection) -> int:
        LOGGER.debug("clearing audit for %s.%s", self.schema, target_table_name)
        return self.purge_after_timestamp(
            target_table_name, smallest_datetime(), "closed", connection
        )

    def purge_after_timestamp(
        self,
        target_table_name: str,
        timestamp: datetime,
        interval: Literal["open", "closed"],
        connection: Connection,
    ) -> int:
        tbo = TableOps(self.schema, self._table_name, connection)
        tbl = tbo.get_table_metadata()
        if interval == "open":
            filter_clause = tbl.c["data_source_ts"] > timestamp
        else:
            filter_clause = tbl.c["data_source_ts"] >= timestamp

        purge_sql = delete(tbl).where(
            and_(tbl.c["table_name"] == target_table_name, filter_clause)
        )

        result = DbOps(connection).execute_sqlalchemy("sql.audit.purge", purge_sql)
        LOGGER.info(
            "purged %d audit entries for %s.%s since %s",
            result.rowcount,
            self.schema,
            target_table_name,
            timestamp.isoformat(),
        )

        return result.rowcount

    def prevalidate_new_items(
        self,
        target_table_name: str,
        new_data_source_items: pl.DataFrame,
        connection: Connection,
    ):
        self.create(connection)
        tbo = TableOps(self.schema, self._table_name, connection)
        tbl = tbo.get_table_metadata()
        latest_log_sql = (
            select(tbl)
            .where(tbl.c["table_name"] == target_table_name)
            .order_by("data_source_ts")
            .limit(1)
        )

        latest_log = DataframeOps(connection).from_selectable(latest_log_sql)
        if latest_log.is_empty():
            return

        latest_log_ts: datetime = latest_log[0, "data_source_ts"]
        invalid_data_source_items = new_data_source_items.filter(
            pl.col("__created_at")
            <= pl.lit(latest_log_ts).cast(pl.dtype_of(pl.col("__created_at")))
        )

        if not invalid_data_source_items.is_empty():
            LOGGER.error("latest data_source_ts: %s", latest_log_ts.isoformat())
            LOGGER.error(invalid_data_source_items)
            raise ValueError(
                "uploading from data_sources with earlier timestamps is not supported"
            )

    def filter_items(
        self,
        data_source_items: pl.DataFrame,
        data_source_col_name: str,
        data_source_ts_col_name: str,
        target_table_name: str,
        connection: Connection,
    ) -> pl.DataFrame:
        audit_tbl = self.create(connection)

        # get the set of datasource items already processed
        target_table_logs_sql = (
            select(audit_tbl.c["data_source"], audit_tbl.c["data_source_ts"])
            .where(audit_tbl.c["table_name"] == target_table_name)
            .order_by(audit_tbl.c["data_source_ts"])
        )

        existing_tbl_entries = (
            DataframeOps(connection)
            .from_selectable(
                target_table_logs_sql,
                schema_overrides={"data_source_ts": pl.Datetime("us", "UTC")},
            )
            .with_columns(pl.col("data_source").cast(pl.Utf8))
        )

        if existing_tbl_entries.is_empty():
            return data_source_items

        data_source_items = self._filter_unprocessed_items(
            data_source_items, existing_tbl_entries, data_source_col_name
        )
        data_source_items = self._filter_historic_items(
            data_source_items, existing_tbl_entries, data_source_ts_col_name
        )

        return data_source_items

    def _filter_historic_items(
        self,
        candidate_items: pl.DataFrame,
        existing_entries: pl.DataFrame,
        data_source_ts_col_name: str,
    ) -> pl.DataFrame:
        most_recently_processed_ts = existing_entries["data_source_ts"].max()
        new_items_only = candidate_items.filter(
            pl.col(data_source_ts_col_name) >= most_recently_processed_ts
        )
        return new_items_only

    def _filter_unprocessed_items(
        self,
        candidate_items: pl.DataFrame,
        existing_entries: pl.DataFrame,
        data_source_col_name: str,
    ) -> pl.DataFrame:
        existing_datasources = (
            existing_entries.get_column("data_source")
            .unique(maintain_order=True)
            .to_list()
        )

        # remove items that are already processed (in the audit table)
        already_processed = pl.col(data_source_col_name).is_in(existing_datasources)
        unprocessed_items = candidate_items.filter(already_processed.not_()).unique(
            maintain_order=True
        )

        LOGGER.debug(
            "found %d (of %d) unprocessed data source items",
            len(unprocessed_items),
            len(candidate_items),
        )

        return unprocessed_items

    def add_entry(
        self,
        data_source_type: InputDataSourceType,
        data_source: str,
        target_table_name: str,
        connection: Connection,
        data_source_timestamp: datetime,
    ) -> bool:
        if data_source_timestamp.tzinfo is None:
            raise Exception(
                "Developer Error: data_source_timestamp must be timezone aware"
            )

        self.create(connection)

        new_item = {
            "table_name": f"{target_table_name}",
            "data_source_type": data_source_type,
            "data_source": data_source,
            "data_source_ts": data_source_timestamp,
            "upload_ts": datetime.now(timezone.utc),
        }

        tbo = TableOps(self.schema, self._table_name, connection)
        tbl = tbo.get_table_metadata()
        insert_stmt = tbl.insert().values(new_item)
        result = DbOps(connection).execute_sqlalchemy("sql.audit.insert", insert_stmt)

        did_insert = result.rowcount == 1
        return did_insert

    def get_latest_entry(
        self,
        connection: Connection,
        asof_timestamp: Optional[datetime] = None,
        target_table_name: Optional[str] = None,
        data_source_type: Optional[InputDataSourceType] = None,
    ) -> pl.DataFrame:
        tbl = self.create(connection)

        filters = []
        if target_table_name is not None:
            filters.append(tbl.c["table_name"] == target_table_name)
        if data_source_type is not None:
            filters.append(tbl.c["data_source_type"] == data_source_type)
        if asof_timestamp is not None:
            if asof_timestamp.tzinfo is None:
                raise ValueError("asof_timestamp must be timezone aware")
            filters.append(tbl.c["data_source_ts"] <= asof_timestamp)

        latest_log_sql = (
            select(tbl).where(and_(*filters)).order_by(tbl.c["data_source_ts"].desc())
        )

        latest_log = (
            DataframeOps(connection)
            .from_selectable(latest_log_sql)
            .group_by("table_name")
            .first()
        )

        return latest_log
