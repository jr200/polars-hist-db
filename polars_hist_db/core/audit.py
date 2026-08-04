import hashlib
import logging
from datetime import UTC, datetime
from typing import Any, Literal

import polars as pl
from sqlalchemy import Connection, Index, Table, and_, delete, func, select

from ..config.input.types import InputDataSourceType
from ..config.table import TableColumnConfig, TableConfig
from ..utils.db_utils import smallest_datetime
from .dataframe import DataframeOps
from .db import DbOps
from .table import TableOps
from .table_config import TableConfigOps

LOGGER = logging.getLogger(__name__)


def _is_xtdb_connection(connection: Any) -> bool:
    return getattr(getattr(connection, "dialect", None), "name", None) == "postgresql"


def _xtdb_table_config_ops(connection: Any) -> Any:
    from ..backends.xtdb_schema import XtdbTableConfigOps

    return XtdbTableConfigOps(connection)


def _xtdb_dataframe_ops(connection: Any) -> Any:
    from ..backends.xtdb_dataframe import XtdbDataframeOps

    return XtdbDataframeOps(connection)


def _sql_literal(value: object) -> str:
    if isinstance(value, datetime):
        return f"TIMESTAMP '{value.isoformat()}'"
    return "'" + str(value).replace("'", "''") + "'"


class AuditOps:
    def __init__(self, schema: str):
        self.schema = schema
        self._table_name = "__audit_log"

    def fqtn(self) -> str:
        return f"{self.schema}.{self._table_name}"

    def drop(self, connection: Connection):
        TableConfigOps(connection).drop(self._table_config())

    def _table_config(self, *, xtdb: bool = False) -> TableConfig:
        columns = [
            TableColumnConfig(
                name="audit_id",
                data_type="VARCHAR(LENGTH=64)" if xtdb else "INT",
                autoincrement=not xtdb,
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

    def _table_sql(self) -> str:
        return f"{self.schema}.{self._table_name}"

    def _xtdb_data_table_exists(self, connection: Connection) -> bool:
        return _xtdb_table_config_ops(connection).table_exists(
            self.schema,
            self._table_name,
        )

    def _empty_xtdb_audit_df(self) -> pl.DataFrame:
        return pl.DataFrame(
            schema={
                "audit_id": pl.String,
                "table_name": pl.Utf8,
                "data_source_type": pl.Utf8,
                "data_source": pl.Utf8,
                "data_source_ts": pl.Datetime("us", "UTC"),
                "upload_ts": pl.Datetime("us", "UTC"),
            }
        )

    def create(self, connection: Connection) -> Table | TableConfig:
        if _is_xtdb_connection(connection):
            table_config = self._table_config(xtdb=True)
            table_config_ops = _xtdb_table_config_ops(connection)
            if not table_config_ops.table_exists(self.schema, self._table_name):
                table_config_ops.create(table_config)
            return table_config

        audit_table_name = str(self._table_name)
        tbo = TableOps(self.schema, audit_table_name, connection)

        if tbo.table_exists():
            tbl = tbo.get_table_metadata()
            return tbl

        table_config = self._table_config()
        tbl = TableConfigOps(connection).create(table_config)
        assert tbl is not None
        Index(
            f"ix_{self._table_name}_table_ts",
            tbl.c["table_name"],
            tbl.c["data_source_ts"],
        ).create(connection)
        return tbl

    def reset_dataset(self, target_table_name: str, connection: Connection) -> None:
        """Wipe a polars_hist_db-managed dataset: erase the target table
        AND the paired audit-log entries in one transaction.

        Raw `ERASE FROM <table>` on the data table alone leaves the
        `__audit_log` high-water mark behind, which then silently drops
        every subsequent ingest as historic (see JRL-271 / JRL-272). This
        is the ONLY sanctioned way to wipe such a dataset.
        """
        target_fqtn = f"{self.schema}.{target_table_name}"
        if _is_xtdb_connection(connection):
            from ..backends.xtdb_transport import _execute_xtdb_transaction

            self.create(connection)
            _execute_xtdb_transaction(
                connection,
                [
                    f"ERASE FROM {target_fqtn} WHERE TRUE",
                    (
                        f"ERASE FROM {self._table_sql()} "
                        f"WHERE table_name = {_sql_literal(target_table_name)}"
                    ),
                ],
            )
            LOGGER.info("reset dataset %s (data + audit-log erased)", target_fqtn)
            return

        target_tbo = TableOps(self.schema, target_table_name, connection)
        target_tbl = target_tbo.get_table_metadata()
        audit_tbl = TableOps(
            self.schema, self._table_name, connection
        ).get_table_metadata()

        DbOps(connection).execute_sqlalchemy(
            "sql.audit.reset_dataset.data", delete(target_tbl)
        )
        # MariaDB / MySQL system-versioned tables keep history rows after
        # a normal DELETE; DELETE HISTORY drops them too. Skipped on
        # non-temporal tables and non-MariaDB dialects.
        dialect = getattr(connection.dialect, "name", "")
        if dialect in ("mariadb", "mysql") and target_tbo.is_temporal_table():
            from sqlalchemy import text

            connection.execute(text(f"DELETE HISTORY FROM {target_fqtn}"))
        DbOps(connection).execute_sqlalchemy(
            "sql.audit.reset_dataset.audit",
            delete(audit_tbl).where(audit_tbl.c["table_name"] == target_table_name),
        )
        LOGGER.info("reset dataset %s (data + audit-log deleted)", target_fqtn)

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
        if _is_xtdb_connection(connection):
            self.create(connection)
            operator = ">" if interval == "open" else ">="
            query = (
                f"DELETE FROM {self._table_sql()} "
                f"WHERE table_name = {_sql_literal(target_table_name)} "
                f"AND data_source_ts {operator} {_sql_literal(timestamp)}"
            )
            from ..backends.xtdb_transport import _execute_xtdb_dml

            _execute_xtdb_dml(connection, query)
            return 0

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
        if _is_xtdb_connection(connection):
            self.create(connection)
            if not self._xtdb_data_table_exists(connection):
                return

            latest_log = _xtdb_dataframe_ops(connection).from_raw_sql(
                "SELECT MAX(data_source_ts) AS data_source_ts "
                f"FROM {self._table_sql()} "
                f"WHERE table_name = {_sql_literal(target_table_name)} ",
                schema_overrides={"data_source_ts": pl.Datetime("us", "UTC")},
            )
            if latest_log.is_empty() or latest_log[0, "data_source_ts"] is None:
                return

            xtdb_latest_log_ts: datetime = latest_log[0, "data_source_ts"]
            invalid_data_source_items = new_data_source_items.filter(
                pl.col("__created_at")
                <= pl.lit(xtdb_latest_log_ts).cast(pl.dtype_of(pl.col("__created_at")))
            )

            if not invalid_data_source_items.is_empty():
                LOGGER.error(
                    "latest data_source_ts: %s",
                    xtdb_latest_log_ts.isoformat(),
                )
                LOGGER.error(invalid_data_source_items)
                raise ValueError(
                    "uploading from data_sources with earlier timestamps is not supported"
                )
            return

        self.create(connection)
        tbo = TableOps(self.schema, self._table_name, connection)
        tbl = tbo.get_table_metadata()
        latest_log_sql = select(
            func.max(tbl.c["data_source_ts"]).label("data_source_ts")
        ).where(tbl.c["table_name"] == target_table_name)

        latest_log = DataframeOps(connection).from_selectable(latest_log_sql)
        if latest_log.is_empty() or latest_log[0, "data_source_ts"] is None:
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
        if _is_xtdb_connection(connection):
            self.create(connection)
            if not self._xtdb_data_table_exists(connection):
                return data_source_items

            dataframe_ops = _xtdb_dataframe_ops(connection)
            latest_log = dataframe_ops.from_raw_sql(
                "SELECT MAX(data_source_ts) AS data_source_ts "
                f"FROM {self._table_sql()} "
                f"WHERE table_name = {_sql_literal(target_table_name)}",
                schema_overrides={"data_source_ts": pl.Datetime("us", "UTC")},
            )
            if latest_log.is_empty() or latest_log[0, "data_source_ts"] is None:
                return data_source_items

            candidates = data_source_items.select(
                pl.col(data_source_col_name).cast(pl.Utf8).alias("data_source")
            ).unique(maintain_order=True)
            existing_tbl_entries = dataframe_ops.table_query(
                self.schema,
                self._table_name,
                candidates,
                ["data_source"],
                table_config=self._table_config(xtdb=True),
            ).with_columns(pl.col("data_source").cast(pl.Utf8))
            data_source_items = self._filter_unprocessed_items(
                data_source_items, existing_tbl_entries, data_source_col_name
            )
            latest_log_ts: datetime = latest_log[0, "data_source_ts"]
            return data_source_items.filter(
                pl.col(data_source_ts_col_name)
                >= pl.lit(latest_log_ts).cast(
                    pl.dtype_of(pl.col(data_source_ts_col_name))
                )
            )

        audit_tbl = self.create(connection)
        assert isinstance(audit_tbl, Table)

        # get the set of datasource items already processed
        target_table_logs_sql = (
            select(
                audit_tbl.c["data_source"],
                func.max(audit_tbl.c["data_source_ts"]).label("data_source_ts"),
            )
            .where(audit_tbl.c["table_name"] == target_table_name)
            .group_by(audit_tbl.c["data_source"])
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
            raise ValueError(
                "Developer Error: data_source_timestamp must be timezone aware"
            )

        new_item = {
            "table_name": f"{target_table_name}",
            "data_source_type": data_source_type,
            "data_source": data_source,
            "data_source_ts": data_source_timestamp,
            "upload_ts": datetime.now(UTC),
        }

        if _is_xtdb_connection(connection):
            table_config = self.create(connection)
            assert isinstance(table_config, TableConfig)
            audit_id_value = f"{self.schema}|{target_table_name}|{data_source_type}|{data_source}|{data_source_timestamp.isoformat()}"
            new_item["audit_id"] = hashlib.sha256(audit_id_value.encode()).hexdigest()
            df = pl.DataFrame([new_item])
            num_rows_changed = _xtdb_dataframe_ops(connection).table_insert(
                df,
                self.schema,
                self._table_name,
                table_config=table_config,
            )
            return num_rows_changed == 1

        self.create(connection)
        tbo = TableOps(self.schema, self._table_name, connection)
        tbl = tbo.get_table_metadata()
        insert_stmt = tbl.insert().values(new_item)
        result = DbOps(connection).execute_sqlalchemy("sql.audit.insert", insert_stmt)

        did_insert = result.rowcount == 1
        return did_insert

    def get_latest_entry(
        self,
        connection: Connection,
        asof_timestamp: datetime | None = None,
        target_table_name: str | None = None,
        data_source_type: InputDataSourceType | None = None,
    ) -> pl.DataFrame:
        if _is_xtdb_connection(connection):
            self.create(connection)
            if not self._xtdb_data_table_exists(connection):
                return self._empty_xtdb_audit_df()

            xtdb_filters: list[str] = []
            if target_table_name is not None:
                xtdb_filters.append(f"table_name = {_sql_literal(target_table_name)}")
            if data_source_type is not None:
                xtdb_filters.append(
                    f"data_source_type = {_sql_literal(data_source_type)}"
                )
            if asof_timestamp is not None:
                if asof_timestamp.tzinfo is None:
                    raise ValueError("asof_timestamp must be timezone aware")
                xtdb_filters.append(f"data_source_ts <= {_sql_literal(asof_timestamp)}")

            where_clause = f"WHERE {' AND '.join(xtdb_filters)}" if xtdb_filters else ""
            latest_log = _xtdb_dataframe_ops(connection).from_raw_sql(
                "SELECT audit_id, table_name, data_source_type, data_source, "
                "data_source_ts, upload_ts FROM ("
                "SELECT *, ROW_NUMBER() OVER ("
                "PARTITION BY table_name ORDER BY data_source_ts DESC"
                ") AS _rn "
                f"FROM {self._table_sql()} {where_clause}"
                ") AS ranked WHERE _rn = 1",
                schema_overrides={
                    "audit_id": pl.String,
                    "data_source_ts": pl.Datetime("us", "UTC"),
                    "upload_ts": pl.Datetime("us", "UTC"),
                },
            )
            return latest_log.with_columns(
                pl.col("data_source_ts").cast(pl.Datetime("us", "UTC")),
                pl.col("upload_ts").cast(pl.Datetime("us", "UTC")),
            )

        tbl = self.create(connection)
        assert isinstance(tbl, Table)

        filters = []
        if target_table_name is not None:
            filters.append(tbl.c["table_name"] == target_table_name)
        if data_source_type is not None:
            filters.append(tbl.c["data_source_type"] == data_source_type)
        if asof_timestamp is not None:
            if asof_timestamp.tzinfo is None:
                raise ValueError("asof_timestamp must be timezone aware")
            filters.append(tbl.c["data_source_ts"] <= asof_timestamp)

        ranked = (
            select(tbl)
            .add_columns(
                func.row_number()
                .over(
                    partition_by=tbl.c["table_name"],
                    order_by=tbl.c["data_source_ts"].desc(),
                )
                .label("_rn")
            )
            .where(and_(True, *filters))
            .subquery()
        )
        latest_log_sql = select(
            *(ranked.c[column.name] for column in tbl.columns)
        ).where(ranked.c["_rn"] == 1)

        latest_log = (
            DataframeOps(connection)
            .from_selectable(latest_log_sql)
            .with_columns(
                pl.col("data_source_ts").cast(pl.Datetime("us", "UTC")),
                pl.col("upload_ts").cast(pl.Datetime("us", "UTC")),
            )
        )

        return latest_log
