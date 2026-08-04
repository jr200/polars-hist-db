from __future__ import annotations

from contextlib import nullcontext
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from ..core import DataframeOps, TableConfigOps, TableOps, TimeHint
from .base import (
    TableHealthResult,
    bounded_table_health_query,
    execute_table_health_query,
)
from .config import DbEngineConfig
from .temporal import system_time_hint_clause

if TYPE_CHECKING:
    from ..config import TableConfig
    from ..overrides import (
        ArrowOverrideStoreConfig,
        CrdtDocumentStoreConfig,
        DocumentAccessStoreConfig,
        LayerCompositionStoreConfig,
        OverrideLedgerConfig,
    )
    from ..overrides.arrow import RepositoryArrowOverrideOperationStore
    from ..overrides.sql import (
        MariaDbCrdtDocumentStore,
        MariaDbDocumentAccessStore,
        MariaDbLayerCompositionStore,
    )


@dataclass(frozen=True)
class MariaDbBackend:
    name: str = "mariadb"

    def create_engine(self, config: DbEngineConfig) -> Engine:
        auth = ""
        if config.username:
            auth = config.username
            if config.password:
                auth = f"{auth}:{config.password}"
            auth = f"{auth}@"

        url = f"mariadb+pymysql://{auth}{config.hostname}:{config.port}"
        return create_engine(
            url,
            pool_size=config.pool_size,
            max_overflow=config.max_overflow,
        )

    def connection_scope(self, engine: Engine) -> Any:
        """Open one atomic MariaDB unit of work."""
        return engine.begin()

    def ingest_transaction(self, connection: Any, system_time: datetime | None) -> Any:
        return nullcontext()

    def open_ingest_connection(self, config: DbEngineConfig) -> None:
        return None

    def close_ingest_connection(self, connection: Any | None) -> None:
        return None

    def staging(
        self,
        connection: Any,
        *,
        ingest_connection: Any | None = None,
    ) -> None:
        return None

    def dataframes(self, connection: Any) -> DataframeOps:
        return DataframeOps(connection)

    def table_configs(self, connection: Any) -> TableConfigOps:
        return TableConfigOps(connection)

    def crdt_documents(
        self,
        connection: Any,
        document_store: CrdtDocumentStoreConfig,
        projection: OverrideLedgerConfig,
    ) -> MariaDbCrdtDocumentStore:
        from ..overrides.sql import MariaDbCrdtDocumentStore

        return MariaDbCrdtDocumentStore(connection, document_store, projection)

    def arrow_overrides(
        self, connection: Any, config: ArrowOverrideStoreConfig
    ) -> RepositoryArrowOverrideOperationStore:
        from ..overrides.arrow import RepositoryArrowOverrideOperationStore
        from ..overrides.arrow_sql import MariaDbArrowOverrideRepository

        return RepositoryArrowOverrideOperationStore(
            MariaDbArrowOverrideRepository(connection, config)
        )

    def document_access(
        self, connection: Any, config: DocumentAccessStoreConfig
    ) -> MariaDbDocumentAccessStore:
        from ..overrides.sql import MariaDbDocumentAccessStore

        return MariaDbDocumentAccessStore(connection, config)

    def layer_compositions(
        self, connection: Any, config: LayerCompositionStoreConfig
    ) -> MariaDbLayerCompositionStore:
        from ..overrides.sql import MariaDbLayerCompositionStore

        return MariaDbLayerCompositionStore(connection, config)

    def tables(self, table_schema: str, table_name: str, connection: Any) -> TableOps:
        return TableOps(table_schema, table_name, connection)

    def table_health_query(
        self, table_schema: str, table_name: str, minimum_rows: int = 0
    ) -> str:
        return bounded_table_health_query(
            table_schema, table_name, minimum_rows, quote="`"
        )

    def check_table_resource(
        self,
        connection: Any,
        table_schema: str,
        table_name: str,
        minimum_rows: int = 0,
    ) -> TableHealthResult:
        return execute_table_health_query(
            connection,
            self.table_health_query(table_schema, table_name, minimum_rows),
            minimum_rows,
        )

    def time_hint_clause(self, time_hint: TimeHint) -> str | None:
        return system_time_hint_clause(time_hint)

    def finalize_ingest_run(
        self, connection: Any, delta_table_config: TableConfig
    ) -> None:
        connection.execute(
            text(f"DELETE FROM {delta_table_config.schema}.{delta_table_config.name}")
        )
