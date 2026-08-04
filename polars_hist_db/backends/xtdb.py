from dataclasses import dataclass
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Any,
)

import polars as pl
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from ..config import (
    DeltaConfig,
    TableConfig,
    ValidTimeConfig,
)
from ..core import TimeHint
from . import xtdb_arrow as _xtdb_arrow
from . import xtdb_dataframe as _xtdb_dataframe
from . import xtdb_delta as _xtdb_delta
from . import xtdb_query as _xtdb_query
from . import xtdb_schema as _xtdb_schema
from . import xtdb_staging as _xtdb_staging
from . import xtdb_transport as _xtdb_transport
from .base import (
    TableHealthResult,
    bounded_table_health_query,
    execute_table_health_query,
)
from .config import DbEngineConfig
from .temporal import system_time_hint_clause
from .xtdb_dataframe import (
    XtdbAdbcDataframeOps,
    XtdbDataframeOps,
)
from .xtdb_delta import _xtdb_temporal_upsert
from .xtdb_schema import XtdbTableConfigOps
from .xtdb_staging import XtdbStagingOps
from .xtdb_transport import (
    _close_xtdb_adbc_connection,
    _create_xtdb_adbc_connection,
    _create_xtdb_engine,
    _xtdb_adbc_uri,
    _xtdb_buffered_transaction_scope,
    _xtdb_connection_scope,
    _xtdb_transaction_active,
)

if TYPE_CHECKING:
    from ..overrides import (
        ArrowOverrideStoreConfig,
        CrdtDocumentStoreConfig,
        DocumentAccessStoreConfig,
        LayerCompositionStoreConfig,
        OverrideLedgerConfig,
    )
    from ..overrides.arrow import RepositoryArrowOverrideOperationStore
    from ..overrides.xtdb import (
        XtdbCrdtDocumentStore,
        XtdbDocumentAccessStore,
        XtdbLayerCompositionStore,
    )


def __getattr__(name: str) -> Any:
    """Keep legacy private XTDB imports working during the module split."""
    try:
        return getattr(_xtdb_dataframe, name)
    except AttributeError:
        try:
            return getattr(_xtdb_arrow, name)
        except AttributeError:
            try:
                return getattr(_xtdb_query, name)
            except AttributeError:
                try:
                    return getattr(_xtdb_delta, name)
                except AttributeError:
                    try:
                        return getattr(_xtdb_staging, name)
                    except AttributeError:
                        try:
                            return getattr(_xtdb_schema, name)
                        except AttributeError:
                            return getattr(_xtdb_transport, name)


def _load_flight_sql() -> Any:
    return _xtdb_transport._load_flight_sql()


@dataclass(frozen=True)
class XtdbBackend:
    name: str = "xtdb"
    max_rows_per_insert: int | None = 10_000

    def create_engine(self, config: DbEngineConfig) -> Engine:
        return _create_xtdb_engine(config, create_engine)

    def connection_scope(self, engine: Engine) -> Any:
        return _xtdb_connection_scope(engine)

    def ingest_transaction(self, connection: Any, system_time: datetime | None) -> Any:
        return _xtdb_buffered_transaction_scope(connection, system_time)

    def adbc_uri(self, config: DbEngineConfig) -> str:
        return _xtdb_adbc_uri(config)

    def create_adbc_connection(self, config: DbEngineConfig) -> Any:
        return _create_xtdb_adbc_connection(config, _load_flight_sql)

    def open_ingest_connection(self, config: DbEngineConfig) -> Any:
        return self.create_adbc_connection(config)

    def close_ingest_connection(self, connection: Any | None) -> None:
        _close_xtdb_adbc_connection(connection)

    def dataframes(self, connection: Any) -> XtdbDataframeOps:
        return XtdbDataframeOps(
            connection,
            max_rows_per_insert=self.max_rows_per_insert,
        )

    def adbc_dataframes(self, connection: Any) -> XtdbAdbcDataframeOps:
        return XtdbAdbcDataframeOps(
            connection,
            max_rows_per_insert=self.max_rows_per_insert,
        )

    def table_configs(self, connection: Any) -> XtdbTableConfigOps:
        return XtdbTableConfigOps(connection)

    def table_health_query(
        self, table_schema: str, table_name: str, minimum_rows: int = 0
    ) -> str:
        return bounded_table_health_query(
            table_schema, table_name, minimum_rows, quote='"'
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

    def crdt_documents(
        self,
        connection: Any,
        document_store: "CrdtDocumentStoreConfig",
        projection: "OverrideLedgerConfig",
    ) -> "XtdbCrdtDocumentStore":
        from ..overrides.xtdb import XtdbCrdtDocumentStore

        return XtdbCrdtDocumentStore(connection, document_store, projection)

    def arrow_overrides(
        self, connection: Any, config: "ArrowOverrideStoreConfig"
    ) -> "RepositoryArrowOverrideOperationStore":
        from ..overrides.arrow import RepositoryArrowOverrideOperationStore
        from ..overrides.arrow_xtdb import XtdbArrowOverrideRepository

        return RepositoryArrowOverrideOperationStore(
            XtdbArrowOverrideRepository(connection, config)
        )

    def document_access(
        self, connection: Any, config: "DocumentAccessStoreConfig"
    ) -> "XtdbDocumentAccessStore":
        from ..overrides.xtdb import XtdbDocumentAccessStore

        return XtdbDocumentAccessStore(connection, config)

    def layer_compositions(
        self, connection: Any, config: "LayerCompositionStoreConfig"
    ) -> "XtdbLayerCompositionStore":
        from ..overrides.xtdb import XtdbLayerCompositionStore

        return XtdbLayerCompositionStore(connection, config)

    def staging(
        self,
        connection: Any,
        *,
        ingest_connection: Any | None = None,
    ) -> XtdbStagingOps:
        return XtdbStagingOps(
            connection,
            max_rows_per_insert=self.max_rows_per_insert,
            adbc_connection=(
                None if _xtdb_transaction_active(connection) else ingest_connection
            ),
        )

    def time_hint_clause(self, time_hint: TimeHint) -> str | None:
        return system_time_hint_clause(time_hint)

    def finalize_ingest_run(
        self, connection: Any, delta_table_config: TableConfig
    ) -> None:
        # Stage rows are already erased per partition by cleanup_run.
        return None

    def temporal_upsert(
        self,
        df: pl.DataFrame,
        table_schema: str,
        table_name: str,
        *,
        connection: Any | None = None,
        dataframe_ops: XtdbDataframeOps | XtdbAdbcDataframeOps | None = None,
        table_config: TableConfig | None = None,
        delta_config: DeltaConfig | None = None,
        update_time: datetime | None = None,
        valid_time: ValidTimeConfig | None = None,
        dropout_close_time: datetime | None = None,
    ) -> int:
        return _xtdb_temporal_upsert(
            df,
            table_schema,
            table_name,
            connection=connection,
            dataframe_ops=dataframe_ops,
            table_config=table_config,
            delta_config=delta_config,
            update_time=update_time,
            valid_time=valid_time,
            dropout_close_time=dropout_close_time,
            max_rows_per_insert=self.max_rows_per_insert,
            dataframe_factory=self.dataframes,
        )
