from __future__ import annotations

import re
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Protocol

from sqlalchemy import text

if TYPE_CHECKING:
    from ..config import TableConfig
    from .config import DbEngineConfig


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class TableHealthResult:
    ready: bool
    minimum_rows: int
    sampled_rows: int

    @property
    def detail(self) -> str:
        if self.minimum_rows == 0:
            return "readable"
        rows = "row" if self.minimum_rows == 1 else "rows"
        if self.ready:
            return f"at least {self.minimum_rows} {rows}"
        return f"need ≥{self.minimum_rows} {rows}, found {self.sampled_rows}"


def bounded_table_health_query(
    table_schema: str,
    table_name: str,
    minimum_rows: int,
    *,
    quote: str,
) -> str:
    if (
        not isinstance(minimum_rows, int)
        or isinstance(minimum_rows, bool)
        or minimum_rows < 0
    ):
        raise ValueError("minimum_rows must be a non-negative integer")
    for identifier in (table_schema, table_name):
        if not _IDENTIFIER_RE.fullmatch(identifier):
            raise ValueError(f"Invalid SQL identifier {identifier!r}")
    qualified_table = f"{quote}{table_schema}{quote}.{quote}{table_name}{quote}"
    return f"SELECT 1 FROM {qualified_table} LIMIT {minimum_rows}"


def execute_table_health_query(
    connection: Any,
    query: str,
    minimum_rows: int,
) -> TableHealthResult:
    sampled_rows = len(connection.execute(text(query)).fetchall())
    return TableHealthResult(
        ready=minimum_rows == 0 or sampled_rows >= minimum_rows,
        minimum_rows=minimum_rows,
        sampled_rows=sampled_rows,
    )


class HistoricalDbBackend(Protocol):
    name: str

    def create_engine(self, config: DbEngineConfig) -> Any: ...

    def connection_scope(self, engine: Any) -> AbstractContextManager[Any]:
        """Open a backend-correct connection and commit successful work."""
        ...

    def ingest_transaction(
        self, connection: Any, system_time: datetime | None
    ) -> AbstractContextManager[Any]: ...

    def open_ingest_connection(self, config: DbEngineConfig) -> Any | None:
        """Open an optional backend-specific bulk-ingest connection."""
        ...

    def close_ingest_connection(self, connection: Any | None) -> None: ...

    def staging(
        self,
        connection: Any,
        *,
        ingest_connection: Any | None = None,
    ) -> Any | None: ...

    def dataframes(self, connection: Any) -> Any: ...

    def table_configs(self, connection: Any) -> Any: ...

    def tables(self, table_schema: str, table_name: str, connection: Any) -> Any: ...

    def table_health_query(
        self, table_schema: str, table_name: str, minimum_rows: int = 0
    ) -> str: ...

    def check_table_resource(
        self,
        connection: Any,
        table_schema: str,
        table_name: str,
        minimum_rows: int = 0,
    ) -> TableHealthResult: ...

    def finalize_ingest_run(
        self, connection: Any, delta_table_config: TableConfig
    ) -> None:
        """Ensure the delta/stage table carries no rows once ingest returns."""
        ...
