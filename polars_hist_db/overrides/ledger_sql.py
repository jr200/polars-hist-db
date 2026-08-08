from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any, cast

import polars as pl
from sqlalchemy import bindparam, text
from sqlalchemy.engine import Connection

from ..backends import get_backend
from ..backends.xtdb_dataframe import XtdbDataframeOps
from ..backends.xtdb_transport import _is_xtdb_table_not_found_error
from ..core import DataframeOps
from .config import build_override_table_config
from .ordering import (
    override_recorded_order_sql,
    override_recorded_time_sql,
    project_personal_override_operations,
)
from .pagination import Page, decode_cursor, page_from_items, validate_limit
from .replicated import ReplicatedOverrideOperation
from .types import OverrideLedgerConfig, OverrideOperation, OverrideTypedValue

logger = logging.getLogger(__name__)


class SqlOverrideLedgerStore:
    """Persist override operations in a SQL-accessible historical database."""

    def __init__(
        self,
        connection: Connection,
        db_backend: str,
        config: OverrideLedgerConfig | None = None,
        *,
        buffer_writes: bool = False,
    ) -> None:
        self.connection = connection
        self.db_backend = db_backend
        self.config = config or OverrideLedgerConfig()
        self.table_config = build_override_table_config(self.config)
        self._pending: list[OverrideOperation] | None = [] if buffer_writes else None
        self._ensure_table()

    def append(self, operation: OverrideOperation) -> None:
        if self._pending is not None:
            self._pending.append(operation)
            return
        self._append_many([operation])

    def flush(self) -> None:
        if not self._pending:
            return
        self._append_many(self._pending)
        self._pending.clear()

    def _append_many(self, operations: list[OverrideOperation]) -> None:
        frame = pl.DataFrame([_operation_row(operation) for operation in operations])
        if self.db_backend == "xtdb":
            XtdbDataframeOps(self.connection).table_insert(
                frame,
                self.config.schema,
                self.config.table,
                table_config=self.table_config,
            )
            return

        DataframeOps(self.connection).table_insert(
            frame,
            self.config.schema,
            self.config.table,
            uniqueness_col_set=["operation_id"],
            prefill_nulls_with_default=False,
        )

    def history_for_entity(
        self,
        owner_user_id: str,
        feed_id: str,
        entity_id: str,
        *,
        cursor: str | None = None,
        limit: int = 100,
    ) -> Page[OverrideOperation]:
        validate_limit(limit)
        cursor_key = decode_cursor(cursor) if cursor else None
        recorded = override_recorded_time_sql(self.db_backend)
        clauses = [
            "owner_user_id = :owner_user_id",
            "feed_id = :feed_id",
            "entity_id = :entity_id",
        ]
        params: dict[str, Any] = {
            "owner_user_id": owner_user_id,
            "feed_id": feed_id,
            "entity_id": entity_id,
            "page_size": limit + 1,
        }
        if cursor_key:
            clauses.append(
                f"({recorded} > :cursor_recorded_at OR "
                f"({recorded} = :cursor_recorded_at AND operation_id > :cursor_operation_id))"
            )
            params.update(
                cursor_recorded_at=cursor_key[0],
                cursor_operation_id=cursor_key[1],
            )
        query = text(
            f"""
            SELECT {self._select_columns()}
            FROM {self.config.schema}.{self.config.table}
            WHERE {" AND ".join(clauses)}
            ORDER BY {recorded}, operation_id
            LIMIT :page_size
            """
        )
        result = self._execute_read(query, params)
        rows = () if result is None else result.mappings()
        history = [_row_operation(dict(row)) for row in rows]
        if self._pending:
            history.extend(
                operation
                for operation in self._pending
                if operation.owner_user_id == owner_user_id
                and operation.feed_id == feed_id
                and operation.entity_id == entity_id
                and (cursor_key is None or _operation_page_key(operation) > cursor_key)
            )
        history.sort(key=_operation_page_key)
        return page_from_items(history, _operation_page_key, limit)

    def projected_history_for_owner(
        self, owner_user_id: str, feed_id: str
    ) -> list[OverrideOperation]:
        recorded = override_recorded_order_sql(self.db_backend)
        result = self._execute_read(
            text(
                f"SELECT {self._select_columns()} "
                f"FROM {self.config.schema}.{self.config.table} "
                "WHERE owner_user_id = :owner_user_id AND feed_id = :feed_id "
                f"ORDER BY {recorded}"
            ),
            {"owner_user_id": owner_user_id, "feed_id": feed_id},
        )
        rows = () if result is None else result.mappings()
        return project_personal_override_operations(
            _row_operation(dict(row)) for row in rows
        )

    def query_operations(
        self,
        owner_user_id: str,
        *,
        view: str,
        valid_at: datetime,
        filters: dict[str, Any],
        cursor: tuple[datetime, str] | None,
        limit: int,
    ) -> tuple[list[OverrideOperation], bool]:
        recorded = override_recorded_time_sql(self.db_backend)
        clauses = ["owner_user_id = :owner_user_id"]
        params: dict[str, Any] = {"owner_user_id": owner_user_id}
        for name in ("entity_id", "field_path"):
            if filters.get(name):
                clauses.append(f"{name} = :{name}")
                params[name] = filters[name]
        if filters.get("known_at"):
            clauses.append(f"{recorded} <= :known_at")
            params["known_at"] = filters["known_at"]
        statement = text(
            f"SELECT {self._select_columns()} "
            f"FROM {self.config.schema}.{self.config.table} "
            f"WHERE {' AND '.join(clauses)} "
            f"ORDER BY {override_recorded_order_sql(self.db_backend)}"
        )
        result = self._execute_read(statement, params)
        rows = project_personal_override_operations(
            []
            if result is None
            else [_row_operation(dict(row)) for row in result.mappings()]
        )
        rows = [
            row
            for row in rows
            if (
                view == "history"
                or view == "active"
                and row.valid_from <= valid_at
                and (row.valid_to is None or valid_at < row.valid_to)
                or view == "upcoming"
                and row.valid_from > valid_at
            )
            and (
                not filters.get("operation_type")
                or row.operation_type == filters["operation_type"]
            )
            and (
                not filters.get("actor_filter")
                or row.actor_user_id == filters["actor_filter"]
                or row.actor_display_name == filters["actor_filter"]
            )
            and (
                not filters.get("recorded_from")
                or cast(datetime, row.recorded_at) >= filters["recorded_from"]
            )
            and (
                not filters.get("recorded_to")
                or cast(datetime, row.recorded_at) < filters["recorded_to"]
            )
            and (
                filters.get("drift") is None
                or row.created_against_stale_source == filters["drift"]
            )
        ]
        rows.sort(
            key=lambda row: (cast(datetime, row.recorded_at), row.operation_id),
            reverse=True,
        )
        if cursor:
            rows = [
                row
                for row in rows
                if (cast(datetime, row.recorded_at), row.operation_id) < cursor
            ]
        return rows[:limit], len(rows) > limit

    def operation_by_id(self, operation_id: str) -> OverrideOperation | None:
        result = self._execute_read(
            text(
                f"SELECT {self._select_columns()} "
                f"FROM {self.config.schema}.{self.config.table} "
                "WHERE operation_id = :operation_id"
            ),
            {"operation_id": operation_id},
        )
        row = None if result is None else result.mappings().first()
        return _row_operation(dict(row)) if row is not None else None

    def shared_operations(
        self,
        layer_ids: list[str],
        *,
        known_at: datetime | None = None,
        filters: dict[str, Any] | None = None,
    ) -> list[ReplicatedOverrideOperation]:
        if not layer_ids:
            return []
        filters = filters or {}
        clauses = ["layer_id IN :layer_ids", "format_version IS NOT NULL"]
        params: dict[str, Any] = {"layer_ids": layer_ids}
        recorded = override_recorded_time_sql(self.db_backend)
        if known_at is not None:
            clauses.append(f"{recorded} <= :known_at")
            params["known_at"] = known_at
        for name in ("entity_id", "field_path", "actor_id", "operation_type"):
            if filters.get(name):
                clauses.append(f"{name} = :{name}")
                params[name] = filters[name]
        statement = text(
            f"SELECT {self._select_columns()} "
            f"FROM {self.config.schema}.{self.config.table} "
            f"WHERE {' AND '.join(clauses)} "
            f"ORDER BY {override_recorded_order_sql(self.db_backend)}"
        ).bindparams(bindparam("layer_ids", expanding=True))
        result = self._execute_read(statement, params)
        return (
            []
            if result is None
            else [_row_replicated(dict(row)) for row in result.mappings()]
        )

    def shared_operation_by_id(
        self, operation_id: str
    ) -> ReplicatedOverrideOperation | None:
        result = self._execute_read(
            text(
                f"SELECT * FROM {self.config.schema}.{self.config.table} "
                "WHERE operation_id = :operation_id AND layer_id IS NOT NULL"
            ),
            {"operation_id": operation_id},
        )
        row = None if result is None else result.mappings().first()
        return _row_replicated(dict(row)) if row is not None else None

    def _execute_read(self, statement: Any, params: dict[str, Any]) -> Any | None:
        try:
            return self.connection.execute(statement, params)
        except Exception as error:
            if self.db_backend != "xtdb" or not _is_xtdb_table_not_found_error(error):
                raise
            logger.info(
                "Override ledger table is not available yet; treating it as empty"
            )
            return None

    def _ensure_table(self) -> None:
        get_backend(self.db_backend).table_configs(self.connection).create(
            self.table_config
        )

    def _select_columns(self) -> str:
        return (
            "*, _system_from AS legacy_system_from"
            if self.db_backend == "xtdb"
            else "*"
        )


def _operation_row(operation: OverrideOperation) -> dict[str, Any]:
    value = operation.value
    return {
        "operation_id": operation.operation_id,
        "change_set_id": operation.change_set_id,
        "owner_user_id": operation.owner_user_id,
        "actor_user_id": operation.actor_user_id,
        "feed_id": operation.feed_id,
        "entity_id": operation.entity_id,
        "field_path": operation.field_path,
        "operation_type": operation.operation_type,
        "value_type": value.value_type if value else None,
        "value_json": _json(value.value_json if value else None),
        "unit": value.unit if value else None,
        "observed_canonical_value_json": _json(operation.observed_canonical_value_json),
        "created_against_stale_source": operation.created_against_stale_source,
        "valid_from": operation.valid_from.astimezone(UTC),
        "valid_to": (
            operation.valid_to.astimezone(UTC)
            if operation.valid_to is not None
            else None
        ),
        "reason": operation.reason,
        "comment": operation.comment,
        "metadata_json": _json(operation.metadata_json),
        "actor_id": operation.actor_user_id,
        "actor_display_name": operation.actor_display_name,
        "recorded_at": operation.recorded_at or datetime.now(UTC),
        "crdt_document_id": operation.document_id,
        "generation": operation.generation,
    }


def _operation_page_key(operation: OverrideOperation) -> tuple[datetime, str]:
    return operation.recorded_at or operation.valid_from, operation.operation_id


def _row_operation(row: dict[str, Any]) -> OverrideOperation:
    value_type = row.get("value_type")
    value = (
        OverrideTypedValue(
            str(value_type),
            _load_json(row.get("value_json")) or {},
            unit=row.get("unit"),
        )
        if value_type
        else None
    )
    metadata = _load_json(row.get("metadata_json")) or {}
    if row.get("recorded_at") is None and row.get("legacy_system_from") is None:
        metadata["recorded_at_estimated_from_valid_time"] = True
    return OverrideOperation(
        operation_id=str(row["operation_id"]),
        change_set_id=str(row["change_set_id"]),
        owner_user_id=str(row["owner_user_id"]),
        actor_user_id=str(row["actor_user_id"]),
        feed_id=str(row["feed_id"]),
        entity_id=str(row["entity_id"]),
        field_path=str(row["field_path"]),
        operation_type=row["operation_type"],
        value=value,
        observed_canonical_value_json=_load_json(
            row.get("observed_canonical_value_json")
        ),
        created_against_stale_source=bool(row["created_against_stale_source"]),
        valid_from=_aware(row["valid_from"]),
        valid_to=_aware(row["valid_to"]) if row.get("valid_to") is not None else None,
        reason=row.get("reason"),
        comment=row.get("comment"),
        metadata_json=metadata,
        recorded_at=_aware(
            row.get("legacy_system_from") or row.get("recorded_at") or row["valid_from"]
        ),
        actor_display_name=row.get("actor_display_name"),
        document_id=row.get("crdt_document_id"),
        generation=int(row.get("generation") or 1),
    )


def _row_replicated(row: dict[str, Any]) -> ReplicatedOverrideOperation:
    metadata = _load_json(row.get("metadata_json")) or {}
    value_type = row.get("value_type")
    value = (
        OverrideTypedValue(
            str(value_type), _load_json(row.get("value_json")) or {}, row.get("unit")
        )
        if value_type
        else None
    )
    return ReplicatedOverrideOperation(
        format_version=int(row.get("format_version") or 1),
        operation_id=str(row["operation_id"]),
        change_set_id=str(row["change_set_id"]),
        layer_id=str(row["layer_id"]),
        actor_id=str(row.get("actor_id") or row["actor_user_id"]),
        feed_id=str(row["feed_id"]),
        entity_id=str(row["entity_id"]),
        field_path=str(row["field_path"]),
        operation_type=str(row["operation_type"]),  # type: ignore[arg-type]
        value=value,
        supersedes_operation_ids=tuple(
            _load_json_array(row.get("supersedes_operation_ids_json"))
        ),
        removes_operation_ids=tuple(
            _load_json_array(row.get("removes_operation_ids_json"))
        ),
        valid_from=_aware(row["valid_from"]),
        valid_to=_aware(row["valid_to"]) if row.get("valid_to") is not None else None,
        recorded_at=_aware(
            row.get("legacy_system_from") or row.get("recorded_at") or row["valid_from"]
        ),
        observed_canonical_value_json=_load_json(
            row.get("observed_canonical_value_json")
        ),
        comment=row.get("comment"),
        metadata_json={
            **metadata,
            "generation": int(row.get("generation") or 1),
            "actor_display_name": row.get("actor_display_name")
            or metadata.get("actor_display_name"),
            "document_id": row.get("crdt_document_id") or metadata.get("document_id"),
        },
        payload_hash=row.get("payload_hash"),
    )


def _json(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _load_json(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    return cast(dict[str, Any], json.loads(str(value)))


def _load_json_array(value: Any) -> list[str]:
    if value is None:
        return []
    parsed = value if isinstance(value, list) else json.loads(str(value))
    if not isinstance(parsed, list):
        raise TypeError("stored operation references are invalid")
    return [str(item) for item in parsed]


def _aware(value: Any) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value))
    if dt.tzinfo is None or dt.utcoffset() is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)
