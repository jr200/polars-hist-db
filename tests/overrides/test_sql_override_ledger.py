from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, ClassVar

import pytest

from polars_hist_db.overrides import OverrideOperation, SqlOverrideLedgerStore


class MissingTableConnection:
    def execute(self, *_args, **_kwargs):
        raise RuntimeError("Table not found: overrides.data_override_operations")


def _missing_table_store(monkeypatch) -> SqlOverrideLedgerStore:
    monkeypatch.setattr(SqlOverrideLedgerStore, "_ensure_table", lambda self: None)
    return SqlOverrideLedgerStore(MissingTableConnection(), "xtdb")  # type: ignore[arg-type]


def test_absent_xtdb_ledger_has_empty_projected_owner_history(monkeypatch):
    store = _missing_table_store(monkeypatch)

    assert store.projected_history_for_owner("owner-1", "feed-1") == []


def test_projected_owner_history_propagates_non_missing_table_errors(monkeypatch):
    monkeypatch.setattr(SqlOverrideLedgerStore, "_ensure_table", lambda self: None)

    class BrokenConnection:
        def execute(self, *_args, **_kwargs):
            raise TimeoutError("database timeout")

    store = SqlOverrideLedgerStore(BrokenConnection(), "xtdb")  # type: ignore[arg-type]

    with pytest.raises(TimeoutError, match="database timeout"):
        store.projected_history_for_owner("owner-1", "feed-1")


def test_absent_xtdb_ledger_is_empty_for_every_read_shape(monkeypatch):
    store = _missing_table_store(monkeypatch)
    page = store.history_for_entity("owner-1", "feed-1", "entity-1")
    operations, has_more = store.query_operations(
        "owner-1",
        view="history",
        valid_at=datetime(2026, 8, 8, tzinfo=UTC),
        filters={},
        cursor=None,
        limit=10,
    )

    assert page.items == ()
    assert page.next_cursor is None
    assert operations == []
    assert has_more is False
    assert store.operation_by_id("operation-1") is None
    assert store.shared_operations(["layer-1"]) == []
    assert store.shared_operation_by_id("operation-1") is None


def test_xtdb_history_uses_system_time_keyset_order(monkeypatch):
    monkeypatch.setattr(SqlOverrideLedgerStore, "_ensure_table", lambda self: None)

    class RecordingConnection:
        statements: ClassVar[list[str]] = []

        def execute(self, statement, *_args, **_kwargs):
            self.statements.append(str(statement))
            return self

        def mappings(self):
            return []

    connection = RecordingConnection()
    store = SqlOverrideLedgerStore(connection, "xtdb")  # type: ignore[arg-type]

    store.history_for_entity("owner-1", "feed-1", "entity-1")
    store.query_operations(
        "owner-1",
        view="history",
        valid_at=datetime(2026, 8, 8, tzinfo=UTC),
        filters={},
        cursor=None,
        limit=10,
    )

    assert "ORDER BY _system_from, operation_id" in connection.statements[0]
    assert "ORDER BY _system_from, CASE WHEN operation_type" in connection.statements[1]


def test_buffered_xtdb_writes_flush_in_one_insert(monkeypatch):
    monkeypatch.setattr(SqlOverrideLedgerStore, "_ensure_table", lambda self: None)
    inserted: dict[str, Any] = {}

    def capture_insert(self, frame, schema, table, **kwargs):
        inserted["rows"] = frame.to_dicts()
        inserted["schema"] = schema
        inserted["table"] = table
        return frame.height

    monkeypatch.setattr(
        "polars_hist_db.overrides.ledger_sql.XtdbDataframeOps.table_insert",
        capture_insert,
    )
    store = SqlOverrideLedgerStore(
        object(),
        "xtdb",
        buffer_writes=True,  # type: ignore[arg-type]
    )
    operation = OverrideOperation(
        operation_id="operation-1",
        change_set_id="change-1",
        owner_user_id="owner-1",
        actor_user_id="actor-1",
        feed_id="feed-1",
        entity_id="entity-1",
        field_path="status",
        operation_type="set",
        value=None,
        observed_canonical_value_json=None,
        created_against_stale_source=False,
        valid_from=datetime(2026, 8, 8, tzinfo=UTC),
        valid_to=None,
    )

    store.append(operation)
    assert inserted == {}

    store.flush()

    assert len(inserted["rows"]) == 1
    assert inserted["rows"][0]["operation_id"] == "operation-1"
    assert inserted["schema"] == "overrides"
    assert inserted["table"] == "data_override_operations"
