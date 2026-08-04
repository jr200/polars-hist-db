from datetime import UTC, datetime, timezone

import pytest

from polars_hist_db.overrides import (
    AccessGrantInput,
    DocumentAccessError,
    DocumentAccessStoreConfig,
    XtdbDocumentAccessStore,
)
from polars_hist_db.overrides.pagination import encode_cursor


def test_xtdb_access_create_submits_one_asserted_transaction(monkeypatch):
    statements: list[str] = []
    store = XtdbDocumentAccessStore(object(), DocumentAccessStoreConfig())
    monkeypatch.setattr(store, "_duplicate", lambda *_: None)
    monkeypatch.setattr(store, "_ensure_tables", lambda: None)
    monkeypatch.setattr(
        "polars_hist_db.overrides.xtdb._execute_xtdb_transaction",
        lambda _, transaction: statements.extend(transaction),
    )

    result = store.create(
        "document-1",
        "Shared corrections",
        None,
        "user-1",
        datetime(2026, 7, 12, 11, tzinfo=UTC),
        initial_grants=(AccessGrantInput("grant-1", "operators", "editor"),),
        idempotency_key="command-1",
        owning_group="operators",
    )

    assert result.accepted is True
    assert result.document.revision == 1
    assert statements[0].startswith("ASSERT NOT EXISTS")
    name_assertion = next(
        statement for statement in statements if "normalized_name" in statement
    )
    assert "owning_group = 'operators'" in name_assertion
    assert "normalized_name = 'shared corrections'" in name_assertion
    assert any("INSERT INTO overrides.document_access" in item for item in statements)
    assert any(
        "INSERT INTO overrides.document_access_grants" in item for item in statements
    )
    assert any(
        "INSERT INTO overrides.document_access_commands" in item for item in statements
    )


def test_xtdb_access_guard_uses_active_revision():
    store = XtdbDocumentAccessStore(object(), DocumentAccessStoreConfig())

    guard = store.guard("document-1", 3)

    assert guard.key_values == {"document_id": "document-1"}
    assert guard.expected_values == {"status": "active", "revision": 3}


def test_xtdb_access_get_uses_logical_key_not_native_id(monkeypatch):
    queries: list[str] = []
    store = XtdbDocumentAccessStore(object(), DocumentAccessStoreConfig())
    monkeypatch.setattr(store, "_rows", lambda sql: queries.append(sql) or [])

    store.get("e9ca81c9-6116-4247-b951-185e82033440")

    assert "WHERE document_id =" in queries[0]
    assert "WHERE _id =" not in queries[0]


def test_xtdb_access_lists_use_keyset_pagination(monkeypatch):
    queries: list[str] = []
    store = XtdbDocumentAccessStore(object(), DocumentAccessStoreConfig())
    monkeypatch.setattr(store, "_rows", lambda sql: queries.append(sql) or [])
    cursor = encode_cursor((datetime(2026, 7, 12, 11, tzinfo=UTC), "document-1"))

    page = store.list_all(cursor=cursor, limit=7)

    assert page.items == ()
    assert page.next_cursor is None
    assert "created_at >" in queries[0]
    assert "document_id > 'document-1'" in queries[0]
    assert "LIMIT 8" in queries[0]


def test_xtdb_access_create_reports_the_assertion_that_conflicted(monkeypatch):
    store = XtdbDocumentAccessStore(object(), DocumentAccessStoreConfig())
    monkeypatch.setattr(store, "_duplicate", lambda *_: None)
    monkeypatch.setattr(store, "_ensure_tables", lambda: None)

    def reject(*_):
        raise RuntimeError("document name already exists\nDETAIL: assertion failed")

    monkeypatch.setattr(
        "polars_hist_db.overrides.xtdb._execute_xtdb_transaction", reject
    )

    with pytest.raises(DocumentAccessError, match="^document name already exists$"):
        store.create(
            "document-1",
            "Shared corrections",
            None,
            "user-1",
            datetime(2026, 7, 12, 11, tzinfo=UTC),
            idempotency_key="command-1",
        )


def test_xtdb_access_create_does_not_mask_unexpected_database_errors(monkeypatch):
    store = XtdbDocumentAccessStore(object(), DocumentAccessStoreConfig())
    monkeypatch.setattr(store, "_duplicate", lambda *_: None)
    monkeypatch.setattr(store, "_ensure_tables", lambda: None)
    database_error = RuntimeError("connection closed")

    def reject(*_):
        raise database_error

    monkeypatch.setattr(
        "polars_hist_db.overrides.xtdb._execute_xtdb_transaction", reject
    )

    with pytest.raises(RuntimeError, match="connection closed") as error:
        store.create(
            "document-1",
            "Shared corrections",
            None,
            "user-1",
            datetime(2026, 7, 12, 11, tzinfo=UTC),
            idempotency_key="command-1",
        )

    assert error.value is database_error


def test_xtdb_access_bootstrap_erases_rows_in_a_followup_transaction(monkeypatch):
    transactions: list[list[str]] = []
    store = XtdbDocumentAccessStore(object(), DocumentAccessStoreConfig())
    monkeypatch.setattr(store, "_rows", lambda *_: [])
    monkeypatch.setattr(
        "polars_hist_db.overrides.xtdb._execute_xtdb_transaction",
        lambda _, statements: transactions.append(list(statements)),
    )

    store._ensure_tables()

    assert len(transactions) == 6
    assert all(len(transaction) == 1 for transaction in transactions)
    assert [
        transaction[0].lstrip().split(maxsplit=1)[0] for transaction in transactions
    ] == [
        "INSERT",
        "ERASE",
        "INSERT",
        "ERASE",
        "INSERT",
        "ERASE",
    ]
