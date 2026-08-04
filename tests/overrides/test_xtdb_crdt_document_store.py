from datetime import UTC, datetime, timezone
from typing import Any
from uuid import uuid4

from pycrdt import Doc, Map

from polars_hist_db.overrides import (
    CrdtDocument,
    CrdtDocumentStoreConfig,
    OverrideLedgerConfig,
    XtdbCrdtDocumentStore,
    prepare_crdt_update,
)


def _operation_update(operation_id: str) -> bytes:
    document: Any = Doc()
    document["operations"] = Map(
        {
            operation_id: {
                "format_version": 1,
                "operation_id": operation_id,
                "change_set_id": str(uuid4()),
                "layer_id": "shared",
                "feed_id": "records",
                "entity_id": "record-1",
                "field_path": "status",
                "operation_type": "set",
                "value": {"value_type": "enum", "value_json": {"value": "open"}},
                "supersedes_operation_ids": [],
                "removes_operation_ids": [],
                "valid_from": "2026-07-12T10:00:00+00:00",
                "valid_to": None,
            }
        }
    )
    return document.get_update()


def test_xtdb_store_submits_one_asserted_transaction(monkeypatch):
    prepared = prepare_crdt_update(
        "document-1",
        None,
        _operation_update(str(uuid4())),
        actor_id="user-1",
        recorded_at=datetime(2026, 7, 12, 11, tzinfo=UTC),
    )
    statements: list[str] = []
    store = XtdbCrdtDocumentStore(
        object(), CrdtDocumentStoreConfig(), OverrideLedgerConfig()
    )
    document = CrdtDocument(
        prepared.document_id,
        1,
        prepared.state_vector,
        prepared.accepted_update,
    )
    monkeypatch.setattr(store, "_ensure_tables", lambda: None)
    monkeypatch.setattr(store, "_duplicate_result", lambda _: None)
    monkeypatch.setattr(store, "load_document", lambda _: document)
    monkeypatch.setattr(
        "polars_hist_db.overrides.xtdb._execute_xtdb_transaction",
        lambda _, transaction: statements.extend(transaction),
    )

    result = store.commit(prepared)

    assert result.accepted is True
    assert statements[0].startswith("ASSERT NOT EXISTS")
    assert any(
        "INSERT INTO overrides.crdt_documents" in statement for statement in statements
    )
    assert any("_valid_from" in statement for statement in statements)


def test_xtdb_store_bootstraps_every_configured_column(monkeypatch):
    statements: list[list[str]] = []
    store = XtdbCrdtDocumentStore(
        object(), CrdtDocumentStoreConfig(), OverrideLedgerConfig()
    )
    monkeypatch.setattr(store, "_rows", lambda _: [])
    monkeypatch.setattr(
        "polars_hist_db.overrides.xtdb._execute_xtdb_transaction",
        lambda _, transaction: statements.append(list(transaction)),
    )

    store._ensure_tables()

    assert "document_id" in statements[0][0]
    assert "head_state_vector_base64" in statements[0][0]
    assert statements[0][1].startswith("ERASE FROM overrides.crdt_documents")


def test_xtdb_store_diff_uses_current_document(monkeypatch):
    source = Doc()
    source["drafts"] = Map({"title": "Current"})
    store = XtdbCrdtDocumentStore(
        object(), CrdtDocumentStoreConfig(), OverrideLedgerConfig()
    )
    monkeypatch.setattr(
        store,
        "load_document",
        lambda _: CrdtDocument(
            "document-1", 1, source.get_state(), source.get_update()
        ),
    )

    caught_up = Doc()
    caught_up.apply_update(store.diff("document-1", b"\x00"))

    assert caught_up.get("drafts", type=Map).to_py() == {"title": "Current"}
