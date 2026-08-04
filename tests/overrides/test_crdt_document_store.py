from datetime import UTC, datetime, timezone
from typing import Any
from uuid import uuid4

import pytest
from pycrdt import Doc, Map

from polars_hist_db.config import TableColumnConfig, TableConfig
from polars_hist_db.overrides import (
    AtomicInsert,
    AtomicUpdate,
    CrdtDocument,
    CrdtPreconditionFailed,
    CrdtRevisionConflict,
    InMemoryCrdtDocumentStore,
    MariaDbCrdtDocumentStore,
    RowGuard,
    prepare_crdt_update,
)


def _document_update() -> bytes:
    document: Any = Doc()
    document["drafts"] = Map({"title": "Initial"})
    return document.get_update()


def test_document_store_merges_offline_updates_and_rebuilds_from_snapshot():
    base_update = _document_update()
    left = Doc()
    right = Doc()
    left.apply_update(base_update)
    right.apply_update(base_update)
    base_state = left.get_state()

    left.get("drafts", type=Map)["left"] = "offline"
    right.get("drafts", type=Map)["right"] = "offline"

    store = InMemoryCrdtDocumentStore()
    store.append_update("document-1", base_update)
    store.append_update("document-1", left.get_update(base_state), expected_revision=1)
    store.append_update("document-1", right.get_update(base_state), expected_revision=2)
    snapshot = store.write_snapshot("document-1")

    rebuilt = Doc()
    rebuilt.apply_update(snapshot.update)
    caught_up = Doc()
    caught_up.apply_update(base_update)
    caught_up.apply_update(store.diff("document-1", base_state))

    assert rebuilt.get("drafts", type=Map).to_py() == {
        "title": "Initial",
        "left": "offline",
        "right": "offline",
    }
    assert snapshot.revision == 3
    assert store.snapshot("document-1") == snapshot
    assert (
        caught_up.get("drafts", type=Map).to_py()
        == rebuilt.get("drafts", type=Map).to_py()
    )


def test_mariadb_store_diff_uses_current_document(monkeypatch):
    source = Doc()
    source["drafts"] = Map({"title": "Current"})
    store = object.__new__(MariaDbCrdtDocumentStore)
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


def test_document_store_deduplicates_exact_updates_and_checks_revisions():
    update = _document_update()
    store = InMemoryCrdtDocumentStore()

    first = store.append_update("document-1", update, expected_revision=0)
    duplicate = store.append_update("document-1", update, expected_revision=0)

    assert first.accepted is True
    assert duplicate.accepted is False
    assert duplicate.document.revision == 1

    with pytest.raises(ValueError, match="expected revision"):
        store.append_update("document-2", update, expected_revision=1)
    assert store.load_document("document-2") is None


def _operation_update(operation_id: str) -> bytes:
    document: Any = Doc()
    document["operations"] = Map(
        {
            operation_id: {
                "format_version": 1,
                "operation_id": operation_id,
                "change_set_id": str(uuid4()),
                "layer_id": "team",
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


def test_prepared_commit_finalizes_operations_and_rejects_later_mutation():
    operation_id = str(uuid4())
    source_update = _operation_update(operation_id)
    accepted_at = datetime(2026, 7, 12, 11, tzinfo=UTC)

    prepared = prepare_crdt_update(
        "document-1",
        None,
        source_update,
        actor_id="user-1",
        recorded_at=accepted_at,
        authoritative_metadata={"actor_display_name": "Verified User"},
    )
    store = InMemoryCrdtDocumentStore()
    committed = store.commit(prepared)

    assert committed.accepted is True
    assert committed.revision == 1
    assert prepared.source_update_hash != prepared.accepted_update_hash
    projected = store.projected_operations("document-1").items
    assert projected[0].actor_id == "user-1"
    assert projected[0].metadata_json == {"actor_display_name": "Verified User"}
    assert projected[0].payload_hash is not None

    document: Any = Doc()
    assert committed.document is not None
    document.apply_update(committed.document.update)
    operations = document.get("operations", type=Map)
    operation = operations.get(operation_id)
    operations[operation_id] = {**operation, "comment": "changed"}

    with pytest.raises(ValueError, match="cannot be mutated"):
        prepare_crdt_update(
            "document-1",
            committed.document,
            document.get_update(committed.document.state_vector),
            actor_id="user-1",
            recorded_at=accepted_at,
        )


def test_prepared_commit_checks_revision_guards_and_atomic_inserts():
    table = TableConfig(
        name="outbox",
        schema="overrides",
        primary_keys=("id",),
        columns=[TableColumnConfig("outbox", "id", "VARCHAR(64)", nullable=False)],
    )
    store = InMemoryCrdtDocumentStore()
    store.seed_row(table, {"id": "layer-1", "version": 2})
    prepared = prepare_crdt_update(
        "document-1",
        None,
        _document_update(),
        actor_id="user-1",
        recorded_at=datetime(2026, 7, 12, 11, tzinfo=UTC),
    )

    result = store.commit(
        prepared,
        guards=[RowGuard(table, {"id": "layer-1"}, {"version": 2})],
        inserts=[AtomicInsert(table, {"id": "outbox-1"})],
        updates=[
            AtomicUpdate(table, {"id": "layer-1"}, {"version": 2}, {"version": 3})
        ],
    )

    assert result.accepted is True
    assert store._rows[store._row_key(table, {"id": "layer-1"})]["version"] == 3
    stale = prepare_crdt_update(
        "document-1",
        None,
        _document_update(),
        actor_id="user-1",
        recorded_at=datetime(2026, 7, 12, 11, tzinfo=UTC),
    )
    with pytest.raises(CrdtRevisionConflict):
        store.commit(stale)

    rejected = prepare_crdt_update(
        "document-2",
        None,
        _document_update(),
        actor_id="user-1",
        recorded_at=datetime(2026, 7, 12, 11, tzinfo=UTC),
    )
    with pytest.raises(CrdtPreconditionFailed):
        store.commit(
            rejected,
            guards=[RowGuard(table, {"id": "layer-1"}, {"version": 2})],
        )

    with pytest.raises(ValueError, match="atomic insert"):
        store.commit(
            rejected,
            updates=[
                AtomicUpdate(table, {"id": "layer-1"}, {"version": 3}, {"version": 4})
            ],
            inserts=[AtomicInsert(table, {"id": "outbox-1"})],
        )
    assert store._rows[store._row_key(table, {"id": "layer-1"})]["version"] == 3
