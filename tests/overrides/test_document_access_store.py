from datetime import UTC, datetime, timezone

import pytest

from polars_hist_db.overrides import (
    AccessGrantInput,
    DocumentArchived,
    DocumentRevisionConflict,
    IdempotencyConflict,
    InMemoryDocumentAccessStore,
)

TIME = datetime(2026, 7, 13, 12, tzinfo=UTC)


def test_lifecycle_is_versioned_and_preserves_grant_history():
    store = InMemoryDocumentAccessStore()
    created = store.create(
        "doc-1",
        "Analysts",
        None,
        "admin",
        TIME,
        initial_grants=[AccessGrantInput("grant-1", "analysts", "editor")],
        idempotency_key="create-1",
    )
    revoked = store.revoke(
        "doc-1", "analysts", "admin", TIME, 1, idempotency_key="revoke-1"
    )
    granted = store.grant(
        "doc-1",
        AccessGrantInput("grant-2", "analysts", "resolver"),
        "admin",
        TIME,
        2,
        idempotency_key="grant-2",
    )
    archived = store.archive("doc-1", "admin", TIME, 3, idempotency_key="archive-1")

    assert created.document.revision == 1
    assert revoked.document.revision == 2
    assert revoked.grants[0].active is False
    assert granted.document.revision == 3
    assert archived.document.status == "archived"
    assert archived.document.revision == 4
    first_page = store.grants("doc-1", include_revoked=True, limit=1)
    second_page = store.grants(
        "doc-1", include_revoked=True, cursor=first_page.next_cursor, limit=1
    )
    assert len(first_page.items + second_page.items) == 2
    assert first_page.next_cursor is not None
    assert second_page.next_cursor is None
    assert store.grants("doc-1").items[0].role == "resolver"
    assert store.list_for_groups(["analysts"], include_archived=True).items == (
        archived.document,
    )
    with pytest.raises(DocumentArchived):
        store.grant(
            "doc-1",
            AccessGrantInput("grant-3", "reviewers", "viewer"),
            "admin",
            TIME,
            4,
            idempotency_key="grant-3",
        )


def test_idempotency_and_revision_conflicts_are_explicit():
    store = InMemoryDocumentAccessStore()
    accepted = store.create(
        "doc-1", "Analysts", None, "admin", TIME, idempotency_key="create-1"
    )
    duplicate = store.create(
        "doc-1", "Analysts", None, "admin", TIME, idempotency_key="create-1"
    )

    assert accepted.accepted
    assert duplicate.duplicate
    with pytest.raises(IdempotencyConflict):
        store.create("doc-2", "Other", None, "admin", TIME, idempotency_key="create-1")
    with pytest.raises(DocumentRevisionConflict):
        store.archive("doc-1", "admin", TIME, 2, idempotency_key="archive-1")


def test_guard_matches_the_portable_active_revision_contract():
    guard = InMemoryDocumentAccessStore().guard("doc-1", 4)

    assert guard.key_values == {"document_id": "doc-1"}
    assert guard.expected_values == {"status": "active", "revision": 4}


def test_create_can_ensure_an_existing_active_document_for_the_same_owner():
    store = InMemoryDocumentAccessStore()
    created = store.create(
        "doc-1",
        "Analysts",
        None,
        "admin",
        TIME,
        initial_grants=[AccessGrantInput("grant-1", "analysts", "manager")],
        idempotency_key="create-1",
        owning_group="analysts",
    )

    existing = store.create(
        "doc-1",
        " analysts ",
        None,
        "admin",
        TIME,
        initial_grants=[AccessGrantInput("grant-2", "analysts", "manager")],
        idempotency_key="create-2",
        owning_group="analysts",
        allow_existing=True,
    )

    assert existing.document == created.document
    assert existing.grants == created.grants
    assert existing.accepted is False
    assert existing.duplicate is True


def test_same_name_can_exist_in_different_ownership_scopes():
    store = InMemoryDocumentAccessStore()
    store.create(
        "doc-1",
        "Analysts",
        None,
        "admin",
        TIME,
        idempotency_key="create-1",
        owning_group="analysts",
    )

    created = store.create(
        "doc-2",
        "Analysts",
        None,
        "admin",
        TIME,
        idempotency_key="create-2",
        owning_group="reviewers",
        allow_existing=True,
    )

    assert created.document.document_id == "doc-2"
    assert created.document.owning_group == "reviewers"
    assert created.accepted is True

    first_page = store.list_all(limit=1)
    second_page = store.list_all(cursor=first_page.next_cursor, limit=1)
    assert {
        document.document_id for document in first_page.items + second_page.items
    } == {
        "doc-1",
        "doc-2",
    }
    assert first_page.next_cursor is not None
    assert second_page.next_cursor is None


def test_allow_existing_idempotency_ignores_generated_identifiers():
    store = InMemoryDocumentAccessStore()
    created = store.create(
        "generated-doc-1",
        "Analysts",
        None,
        "admin",
        TIME,
        initial_grants=[AccessGrantInput("generated-grant-1", "analysts", "manager")],
        idempotency_key="create-1",
        owning_group="analysts",
        allow_existing=True,
    )

    retried = store.create(
        "generated-doc-2",
        "Analysts",
        None,
        "admin",
        TIME,
        initial_grants=[AccessGrantInput("generated-grant-2", "analysts", "manager")],
        idempotency_key="create-1",
        owning_group="analysts",
        allow_existing=True,
    )

    assert retried.document == created.document
    assert retried.duplicate is True


def test_allow_existing_records_the_idempotency_key():
    store = InMemoryDocumentAccessStore()
    store.create(
        "doc-1",
        "Analysts",
        None,
        "admin",
        TIME,
        idempotency_key="create-1",
        owning_group="analysts",
    )
    store.create(
        "generated-doc",
        "Analysts",
        None,
        "admin",
        TIME,
        idempotency_key="ensure-1",
        owning_group="analysts",
        allow_existing=True,
    )

    with pytest.raises(IdempotencyConflict):
        store.create("doc-2", "Other", None, "admin", TIME, idempotency_key="ensure-1")


def test_invalid_initial_grants_do_not_partially_create_document():
    store = InMemoryDocumentAccessStore()

    with pytest.raises(ValueError, match="initial grants must be unique"):
        store.create(
            "doc-1",
            "Analysts",
            None,
            "admin",
            TIME,
            initial_grants=[
                AccessGrantInput("grant-1", "group", "viewer"),
                AccessGrantInput("grant-2", "GROUP", "editor"),
            ],
            idempotency_key="create-1",
        )

    assert store.get("doc-1") is None
