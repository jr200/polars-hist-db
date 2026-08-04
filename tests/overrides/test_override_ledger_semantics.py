from datetime import UTC, datetime

import pytest

from polars_hist_db.overrides import (
    InMemoryOverrideLedgerStore,
    OverrideLedger,
    OverrideTypedValue,
)


def _utc(hour: int) -> datetime:
    return datetime(2026, 7, 9, hour, tzinfo=UTC)


def test_record_classification_set_replace_and_system_close_timeline():
    store = InMemoryOverrideLedgerStore()
    ledger = OverrideLedger(store)

    ledger.set_field(
        owner_user_id="user-1",
        actor_user_id="user-1",
        feed_id="records",
        entity_id="record-1",
        field_path="classification",
        value=OverrideTypedValue("enum", {"value": "probable"}),
        observed_canonical_value_json={"value": "expected"},
        valid_from=_utc(13),
        comment="first assessment",
    )
    ledger.set_field(
        owner_user_id="user-1",
        actor_user_id="user-1",
        feed_id="records",
        entity_id="record-1",
        field_path="classification",
        value=OverrideTypedValue("enum", {"value": "possible"}),
        observed_canonical_value_json={"value": "expected"},
        valid_from=_utc(15),
    )
    ledger.close_field(
        owner_user_id="user-1",
        actor_user_id="system",
        feed_id="records",
        entity_id="record-1",
        field_path="classification",
        valid_to=_utc(20),
        reason="source_match",
        system=True,
    )

    raw_history = ledger.history_for_entity("user-1", "records", "record-1").items
    history = ledger.projected_history_for_entity("user-1", "records", "record-1").items

    assert [operation.operation_type for operation in raw_history] == [
        "set",
        "close",
        "set",
        "system_close",
    ]
    assert raw_history[0].valid_to is None
    assert raw_history[2].valid_to is None
    assert history[0].valid_from == _utc(13)
    assert history[0].valid_to == _utc(15)
    assert history[1].valid_from == _utc(15)
    assert history[1].valid_to == _utc(15)
    assert history[1].reason == "replaced"
    assert history[2].value == OverrideTypedValue("enum", {"value": "possible"})
    assert history[2].valid_from == _utc(15)
    assert history[2].valid_to == _utc(20)
    assert history[3].valid_from == _utc(20)
    assert history[3].valid_to == _utc(20)
    assert history[3].reason == "source_match"
    assert ledger.active_for_entity("user-1", "records", "record-1") == []

    first_page = ledger.history_for_entity("user-1", "records", "record-1", limit=2)
    second_page = ledger.history_for_entity(
        "user-1",
        "records",
        "record-1",
        cursor=first_page.next_cursor,
        limit=2,
    )
    assert first_page.items + second_page.items == raw_history
    assert first_page.next_cursor is not None
    assert second_page.next_cursor is None
    with pytest.raises(ValueError, match="limit must be between"):
        ledger.history_for_entity("user-1", "records", "record-1", limit=0)
    with pytest.raises(ValueError, match="invalid cursor"):
        ledger.history_for_entity(
            "user-1", "records", "record-1", cursor="not-a-cursor"
        )


def test_set_rejects_naive_valid_from():
    ledger = OverrideLedger(InMemoryOverrideLedgerStore())

    with pytest.raises(ValueError, match="timezone-aware"):
        ledger.set_field(
            owner_user_id="user-1",
            actor_user_id="user-1",
            feed_id="records",
            entity_id="record-1",
            field_path="status",
            value=OverrideTypedValue("enum", {"value": "deleted"}),
            observed_canonical_value_json={"value": "expected"},
            valid_from=datetime.fromisoformat("2026-07-09T13:00:00"),
        )


def test_grouped_sets_share_change_set_id():
    ledger = OverrideLedger(InMemoryOverrideLedgerStore())

    ledger.set_field(
        owner_user_id="user-1",
        actor_user_id="user-1",
        feed_id="records",
        entity_id="record-1",
        field_path="classification",
        value=OverrideTypedValue("enum", {"value": "probable"}),
        observed_canonical_value_json={"value": "expected"},
        valid_from=_utc(13),
        change_set_id="change-1",
    )
    ledger.set_field(
        owner_user_id="user-1",
        actor_user_id="user-1",
        feed_id="records",
        entity_id="record-1",
        field_path="status",
        value=OverrideTypedValue("enum", {"value": "probable"}),
        observed_canonical_value_json={"value": "expected"},
        valid_from=_utc(13),
        change_set_id="change-1",
    )

    history = ledger.history_for_entity("user-1", "records", "record-1").items
    assert {operation.change_set_id for operation in history} == {"change-1"}
    assert len(ledger.active_for_entity("user-1", "records", "record-1")) == 2


def test_replacing_same_field_leaves_one_active_override_and_keeps_audit_history():
    ledger = OverrideLedger(InMemoryOverrideLedgerStore())

    first = ledger.set_field(
        owner_user_id="user-1",
        actor_user_id="user-1",
        feed_id="records",
        entity_id="record-1",
        field_path="status",
        value=OverrideTypedValue("enum", {"value": "loading"}),
        observed_canonical_value_json={"value": "scheduled"},
        valid_from=_utc(13),
    )
    second = ledger.set_field(
        owner_user_id="user-1",
        actor_user_id="user-1",
        feed_id="records",
        entity_id="record-1",
        field_path="status",
        value=OverrideTypedValue("enum", {"value": "in_transit"}),
        observed_canonical_value_json={"value": "scheduled"},
        valid_from=_utc(14),
    )

    history = ledger.projected_history_for_entity("user-1", "records", "record-1").items
    active = ledger.active_for_entity("user-1", "records", "record-1")

    assert [operation.operation_type for operation in history] == [
        "set",
        "close",
        "set",
    ]
    assert history[0].operation_id == first.operation_id
    assert history[0].valid_to == _utc(14)
    assert history[2].operation_id == second.operation_id
    assert active == [second]


def test_user_close_preserves_prior_operation_history():
    ledger = OverrideLedger(InMemoryOverrideLedgerStore())

    set_operation = ledger.set_field(
        owner_user_id="user-1",
        actor_user_id="user-1",
        feed_id="records",
        entity_id="record-1",
        field_path="status",
        value=OverrideTypedValue("enum", {"value": "in_transit"}),
        observed_canonical_value_json={"value": "loading"},
        valid_from=_utc(13),
    )
    close_operation = ledger.close_field(
        owner_user_id="user-1",
        actor_user_id="user-1",
        feed_id="records",
        entity_id="record-1",
        field_path="status",
        valid_to=_utc(16),
        reason="user_close",
    )

    history = ledger.projected_history_for_entity("user-1", "records", "record-1").items

    assert [operation.operation_id for operation in history] == [
        set_operation.operation_id,
        close_operation.operation_id,
    ]
    assert [operation.operation_type for operation in history] == ["set", "close"]
    assert history[0].valid_to == _utc(16)
    assert ledger.active_for_entity("user-1", "records", "record-1") == []


def test_stale_source_flag_and_observed_canonical_value_are_preserved():
    ledger = OverrideLedger(InMemoryOverrideLedgerStore())

    operation = ledger.set_field(
        owner_user_id="user-1",
        actor_user_id="user-1",
        feed_id="records",
        entity_id="record-1",
        field_path="record_mcm",
        value=OverrideTypedValue("decimal", {"value": "123.456"}, unit="mcm"),
        observed_canonical_value_json={"value": "100.000"},
        created_against_stale_source=True,
        valid_from=_utc(13),
    )

    assert operation.observed_canonical_value_json == {"value": "100.000"}
    assert operation.created_against_stale_source is True
    assert operation.value is not None
    assert operation.value.unit == "mcm"


def test_operation_shape_keeps_shared_layer_metadata():
    ledger = OverrideLedger(InMemoryOverrideLedgerStore())

    operation = ledger.set_field(
        owner_user_id="user-1",
        actor_user_id="user-2",
        feed_id="records",
        entity_id="record-1",
        field_path="status",
        value=OverrideTypedValue("enum", {"value": "in_transit"}),
        observed_canonical_value_json={"value": "loading"},
        created_against_stale_source=True,
        valid_from=_utc(13),
        change_set_id="change-1",
        comment="reviewed",
        metadata_json={"client_operation_id": "client-op-1"},
    )

    assert operation.operation_id
    assert operation.change_set_id == "change-1"
    assert operation.owner_user_id == "user-1"
    assert operation.actor_user_id == "user-2"
    assert operation.feed_id == "records"
    assert operation.entity_id == "record-1"
    assert operation.field_path == "status"
    assert operation.value == OverrideTypedValue("enum", {"value": "in_transit"})
    assert operation.observed_canonical_value_json == {"value": "loading"}
    assert operation.created_against_stale_source is True
    assert operation.comment == "reviewed"
    assert operation.metadata_json == {"client_operation_id": "client-op-1"}
