from dataclasses import replace
from datetime import UTC, datetime, timezone
from typing import Literal
from uuid import uuid4

import pytest

from polars_hist_db.overrides import (
    InMemoryReplicatedOverrideLedger,
    OverrideTypedValue,
    ReplicatedOverrideOperation,
    project_replicated_override_operations,
)


def _utc(hour: int) -> datetime:
    return datetime(2026, 7, 12, hour, tzinfo=UTC)


def _operation(
    *,
    operation_type: Literal["set", "remove"] = "set",
    value: str | None = "scheduled",
    supersedes: tuple[str, ...] = (),
    removes: tuple[str, ...] = (),
    valid_from: datetime | None = None,
    valid_to: datetime | None = None,
) -> ReplicatedOverrideOperation:
    return ReplicatedOverrideOperation(
        format_version=1,
        operation_id=str(uuid4()),
        change_set_id=str(uuid4()),
        layer_id="team",
        actor_id="user-1",
        feed_id="records",
        entity_id="record-1",
        field_path="status",
        operation_type=operation_type,
        value=None if value is None else OverrideTypedValue("enum", {"value": value}),
        supersedes_operation_ids=supersedes,
        removes_operation_ids=removes,
        valid_from=valid_from or _utc(10),
        valid_to=valid_to,
        recorded_at=_utc(11),
    )


def test_concurrent_values_remain_a_conflict_in_any_delivery_order():
    left = _operation(value="loading")
    right = _operation(value="in_transit")

    forward = project_replicated_override_operations([left, right], _utc(12))
    reverse = project_replicated_override_operations([right, left], _utc(12))

    assert forward["status"].state == "conflict"
    assert forward == reverse
    assert {operation.operation_id for operation in forward["status"].operations} == {
        left.operation_id,
        right.operation_id,
    }


def test_equal_values_are_clean_and_replacement_or_remove_only_affect_references():
    original = _operation(value="loading")
    concurrent = _operation(value="loading")
    replacement = _operation(
        value="in_transit", supersedes=(original.operation_id,), valid_from=_utc(13)
    )
    removal = _operation(
        operation_type="remove",
        value=None,
        removes=(original.operation_id,),
        valid_from=_utc(14),
    )

    assert (
        project_replicated_override_operations([original, concurrent], _utc(12))[
            "status"
        ].state
        == "clean"
    )
    after_replace = project_replicated_override_operations(
        [original, concurrent, replacement], _utc(13)
    )["status"]
    after_remove = project_replicated_override_operations(
        [original, concurrent, replacement, removal], _utc(14)
    )["status"]

    assert {operation.operation_id for operation in after_replace.operations} == {
        concurrent.operation_id,
        replacement.operation_id,
    }
    assert after_replace.state == "conflict"
    assert {operation.operation_id for operation in after_remove.operations} == {
        concurrent.operation_id,
        replacement.operation_id,
    }


def test_ledger_validates_references_and_exact_replay_without_partial_append():
    ledger = InMemoryReplicatedOverrideLedger()
    original = _operation(value="loading")
    replacement = _operation(value="in_transit", supersedes=(original.operation_id,))

    first, second = ledger.append_batch([original, replacement])

    assert first.accepted is True
    assert second.accepted is True
    assert ledger.append(original).accepted is False

    with pytest.raises(ValueError, match="different content"):
        ledger.append(replace(original, comment="changed"))
    with pytest.raises(ValueError, match="must already exist"):
        ledger.append(_operation(supersedes=(str(uuid4()),)))
    invalid = _operation(supersedes=(str(uuid4()),))
    uncommitted = _operation(value="delayed")
    with pytest.raises(ValueError, match="must already exist"):
        ledger.append_batch([uncommitted, invalid])

    first_page = ledger.history_for_entity("team", "records", "record-1", limit=1)
    second_page = ledger.history_for_entity(
        "team",
        "records",
        "record-1",
        cursor=first_page.next_cursor,
        limit=1,
    )
    assert {
        operation.operation_id for operation in first_page.items + second_page.items
    } == {original.operation_id, replacement.operation_id}
    assert first_page.next_cursor is not None
    assert second_page.next_cursor is None
