from dataclasses import replace
from datetime import UTC, datetime, timezone
from uuid import uuid4

import pytest

from polars_hist_db.overrides import (
    CompositionRevision,
    CorrectionProposal,
    InMemoryOverrideOperationsStore,
    OperationQuery,
    OverrideTypedValue,
    ReplicatedOverrideOperation,
)


def _utc(hour: int) -> datetime:
    return datetime(2026, 7, 13, hour, tzinfo=UTC)


def _operation(
    layer: str, value: str, *, hour: int = 10
) -> ReplicatedOverrideOperation:
    return ReplicatedOverrideOperation(
        1,
        str(uuid4()),
        str(uuid4()),
        layer,
        "user-1",
        "records",
        "record-1",
        "status",
        "set",
        OverrideTypedValue("enum", {"value": value}),
        (),
        (),
        _utc(hour),
        None,
        _utc(hour + 1),
        metadata_json={"generation": 1},
    )


def test_query_composition_correction_and_purge_contract() -> None:
    store = InMemoryOverrideOperationsStore()
    store.create_layer("root", "analysts", "document-root")
    store.create_layer("child", "analysts")
    child = _operation("child", "Loading")
    root = _operation("root", "In Transit")
    store.append(child)
    store.append(root)
    store.add_composition(
        CompositionRevision(str(uuid4()), "root", ("child",), _utc(0), None, _utc(1))
    )

    page = store.query(OperationQuery(layer_id="root", view="history", limit=1))
    projection = store.project("root", _utc(12), _utc(12))["status"]

    assert [operation.operation_id for operation in page.operations] == [
        root.operation_id
    ]
    assert projection.winning_layer_id == "root"
    assert projection.shadowed[0][0] == "child"

    proposal = CorrectionProposal(
        root.operation_id,
        "user-2",
        OverrideTypedValue("enum", {"value": "Delivered"}),
        _utc(10),
        None,
        "confirmed",
    )
    preview = store.preview_correction(proposal, now=_utc(12))
    corrected = store.commit_correction(proposal, preview.frontier_token, now=_utc(12))
    assert corrected.supersedes_operation_ids == (root.operation_id,)

    purge = store.preview_purge(
        "document-root", [root.operation_id], reason="privacy request", now=_utc(13)
    )
    result = store.execute_purge(
        purge,
        [root.operation_id],
        actor_id="compliance-admin",
        reason="privacy request",
        now=_utc(13),
    )
    assert result.generation == 2
    remaining = store.query(OperationQuery(layer_id="root", view="history"))
    assert all(
        operation.operation_id != root.operation_id
        for operation in remaining.operations
    )
    with pytest.raises(ValueError, match="generation_changed"):
        store.require_generation("document-root", 1)


def test_query_uses_filtered_cursor_pagination() -> None:
    store = InMemoryOverrideOperationsStore()
    store.create_layer("root", "analysts")
    store.create_layer("child", "analysts")
    older = _operation("root", "Loading", hour=9)
    newer = _operation("root", "Delivered", hour=10)
    unrelated = replace(
        _operation("child", "High", hour=11),
        entity_id="record-2",
        field_path="priority",
    )
    for operation in (newer, unrelated, older):
        store.append(operation)

    first = store.operations_for_layer("root", limit=1)
    second = store.operations_for_layer("root", cursor=first.next_cursor, limit=1)

    assert [operation.operation_id for operation in first.operations] == [
        newer.operation_id
    ]
    assert first.next_cursor is not None
    assert [operation.operation_id for operation in second.operations] == [
        older.operation_id
    ]
    assert second.next_cursor is None
    filtered = store.query(
        OperationQuery(entity_id="record-2", field_path="priority", view="history")
    )
    assert [operation.operation_id for operation in filtered.operations] == [
        unrelated.operation_id
    ]
    bounded = store.query(
        OperationQuery(
            layer_id="root",
            view="history",
            recorded_from=_utc(10),
            recorded_to=_utc(11),
        )
    )
    assert [operation.operation_id for operation in bounded.operations] == [
        older.operation_id
    ]


def test_composition_rejects_cross_group_cycles_and_duplicate_reachability() -> None:
    store = InMemoryOverrideOperationsStore()
    for layer in ("a", "b", "c"):
        store.create_layer(layer, "one")
    store.create_layer("outside", "two")
    store.add_composition(
        CompositionRevision(str(uuid4()), "b", ("c",), _utc(0), None, _utc(1))
    )

    with pytest.raises(ValueError, match="owning group"):
        store.add_composition(
            CompositionRevision(str(uuid4()), "a", ("outside",), _utc(0), None, _utc(2))
        )
    with pytest.raises(ValueError, match="duplicate reachable"):
        store.add_composition(
            CompositionRevision(str(uuid4()), "a", ("b", "c"), _utc(0), None, _utc(2))
        )

    store.add_composition(
        CompositionRevision(str(uuid4()), "a", ("b",), _utc(0), None, _utc(2))
    )
    with pytest.raises(ValueError, match="cycle"):
        store.add_composition(
            CompositionRevision(str(uuid4()), "c", ("a",), _utc(13), None, _utc(13))
        )


def test_composition_validates_ancestors_affected_by_a_child_revision() -> None:
    store = InMemoryOverrideOperationsStore()
    for layer in ("root", "left", "right"):
        store.create_layer(layer, "one")
    store.add_composition(
        CompositionRevision(
            str(uuid4()), "root", ("left", "right"), _utc(0), None, _utc(1)
        )
    )

    with pytest.raises(ValueError, match="duplicate reachable"):
        store.add_composition(
            CompositionRevision(
                str(uuid4()), "left", ("right",), _utc(0), None, _utc(2)
            )
        )


def test_composition_revision_can_immutably_replace_an_open_schedule() -> None:
    store = InMemoryOverrideOperationsStore()
    for layer in ("root", "left", "right"):
        store.create_layer(layer, "one")
    original = CompositionRevision(
        str(uuid4()), "root", ("left",), _utc(0), None, _utc(1)
    )
    store.add_composition(original)
    revised = CompositionRevision(
        str(uuid4()),
        "root",
        ("right",),
        _utc(0),
        None,
        _utc(2),
        original.revision_id,
    )
    store.add_composition(revised)

    assert store.composition_order("root", _utc(12), _utc(1)) == ("root", "left")
    assert store.composition_order("root", _utc(12), _utc(2)) == ("root", "right")


def test_correction_token_rejects_a_changed_frontier() -> None:
    store = InMemoryOverrideOperationsStore()
    store.create_layer("root", "analysts")
    original = _operation("root", "Loading")
    store.append(original)
    proposal = CorrectionProposal(
        original.operation_id,
        "user-2",
        OverrideTypedValue("enum", {"value": "Delivered"}),
        _utc(10),
        None,
    )
    preview = store.preview_correction(proposal, now=_utc(12))
    store.append(replace(_operation("root", "Scheduled"), valid_from=_utc(10)))

    with pytest.raises(ValueError, match="stale_frontier"):
        store.commit_correction(proposal, preview.frontier_token, now=_utc(12))


def test_backdated_correction_reports_a_removed_canonical_gap() -> None:
    store = InMemoryOverrideOperationsStore()
    store.create_layer("root", "analysts")
    original = _operation("root", "Loading", hour=10)
    store.append(original)

    preview = store.preview_correction(
        CorrectionProposal(
            original.operation_id,
            "user-2",
            OverrideTypedValue("enum", {"value": "Scheduled"}),
            _utc(8),
            None,
        ),
        now=_utc(12),
    )

    assert preview.canonical_gaps_introduced == ()
    assert preview.canonical_gaps_removed == ((_utc(8), None),)
