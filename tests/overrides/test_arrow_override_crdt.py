from collections.abc import Sequence
from contextlib import nullcontext
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

import polars as pl
import pyarrow as pa
import pytest

from polars_hist_db.overrides import (
    ArrowOverrideContractError,
    ArrowOverrideGenerationChanged,
    ArrowOverrideLayerNotFound,
    ArrowOverridePreconditionFailed,
    DocumentAccessStoreConfig,
    InMemoryArrowOverrideOperationStore,
    InMemoryArrowOverrideRepository,
    RepositoryArrowOverrideOperationStore,
    RowGuard,
    arrow_override_operation_schema,
    arrow_override_operations_from_storage,
    arrow_override_storage_frames,
    build_document_access_table_configs,
    decode_arrow_override_acknowledgements,
    decode_arrow_override_operations,
    decode_arrow_override_projection,
    encode_arrow_override_acknowledgements,
    encode_arrow_override_operations,
    encode_arrow_override_projection,
    finalize_arrow_override_operations,
    validate_arrow_override_operations,
)


def _value(kind: str, value: object) -> dict[str, object]:
    result: dict[str, object] = {
        field.name: None
        for field in arrow_override_operation_schema().field("value").type
    }
    result["kind"] = kind
    result[
        f"{kind}_value"
        if kind not in {"string_list", "extension"}
        else f"{kind}_payload"
        if kind == "extension"
        else "string_list_value"
    ] = value
    if kind == "extension":
        result["extension_schema_id"] = "example/v1"
    return result


def _proposal(
    *,
    operation_id: UUID | None = None,
    change_set_id: UUID | None = None,
    entity_id: str = "record-1",
    field_path: str = "status",
    value: dict[str, object] | None = None,
    supersedes: list[bytes] | None = None,
    removes: list[bytes] | None = None,
    operation_type: str = "set",
) -> pa.Table:
    row = {
        "format_version": 1,
        "operation_id": (operation_id or uuid4()).bytes,
        "change_set_id": (change_set_id or uuid4()).bytes,
        "layer_id": None,
        "generation": None,
        "layer_revision": None,
        "feed_id": "records",
        "entity_id": entity_id,
        "field_path": field_path,
        "operation_type": operation_type,
        "value": value if value is not None else _value("string", "open"),
        "unit": None,
        "supersedes_ids": supersedes or [],
        "removes_ids": removes or [],
        "valid_from": datetime(2026, 7, 19, 1, tzinfo=UTC),
        "valid_to": None,
        "observed_value": _value("string", "scheduled"),
        "source_drift": False,
        "comment": None,
        "actor_subject": None,
        "actor_display_name": None,
        "recorded_at": None,
        "payload_hash": None,
    }
    if operation_type == "remove":
        row["value"] = None
        row["observed_value"] = None
    return pa.Table.from_pylist([row], schema=arrow_override_operation_schema())


def test_schema_preserves_native_values_and_ipc_roundtrip() -> None:
    values = [
        _value("string", "ready"),
        _value("boolean", True),
        _value("integer", 42),
        _value("float", 1.25),
        _value("decimal", Decimal("123.456000000000")),
        _value("timestamp", datetime(2026, 7, 19, 1, 2, 3, 456789, tzinfo=UTC)),
        _value("date", date(2026, 7, 19)),
        _value("time", time(1, 2, 3, 456789)),
        _value("duration", timedelta(days=2, seconds=3, microseconds=4)),
        _value("binary", b"typed"),
        _value("string_list", ["a", "b"]),
        _value("extension", b"\x01\x02"),
        _value("null", None),
    ]
    tables = [
        _proposal(entity_id=f"record-{index}", value=value)
        for index, value in enumerate(values)
    ]
    proposed = pa.concat_tables(tables)

    validate_arrow_override_operations(proposed, authority="client")
    decoded = decode_arrow_override_operations(
        encode_arrow_override_operations(proposed)
    )

    assert decoded.equals(proposed)
    assert decoded.schema.field("valid_from").type == pa.timestamp("us", tz="UTC")
    assert decoded.schema.field("value").type.field(
        "decimal_value"
    ).type == pa.decimal128(38, 12)
    assert decoded.schema.field("operation_id").type == pa.binary(16)


def test_finalization_adds_authoritative_fields_and_stable_binary_hash() -> None:
    proposal = _proposal(value=_value("integer", 1))
    layer_id = uuid4()
    recorded_at = datetime(2026, 7, 19, 2, 3, 4, 567890, tzinfo=UTC)

    committed = finalize_arrow_override_operations(
        proposal,
        layer_id=layer_id,
        generation=2,
        layer_revision=7,
        actor_subject="subject-1",
        actor_display_name="Verified Name",
        recorded_at=recorded_at,
    )
    roundtripped = decode_arrow_override_operations(
        encode_arrow_override_operations(committed)
    )

    validate_arrow_override_operations(roundtripped, authority="committed")
    assert roundtripped["layer_id"][0].as_py() == layer_id.bytes
    assert roundtripped["recorded_at"][0].as_py() == recorded_at
    assert len(roundtripped["payload_hash"][0].as_py()) == 32


def test_committed_operations_roundtrip_through_typed_storage_frames() -> None:
    values = [
        _value("string", "ready"),
        _value("boolean", True),
        _value("integer", 42),
        _value("float", 1.25),
        _value("decimal", Decimal("123.456000000000")),
        _value("timestamp", datetime(2026, 7, 19, 1, 2, 3, 456789, tzinfo=UTC)),
        _value("date", date(2026, 7, 19)),
        _value("time", time(1, 2, 3, 456789)),
        _value("duration", timedelta(days=2, seconds=3, microseconds=4)),
        _value("binary", b"typed"),
        _value("string_list", ["a", "b"]),
        _value("extension", b"\x01\x02"),
        _value("null", None),
    ]
    committed = finalize_arrow_override_operations(
        pa.concat_tables(
            [
                _proposal(entity_id=f"record-{index}", value=value)
                for index, value in enumerate(values)
            ]
        ),
        layer_id=uuid4(),
        generation=1,
        layer_revision=1,
        actor_subject="subject-1",
        actor_display_name="Verified Name",
        recorded_at=datetime(2026, 7, 19, 2, tzinfo=UTC),
    )

    operations, references, string_lists = arrow_override_storage_frames(committed)
    restored = arrow_override_operations_from_storage(
        operations, references, string_lists
    )

    assert restored.equals(committed)
    assert operations.schema["value_decimal"] == pl.Decimal(38, 12)
    assert operations.schema["value_float"] == pl.Float64
    assert operations.schema["value_timestamp"] == pl.Datetime("us", "UTC")
    assert operations.schema["value_time"] == pl.Time
    assert string_lists["value"].to_list() == ["a", "b"]


def test_client_cannot_supply_authoritative_fields_or_malformed_values() -> None:
    proposal = _proposal()
    forged = proposal.set_column(
        proposal.schema.get_field_index("actor_subject"),
        "actor_subject",
        pa.array(["forged"], type=pa.string()),
    )
    malformed = _proposal(value={**_value("integer", 1), "string_value": "also set"})
    non_finite = _proposal(value=_value("float", float("inf")))
    missing_scope = proposal.set_column(
        proposal.schema.get_field_index("feed_id"),
        proposal.schema.field("feed_id"),
        pa.array([None], type=pa.string()),
    )

    with pytest.raises(ArrowOverrideContractError, match="authoritative"):
        validate_arrow_override_operations(forged, authority="client")
    with pytest.raises(ArrowOverrideContractError, match="exactly"):
        validate_arrow_override_operations(malformed, authority="client")
    with pytest.raises(ArrowOverrideContractError, match="finite"):
        validate_arrow_override_operations(non_finite, authority="client")
    with pytest.raises(ArrowOverrideContractError, match="required"):
        validate_arrow_override_operations(missing_scope, authority="client")


def test_sync_is_idempotent_and_returns_only_affected_projection() -> None:
    store = InMemoryArrowOverrideOperationStore()
    layer_id = uuid4()
    operation_id = uuid4()
    proposal = _proposal(operation_id=operation_id)
    now = datetime(2026, 7, 19, 2, tzinfo=UTC)
    store.create_layer(layer_id)

    first = store.sync(
        layer_id=layer_id,
        generation=1,
        known_revision=0,
        pending=proposal,
        actor_subject="subject-1",
        actor_display_name=None,
        recorded_at=now,
    )
    replay = store.sync(
        layer_id=layer_id,
        generation=1,
        known_revision=0,
        pending=proposal,
        actor_subject="subject-1",
        actor_display_name=None,
        recorded_at=now + timedelta(seconds=1),
    )

    assert first.revision == replay.revision == 1
    assert first.acknowledgements["status"].to_pylist() == ["accepted"]
    assert first.operations_delta.num_rows == 1
    validate_arrow_override_operations(first.operations_delta, authority="committed")
    assert first.operations_delta["operation_id"].to_pylist() == [
        proposal["operation_id"][0].as_py()
    ]
    assert replay.acknowledgements["status"].to_pylist() == ["duplicate"]
    assert first.projection_delta.num_rows == 1
    assert first.projection_delta["frontier_state"].to_pylist() == ["clean"]
    assert first.projection_delta["winning_operation_ids"].to_pylist() == [
        [operation_id.bytes]
    ]
    assert decode_arrow_override_acknowledgements(
        encode_arrow_override_acknowledgements(first.acknowledgements)
    ).equals(first.acknowledgements)
    assert decode_arrow_override_projection(
        encode_arrow_override_projection(first.projection_delta)
    ).equals(first.projection_delta)


def test_sync_atomically_rejects_a_stale_external_guard() -> None:
    store = InMemoryArrowOverrideOperationStore()
    layer_id = uuid4()
    store.create_layer(layer_id)
    table = build_document_access_table_configs(DocumentAccessStoreConfig())[0]
    store.in_memory_repository.seed_guard_row(
        table, {"document_id": str(layer_id), "status": "active", "revision": 2}
    )

    with pytest.raises(ArrowOverridePreconditionFailed, match="guard"):
        store.sync(
            layer_id=layer_id,
            generation=1,
            known_revision=0,
            pending=_proposal(),
            actor_subject="subject-1",
            actor_display_name=None,
            recorded_at=datetime(2026, 7, 19, 2, tzinfo=UTC),
            guards=(
                RowGuard(
                    table,
                    {"document_id": str(layer_id)},
                    {"status": "active", "revision": 1},
                ),
            ),
        )

    head = store.in_memory_repository.head(layer_id)
    assert head is not None
    assert head.revision == 0


def test_sync_emits_low_cardinality_telemetry(monkeypatch) -> None:
    measurements: list[dict[str, object]] = []

    class Span:
        def __init__(self) -> None:
            self.attributes: dict[str, object] = {}

        def set_attribute(self, name: str, value: object) -> None:
            self.attributes[name] = value

    span = Span()
    monkeypatch.setattr(
        "polars_hist_db.overrides.arrow.arrow_override_sync_span",
        lambda **_kwargs: nullcontext(span),
    )
    monkeypatch.setattr(
        "polars_hist_db.overrides.arrow.record_arrow_override_sync",
        lambda **kwargs: measurements.append(kwargs),
    )
    store = InMemoryArrowOverrideOperationStore()
    layer_id = uuid4()
    store.create_layer(layer_id)

    store.sync(
        layer_id=layer_id,
        generation=1,
        known_revision=0,
        pending=_proposal(),
        actor_subject="subject-1",
        actor_display_name="Verified Name",
        recorded_at=datetime(2026, 7, 19, 2, tzinfo=UTC),
    )

    assert measurements[0]["backend"] == "memory"
    assert measurements[0]["outcome"] == "accepted"
    assert measurements[0]["accepted"] == 1
    assert set(span.attributes) == {
        "override.accepted_rows",
        "override.duplicate_rows",
        "override.projection_rows",
        "override.conflicts",
        "override.revision",
    }


def test_concurrent_values_form_conflict_and_supersession_resolves_it() -> None:
    store = InMemoryArrowOverrideOperationStore()
    layer_id = uuid4()
    left_id, right_id = uuid4(), uuid4()
    now = datetime(2026, 7, 19, 2, tzinfo=UTC)
    store.create_layer(layer_id)
    concurrent = pa.concat_tables(
        [
            _proposal(operation_id=left_id, value=_value("string", "left")),
            _proposal(operation_id=right_id, value=_value("string", "right")),
        ]
    )

    conflict = store.sync(
        layer_id=layer_id,
        generation=1,
        known_revision=0,
        pending=concurrent,
        actor_subject="subject-1",
        actor_display_name=None,
        recorded_at=now,
    )
    resolution = store.sync(
        layer_id=layer_id,
        generation=1,
        known_revision=1,
        pending=_proposal(
            value=_value("string", "resolved"),
            supersedes=[left_id.bytes, right_id.bytes],
        ),
        actor_subject="subject-2",
        actor_display_name=None,
        recorded_at=now + timedelta(seconds=1),
    )

    assert conflict.projection_delta["frontier_state"].to_pylist() == ["conflict"]
    assert conflict.projection_delta["value"].null_count == 1
    assert resolution.projection_delta["frontier_state"].to_pylist() == ["clean"]
    assert resolution.projection_delta["value"][0].as_py()["string_value"] == "resolved"


def test_sync_rejects_reused_ids_bad_references_and_generation_changes() -> None:
    store = InMemoryArrowOverrideOperationStore()
    missing_layer = uuid4()
    layer_id = uuid4()
    operation_id = uuid4()
    now = datetime(2026, 7, 19, 2, tzinfo=UTC)
    with pytest.raises(ArrowOverrideLayerNotFound):
        store.sync(
            layer_id=missing_layer,
            generation=1,
            known_revision=0,
            pending=_proposal(),
            actor_subject="subject-1",
            actor_display_name=None,
            recorded_at=now,
        )
    store.create_layer(layer_id)
    store.sync(
        layer_id=layer_id,
        generation=1,
        known_revision=0,
        pending=_proposal(operation_id=operation_id),
        actor_subject="subject-1",
        actor_display_name=None,
        recorded_at=now,
    )

    with pytest.raises(ArrowOverrideContractError, match="different typed content"):
        store.sync(
            layer_id=layer_id,
            generation=1,
            known_revision=1,
            pending=_proposal(
                operation_id=operation_id, value=_value("string", "changed")
            ),
            actor_subject="subject-1",
            actor_display_name=None,
            recorded_at=now,
        )
    with pytest.raises(ArrowOverrideContractError, match="already exist"):
        store.sync(
            layer_id=layer_id,
            generation=1,
            known_revision=1,
            pending=_proposal(supersedes=[uuid4().bytes]),
            actor_subject="subject-1",
            actor_display_name=None,
            recorded_at=now,
        )

    store.reset_generation(layer_id, 2)
    with pytest.raises(ArrowOverrideGenerationChanged):
        store.sync(
            layer_id=layer_id,
            generation=1,
            known_revision=1,
            pending=_proposal(),
            actor_subject="subject-1",
            actor_display_name=None,
            recorded_at=now,
        )


def test_same_batch_references_are_order_independent_and_cycles_fail() -> None:
    store = InMemoryArrowOverrideOperationStore()
    layer_id = uuid4()
    left_id, right_id = uuid4(), uuid4()
    now = datetime(2026, 7, 19, 2, tzinfo=UTC)
    store.create_layer(layer_id)

    result = store.sync(
        layer_id=layer_id,
        generation=1,
        known_revision=0,
        pending=pa.concat_tables(
            [
                _proposal(
                    operation_id=right_id,
                    value=_value("string", "new"),
                    supersedes=[left_id.bytes],
                ),
                _proposal(operation_id=left_id, value=_value("string", "old")),
            ]
        ),
        actor_subject="subject-1",
        actor_display_name=None,
        recorded_at=now,
    )

    assert result.projection_delta["frontier_state"].to_pylist() == ["clean"]
    assert result.projection_delta["value"][0].as_py()["string_value"] == "new"

    cycle_store = InMemoryArrowOverrideOperationStore()
    cycle_store.create_layer(layer_id)
    with pytest.raises(ArrowOverrideContractError, match="cycles"):
        cycle_store.sync(
            layer_id=layer_id,
            generation=1,
            known_revision=0,
            pending=pa.concat_tables(
                [
                    _proposal(operation_id=left_id, supersedes=[right_id.bytes]),
                    _proposal(operation_id=right_id, supersedes=[left_id.bytes]),
                ]
            ),
            actor_subject="subject-1",
            actor_display_name=None,
            recorded_at=now,
        )


def test_revision_compare_and_swap_is_retried_without_duplicate_commit() -> None:
    class RacingRepository(InMemoryArrowOverrideRepository):
        def __init__(self) -> None:
            super().__init__()
            self.attempts = 0

        def append_if_revision(
            self,
            layer_id: UUID,
            generation: int,
            expected_revision: int,
            committed: pa.Table,
            updated_at: datetime,
            guards: Sequence[RowGuard] = (),
        ) -> bool:
            self.attempts += 1
            if self.attempts == 1:
                return False
            return super().append_if_revision(
                layer_id,
                generation,
                expected_revision,
                committed,
                updated_at,
                guards,
            )

    repository = RacingRepository()
    store = RepositoryArrowOverrideOperationStore(repository)
    layer_id = uuid4()
    repository.create_layer(layer_id)

    result = store.sync(
        layer_id=layer_id,
        generation=1,
        known_revision=0,
        pending=_proposal(),
        actor_subject="subject-1",
        actor_display_name=None,
        recorded_at=datetime(2026, 7, 19, 2, tzinfo=UTC),
    )

    assert repository.attempts == 2
    assert result.revision == 1
    assert result.acknowledgements["status"].to_pylist() == ["accepted"]
