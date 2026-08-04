from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from hashlib import sha256
from typing import Literal
from uuid import UUID

from .pagination import Page, paginate
from .types import OverrideTypedValue

ReplicatedOverrideOperationType = Literal["set", "remove"]
OverrideFrontierState = Literal["none", "clean", "conflict"]


@dataclass(frozen=True)
class ReplicatedOverrideOperation:
    format_version: int
    operation_id: str
    change_set_id: str
    layer_id: str
    actor_id: str
    feed_id: str
    entity_id: str
    field_path: str
    operation_type: ReplicatedOverrideOperationType
    value: OverrideTypedValue | None
    supersedes_operation_ids: tuple[str, ...]
    removes_operation_ids: tuple[str, ...]
    valid_from: datetime
    valid_to: datetime | None
    recorded_at: datetime
    observed_canonical_value_json: dict[str, object] | None = None
    comment: str | None = None
    metadata_json: dict[str, object] | None = None
    payload_hash: str | None = None


@dataclass(frozen=True)
class ReplicatedOverrideAppendResult:
    operation: ReplicatedOverrideOperation
    accepted: bool


@dataclass(frozen=True)
class OverrideFrontier:
    field_path: str
    state: OverrideFrontierState
    operations: tuple[ReplicatedOverrideOperation, ...]


class InMemoryReplicatedOverrideLedger:
    def __init__(self) -> None:
        self._operations: dict[str, ReplicatedOverrideOperation] = {}

    def append(
        self, operation: ReplicatedOverrideOperation
    ) -> ReplicatedOverrideAppendResult:
        return self.append_batch([operation])[0]

    def append_batch(
        self, operations: Iterable[ReplicatedOverrideOperation]
    ) -> list[ReplicatedOverrideAppendResult]:
        pending = list(operations)
        known = dict(self._operations)
        results: list[ReplicatedOverrideAppendResult] = []

        for operation in pending:
            existing = known.get(operation.operation_id)
            if existing is not None:
                if operation_payload_hash(existing) != operation_payload_hash(
                    operation
                ):
                    raise ValueError("operation ID was reused with different content")
                results.append(ReplicatedOverrideAppendResult(existing, accepted=False))
                continue

            validate_replicated_override_operation(operation, known.values())
            finalized = finalize_replicated_override_operation(operation)
            known[finalized.operation_id] = finalized
            results.append(ReplicatedOverrideAppendResult(finalized, accepted=True))

        self._operations = known
        return results

    def history_for_entity(
        self,
        layer_id: str,
        feed_id: str,
        entity_id: str,
        *,
        cursor: str | None = None,
        limit: int = 100,
    ) -> Page[ReplicatedOverrideOperation]:
        return paginate(
            (
                operation
                for operation in self._operations.values()
                if operation.layer_id == layer_id
                and operation.feed_id == feed_id
                and operation.entity_id == entity_id
            ),
            lambda operation: (operation.recorded_at, operation.operation_id),
            cursor=cursor,
            limit=limit,
        )


def finalize_replicated_override_operation(
    operation: ReplicatedOverrideOperation,
) -> ReplicatedOverrideOperation:
    return replace(operation, payload_hash=operation_payload_hash(operation))


def operation_payload_hash(operation: ReplicatedOverrideOperation) -> str:
    payload = {
        "format_version": operation.format_version,
        "operation_id": operation.operation_id,
        "change_set_id": operation.change_set_id,
        "layer_id": operation.layer_id,
        "actor_id": operation.actor_id,
        "feed_id": operation.feed_id,
        "entity_id": operation.entity_id,
        "field_path": operation.field_path,
        "operation_type": operation.operation_type,
        "value": None
        if operation.value is None
        else {
            "value_type": operation.value.value_type,
            "value_json": operation.value.value_json,
            "unit": operation.value.unit,
        },
        "supersedes_operation_ids": operation.supersedes_operation_ids,
        "removes_operation_ids": operation.removes_operation_ids,
        "valid_from": _timestamp(operation.valid_from),
        "valid_to": _timestamp(operation.valid_to),
        "recorded_at": _timestamp(operation.recorded_at),
        "observed_canonical_value_json": operation.observed_canonical_value_json,
        "comment": operation.comment,
        "metadata_json": operation.metadata_json or {},
    }
    return sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def validate_replicated_override_operation(
    operation: ReplicatedOverrideOperation,
    known_operations: Iterable[ReplicatedOverrideOperation] = (),
) -> None:
    if operation.format_version != 1:
        raise ValueError("unsupported replicated override format version")
    _require_uuid(operation.operation_id, "operation_id")
    _require_uuid(operation.change_set_id, "change_set_id")
    _require_timestamp(operation.valid_from, "valid_from")
    _require_timestamp(operation.recorded_at, "recorded_at")
    if operation.valid_to is not None:
        _require_timestamp(operation.valid_to, "valid_to")
    if operation.operation_type == "set":
        if operation.value is None:
            raise ValueError("set replicated override operations require a value")
        if operation.removes_operation_ids:
            raise ValueError("set replicated override operations cannot remove values")
        if (
            operation.valid_to is not None
            and operation.valid_to <= operation.valid_from
        ):
            raise ValueError("set valid_to must be greater than valid_from")
    elif operation.operation_type == "remove":
        if operation.value is not None:
            raise ValueError(
                "remove replicated override operations cannot carry a value"
            )
        if operation.supersedes_operation_ids:
            raise ValueError(
                "remove replicated override operations cannot supersede values"
            )
        if operation.valid_to is not None:
            raise ValueError(
                "remove replicated override operations cannot have valid_to"
            )
    else:
        raise ValueError("unsupported replicated override operation type")

    references = operation.supersedes_operation_ids + operation.removes_operation_ids
    if len(references) != len(set(references)):
        raise ValueError("operation references must be unique")
    if operation.operation_id in references:
        raise ValueError("operation cannot reference itself")

    known = {known.operation_id: known for known in known_operations}
    for reference in references:
        _require_uuid(reference, "operation reference")
        target = known.get(reference)
        if target is None:
            raise ValueError("operation reference must already exist")
        if _scope(target) != _scope(operation):
            raise ValueError("operation reference must target the same override field")

    if (
        operation.payload_hash is not None
        and operation.payload_hash != operation_payload_hash(operation)
    ):
        raise ValueError("operation payload hash does not match content")


def project_replicated_override_operations(
    operations: Iterable[ReplicatedOverrideOperation], valid_at: datetime
) -> dict[str, OverrideFrontier]:
    _require_timestamp(valid_at, "valid_at")
    grouped: dict[str, list[ReplicatedOverrideOperation]] = {}
    for operation in operations:
        if operation.valid_from <= valid_at:
            grouped.setdefault(operation.field_path, []).append(operation)

    frontiers: dict[str, OverrideFrontier] = {}
    for field_path, field_operations in grouped.items():
        removed = {
            reference
            for operation in field_operations
            for reference in operation.supersedes_operation_ids
            + operation.removes_operation_ids
        }
        active = tuple(
            sorted(
                (
                    operation
                    for operation in field_operations
                    if operation.operation_type == "set"
                    and operation.operation_id not in removed
                    and (operation.valid_to is None or valid_at < operation.valid_to)
                ),
                key=lambda operation: operation.operation_id,
            )
        )
        values = {_value_key(operation.value) for operation in active}
        state: OverrideFrontierState = (
            "none" if not active else "clean" if len(values) == 1 else "conflict"
        )
        frontiers[field_path] = OverrideFrontier(field_path, state, active)
    return frontiers


def _scope(operation: ReplicatedOverrideOperation) -> tuple[str, str, str, str]:
    return (
        operation.layer_id,
        operation.feed_id,
        operation.entity_id,
        operation.field_path,
    )


def _timestamp(value: datetime | None) -> str | None:
    return None if value is None else value.astimezone(UTC).isoformat()


def _value_key(value: OverrideTypedValue | None) -> str:
    assert value is not None
    return json.dumps(
        {
            "value_type": value.value_type,
            "value_json": value.value_json,
            "unit": value.unit,
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def _require_uuid(value: str, name: str) -> None:
    try:
        UUID(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a UUID") from exc


def _require_timestamp(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
