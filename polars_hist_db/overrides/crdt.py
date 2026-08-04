from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime
from hashlib import sha256
from typing import Any

from polars_hist_db.config import TableConfig

from .pagination import Page, paginate
from .replicated import (
    ReplicatedOverrideOperation,
    finalize_replicated_override_operation,
    operation_payload_hash,
    validate_replicated_override_operation,
)
from .types import OverrideTypedValue


@dataclass(frozen=True)
class CrdtDocument:
    document_id: str
    revision: int
    state_vector: bytes
    update: bytes


@dataclass(frozen=True)
class PreparedCrdtCommit:
    document_id: str
    base_revision: int
    source_update_hash: str
    accepted_update: bytes
    accepted_update_hash: str
    state_vector: bytes
    operations: tuple[ReplicatedOverrideOperation, ...]
    is_noop: bool = False
    generation: int = 1


@dataclass(frozen=True)
class CrdtCommitResult:
    document: CrdtDocument | None
    revision: int
    accepted_update: bytes
    accepted: bool
    duplicate: bool = False


# Backwards-compatible name for callers of the original in-memory store.
CrdtAppendResult = CrdtCommitResult


@dataclass(frozen=True)
class RowGuard:
    table_config: TableConfig
    key_values: Mapping[str, object]
    expected_values: Mapping[str, object]


@dataclass(frozen=True)
class AtomicInsert:
    table_config: TableConfig
    row: Mapping[str, object]


@dataclass(frozen=True)
class AtomicUpdate:
    table_config: TableConfig
    key_values: Mapping[str, object]
    expected_values: Mapping[str, object]
    values: Mapping[str, object]


class CrdtRevisionConflict(ValueError):
    pass


class CrdtPreconditionFailed(ValueError):
    pass


def prepare_crdt_update(
    document_id: str,
    current_document: CrdtDocument | None,
    source_update: bytes,
    *,
    actor_id: str,
    recorded_at: datetime,
    authoritative_metadata: Mapping[str, object] | None = None,
) -> PreparedCrdtCommit:
    document = _doc()
    base_revision = 0
    if current_document is not None:
        if current_document.document_id != document_id:
            raise ValueError("CRDT document ID does not match current document")
        document.apply_update(current_document.update)
        base_revision = current_document.revision

    base_state = document.get_state()
    before = _operations(document, actor_id=None, recorded_at=None)
    for operation in before.values():
        validate_replicated_override_operation(operation, before.values())
    document.apply_update(source_update)
    if document.get_state() == base_state:
        return PreparedCrdtCommit(
            document_id=document_id,
            base_revision=base_revision,
            source_update_hash=_hash(source_update),
            accepted_update=b"",
            accepted_update_hash=_hash(b""),
            state_vector=base_state,
            operations=(),
            is_noop=True,
        )

    operations_map = _operations_map(document, create=False)
    after = (
        {}
        if operations_map is None
        else {
            operation_id: _operation_from_yjs(
                operation_id,
                raw,
                actor_id=None if operation_id in before else actor_id,
                recorded_at=None if operation_id in before else recorded_at,
            )
            for operation_id, raw in operations_map.to_py().items()
        }
    )
    if before.keys() - after.keys():
        raise ValueError("published CRDT operations cannot be deleted")
    for operation_id, operation in before.items():
        if operation_payload_hash(after[operation_id]) != operation_payload_hash(
            operation
        ):
            raise ValueError("published CRDT operations cannot be mutated")

    new_operations: list[ReplicatedOverrideOperation] = []
    known_operations = list(before.values())
    for operation_id in sorted(after.keys() - before.keys()):
        operation = after[operation_id]
        if authoritative_metadata:
            operation = replace(
                operation,
                metadata_json={
                    **(operation.metadata_json or {}),
                    **authoritative_metadata,
                },
            )
        validate_replicated_override_operation(operation, known_operations)
        finalized = finalize_replicated_override_operation(operation)
        new_operations.append(finalized)
        known_operations.append(finalized)
        final_operations_map = _operations_map(document, create=True)
        assert final_operations_map is not None
        final_operations_map[operation_id] = _operation_to_yjs(finalized)

    accepted_update = document.get_update(base_state)
    return PreparedCrdtCommit(
        document_id=document_id,
        base_revision=base_revision,
        source_update_hash=_hash(source_update),
        accepted_update=accepted_update,
        accepted_update_hash=_hash(accepted_update),
        state_vector=document.get_state(),
        operations=tuple(new_operations),
    )


def operation_source_update(operation: ReplicatedOverrideOperation) -> bytes:
    """Encode one provisional operation for the normal authoritative commit path."""
    document = _doc()
    operations = _operations_map(document, create=True)
    assert operations is not None
    operations[operation.operation_id] = {
        key: value
        for key, value in _operation_to_yjs(operation).items()
        if key not in {"actor_id", "recorded_at", "payload_hash"}
    }
    return document.get_update()


def prepare_crdt_generation(
    document_id: str,
    operations: Sequence[ReplicatedOverrideOperation],
    *,
    generation: int,
) -> PreparedCrdtCommit:
    """Build a sanitized replacement generation after the prior one is erased."""
    document = _doc()
    operations_map = _operations_map(document, create=True)
    assert operations_map is not None
    finalized = tuple(
        finalize_replicated_override_operation(item) for item in operations
    )
    for operation in finalized:
        operations_map[operation.operation_id] = _operation_to_yjs(operation)
    update = document.get_update()
    return PreparedCrdtCommit(
        document_id=document_id,
        base_revision=0,
        source_update_hash=_hash(update),
        accepted_update=update,
        accepted_update_hash=_hash(update),
        state_vector=document.get_state(),
        operations=finalized,
        generation=generation,
    )


class InMemoryCrdtDocumentStore:
    """Reference prepared-commit store for tests and embedded applications."""

    def __init__(self) -> None:
        self._documents: dict[str, Any] = {}
        self._revisions: dict[str, int] = {}
        self._source_results: dict[str, dict[str, CrdtCommitResult]] = {}
        self._operations: dict[str, dict[str, ReplicatedOverrideOperation]] = {}
        self._snapshots: dict[str, CrdtDocument] = {}
        self._rows: dict[
            tuple[str, str, tuple[tuple[str, object], ...]], dict[str, object]
        ] = {}

    def append_update(
        self,
        document_id: str,
        update: bytes,
        *,
        expected_revision: int | None = None,
    ) -> CrdtAppendResult:
        prior = self._source_results.get(document_id, {}).get(_hash(update))
        if prior is not None:
            return replace(prior, accepted=False, duplicate=True)
        current = self.load_document(document_id)
        if expected_revision is not None and expected_revision != (
            0 if current is None else current.revision
        ):
            raise CrdtRevisionConflict(
                "CRDT document revision does not match expected revision"
            )
        prepared = prepare_crdt_update(
            document_id,
            current,
            update,
            actor_id="system",
            recorded_at=datetime.now().astimezone(),
        )
        return self.commit(prepared)

    def commit(
        self,
        prepared: PreparedCrdtCommit,
        *,
        guards: Sequence[RowGuard] = (),
        inserts: Sequence[AtomicInsert] = (),
        updates: Sequence[AtomicUpdate] = (),
    ) -> CrdtCommitResult:
        prior = self._source_results.get(prepared.document_id, {}).get(
            prepared.source_update_hash
        )
        if prior is not None:
            return replace(prior, accepted=False, duplicate=True)

        current = self.load_document(prepared.document_id)
        revision = 0 if current is None else current.revision
        if revision != prepared.base_revision:
            raise CrdtRevisionConflict(
                "CRDT document revision does not match prepared revision"
            )
        if prepared.is_noop:
            return CrdtCommitResult(
                current, revision, b"", accepted=False, duplicate=True
            )

        self._validate_guards(guards)
        self._validate_updates(updates)
        insert_keys = [
            self._row_key(insert.table_config, insert.row) for insert in inserts
        ]
        if len(insert_keys) != len(set(insert_keys)) or any(
            key in self._rows for key in insert_keys
        ):
            raise ValueError("atomic insert conflicts with an existing row")
        known_operations = self._operations.setdefault(prepared.document_id, {})
        for operation in prepared.operations:
            existing = known_operations.get(operation.operation_id)
            if existing is not None and operation_payload_hash(
                existing
            ) != operation_payload_hash(operation):
                raise ValueError("operation ID was reused with different content")

        document = self._documents.setdefault(prepared.document_id, _doc())
        if _hash(prepared.accepted_update) != prepared.accepted_update_hash:
            raise ValueError("accepted CRDT update hash does not match update")
        document.apply_update(prepared.accepted_update)
        self._revisions[prepared.document_id] = revision + 1
        known_operations.update(
            {operation.operation_id: operation for operation in prepared.operations}
        )
        for insert, key in zip(inserts, insert_keys, strict=True):
            self._rows[key] = dict(insert.row)
        self._apply_updates(updates)

        result = CrdtCommitResult(
            self.load_document(prepared.document_id),
            revision + 1,
            prepared.accepted_update,
            accepted=True,
        )
        self._source_results.setdefault(prepared.document_id, {})[
            prepared.source_update_hash
        ] = result
        return result

    def _validate_updates(self, updates: Sequence[AtomicUpdate]) -> None:
        for update in updates:
            key = self._row_key(update.table_config, update.key_values)
            row = self._rows.get(key)
            if row is None or any(
                row.get(name) != value for name, value in update.expected_values.items()
            ):
                raise CrdtPreconditionFailed("CRDT atomic update no longer matches")

    def _apply_updates(self, updates: Sequence[AtomicUpdate]) -> None:
        for update in updates:
            key = self._row_key(update.table_config, update.key_values)
            row = self._rows[key]
            row.update(update.values)

    def load_document(self, document_id: str) -> CrdtDocument | None:
        document = self._documents.get(document_id)
        if document is None:
            return None
        return CrdtDocument(
            document_id=document_id,
            revision=self._revisions[document_id],
            state_vector=document.get_state(),
            update=document.get_update(),
        )

    def diff(self, document_id: str, state_vector: bytes) -> bytes:
        document = self._documents.get(document_id)
        if document is None:
            raise KeyError(f"unknown CRDT document: {document_id}")
        return document.get_update(state_vector)

    def write_snapshot(
        self, document_id: str, expected_revision: int | None = None
    ) -> CrdtDocument:
        document = self.load_document(document_id)
        if document is None:
            raise KeyError(f"unknown CRDT document: {document_id}")
        if expected_revision is not None and expected_revision != document.revision:
            raise CrdtRevisionConflict(
                "CRDT document revision does not match expected revision"
            )
        self._snapshots[document_id] = document
        return document

    def snapshot(self, document_id: str) -> CrdtDocument | None:
        return self._snapshots.get(document_id)

    def seed_row(self, table_config: TableConfig, row: Mapping[str, object]) -> None:
        self._rows[self._row_key(table_config, row)] = dict(row)

    def projected_operations(
        self,
        document_id: str,
        *,
        cursor: str | None = None,
        limit: int = 100,
    ) -> Page[ReplicatedOverrideOperation]:
        return paginate(
            self._operations.get(document_id, {}).values(),
            lambda operation: (operation.recorded_at, operation.operation_id),
            cursor=cursor,
            limit=limit,
        )

    def _validate_guards(self, guards: Sequence[RowGuard]) -> None:
        for guard in guards:
            row = self._rows.get(self._row_key(guard.table_config, guard.key_values))
            if row is None or any(
                row.get(column) != value
                for column, value in guard.expected_values.items()
            ):
                raise CrdtPreconditionFailed("CRDT commit guard no longer matches")

    @staticmethod
    def _row_key(
        table_config: TableConfig, row: Mapping[str, object]
    ) -> tuple[str, str, tuple[tuple[str, object], ...]]:
        key_columns = tuple(table_config.primary_keys)
        if not key_columns or any(column not in row for column in key_columns):
            raise ValueError("atomic rows require every configured primary key")
        return (
            table_config.schema,
            table_config.name,
            tuple((column, row[column]) for column in key_columns),
        )


def _operations(
    document: Any,
    *,
    actor_id: str | None,
    recorded_at: datetime | None,
) -> dict[str, ReplicatedOverrideOperation]:
    result: dict[str, ReplicatedOverrideOperation] = {}
    operations_map = _operations_map(document, create=False)
    if operations_map is None:
        return result
    for operation_id, raw in operations_map.to_py().items():
        result[operation_id] = _operation_from_yjs(
            operation_id,
            raw,
            actor_id=actor_id,
            recorded_at=recorded_at,
        )
    return result


def _operations_map(document: Any, *, create: bool) -> Any | None:
    from pycrdt import Map

    operations = document.get("operations", type=Map)
    if operations is None and create:
        operations = Map()
        document["operations"] = operations
    return operations


def _operation_from_yjs(
    operation_id: str,
    raw: object,
    *,
    actor_id: str | None,
    recorded_at: datetime | None,
) -> ReplicatedOverrideOperation:
    if not isinstance(raw, dict):
        raise TypeError("CRDT operation entries must be objects")
    provisional = actor_id is not None and recorded_at is not None
    if provisional and any(
        raw.get(field) is not None
        for field in ("actor_id", "recorded_at", "payload_hash")
    ):
        raise ValueError("client CRDT operations cannot set authoritative fields")
    value_raw = raw.get("value")
    if value_raw is None:
        value = None
    elif isinstance(value_raw, dict) and isinstance(value_raw.get("value_json"), dict):
        value = OverrideTypedValue(
            value_type=_string(value_raw.get("value_type"), "value.value_type"),
            value_json=dict(value_raw["value_json"]),
            unit=_optional_string(value_raw.get("unit"), "value.unit"),
        )
    else:
        raise ValueError("CRDT operation value must be an object")
    declared_id = _string(raw.get("operation_id"), "operation_id")
    if declared_id != operation_id:
        raise ValueError("CRDT operation map key must match operation_id")
    resolved_actor_id = (
        actor_id if provisional else _string(raw.get("actor_id"), "actor_id")
    )
    resolved_recorded_at = (
        recorded_at
        if provisional
        else _timestamp(raw.get("recorded_at"), "recorded_at")
    )
    assert resolved_actor_id is not None
    assert resolved_recorded_at is not None
    return ReplicatedOverrideOperation(
        format_version=_integer(raw.get("format_version"), "format_version"),
        operation_id=operation_id,
        change_set_id=_string(raw.get("change_set_id"), "change_set_id"),
        layer_id=_string(raw.get("layer_id"), "layer_id"),
        actor_id=resolved_actor_id,
        feed_id=_string(raw.get("feed_id"), "feed_id"),
        entity_id=_string(raw.get("entity_id"), "entity_id"),
        field_path=_string(raw.get("field_path"), "field_path"),
        operation_type=_string(raw.get("operation_type"), "operation_type"),  # type: ignore[arg-type]
        value=value,
        supersedes_operation_ids=_string_tuple(raw.get("supersedes_operation_ids")),
        removes_operation_ids=_string_tuple(raw.get("removes_operation_ids")),
        valid_from=_timestamp(raw.get("valid_from"), "valid_from"),
        valid_to=_optional_timestamp(raw.get("valid_to"), "valid_to"),
        recorded_at=resolved_recorded_at,
        observed_canonical_value_json=_optional_dict(
            raw.get("observed_canonical_value_json"), "observed_canonical_value_json"
        ),
        comment=_optional_string(raw.get("comment"), "comment"),
        metadata_json=_optional_dict(raw.get("metadata_json"), "metadata_json"),
        payload_hash=None
        if provisional
        else _string(raw.get("payload_hash"), "payload_hash"),
    )


def _operation_to_yjs(operation: ReplicatedOverrideOperation) -> dict[str, object]:
    return {
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
        "supersedes_operation_ids": list(operation.supersedes_operation_ids),
        "removes_operation_ids": list(operation.removes_operation_ids),
        "valid_from": operation.valid_from.isoformat(),
        "valid_to": None
        if operation.valid_to is None
        else operation.valid_to.isoformat(),
        "recorded_at": operation.recorded_at.isoformat(),
        "observed_canonical_value_json": operation.observed_canonical_value_json,
        "comment": operation.comment,
        "metadata_json": operation.metadata_json or {},
        "payload_hash": operation.payload_hash,
    }


def _hash(value: bytes) -> str:
    return sha256(value).hexdigest()


def _string(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"CRDT operation {name} must be a string")
    return value


def _optional_string(value: object, name: str) -> str | None:
    if value is None:
        return None
    return _string(value, name)


def _integer(value: object, name: str) -> int:
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if not isinstance(value, int):
        raise TypeError(f"CRDT operation {name} must be an integer")
    return value


def _string_tuple(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError("CRDT operation references must be string arrays")
    return tuple(value)


def _optional_dict(value: object, name: str) -> dict[str, object] | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise TypeError(f"CRDT operation {name} must be an object")
    return dict(value)


def _timestamp(value: object, name: str) -> datetime:
    if not isinstance(value, str):
        raise TypeError(f"CRDT operation {name} must be an ISO timestamp")
    try:
        return datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"CRDT operation {name} must be an ISO timestamp") from exc


def _optional_timestamp(value: object, name: str) -> datetime | None:
    return None if value is None else _timestamp(value, name)


def _doc() -> Any:
    try:
        from pycrdt import Doc
    except ImportError as exc:
        raise RuntimeError(
            "CRDT support requires the 'crdt' extra: pip install polars-hist-db[crdt]"
        ) from exc
    return Doc()
