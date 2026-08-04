from __future__ import annotations

import json
from bisect import bisect_left, bisect_right, insort
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Literal, Protocol
from uuid import uuid4

from .replicated import (
    OverrideFrontier,
    ReplicatedOverrideOperation,
    finalize_replicated_override_operation,
    project_replicated_override_operations,
)
from .types import OverrideTypedValue

OverrideView = Literal["active", "upcoming", "history"]


@dataclass(frozen=True)
class OperationQuery:
    layer_id: str | None = None
    entity_id: str | None = None
    field_path: str | None = None
    actor_id: str | None = None
    operation_types: frozenset[str] = frozenset()
    valid_at: datetime | None = None
    known_at: datetime | None = None
    recorded_from: datetime | None = None
    recorded_to: datetime | None = None
    source_drift: bool | None = None
    view: OverrideView = "active"
    cursor: str | None = None
    limit: int = 100


@dataclass(frozen=True)
class OperationPage:
    operations: tuple[ReplicatedOverrideOperation, ...]
    next_cursor: str | None


@dataclass(frozen=True)
class OverrideLayer:
    layer_id: str
    owning_group: str
    document_id: str
    generation: int = 1


@dataclass(frozen=True)
class CompositionRevision:
    revision_id: str
    layer_id: str
    child_layer_ids: tuple[str, ...]
    valid_from: datetime
    valid_to: datetime | None
    recorded_at: datetime
    supersedes_revision_id: str | None = None


@dataclass(frozen=True)
class EffectiveFieldProjection:
    field_path: str
    frontier: OverrideFrontier
    winning_layer_id: str
    precedence_path: tuple[str, ...]
    shadowed: tuple[tuple[str, OverrideFrontier], ...]


@dataclass(frozen=True)
class CorrectionProposal:
    target_operation_id: str
    actor_id: str
    value: OverrideTypedValue | None
    valid_from: datetime
    valid_to: datetime | None
    comment: str | None = None


@dataclass(frozen=True)
class CorrectionPreview:
    changed_intervals: tuple[tuple[datetime, datetime | None], ...]
    superseded_operation_ids: tuple[str, ...]
    canonical_gaps_introduced: tuple[tuple[datetime, datetime | None], ...]
    canonical_gaps_removed: tuple[tuple[datetime, datetime | None], ...]
    conflicts_created: int
    conflicts_resolved: int
    frontier_token: str
    expires_at: datetime


@dataclass(frozen=True)
class PurgePreview:
    document_id: str
    generation: int
    operation_count: int
    token: str
    expires_at: datetime


@dataclass(frozen=True)
class PurgeResult:
    document_id: str
    old_generation: int
    generation: int
    erased_count: int
    rebuilt_count: int
    tombstone_count: int
    completed_at: datetime


class OverrideOperationsStore(Protocol):
    def query(self, query: OperationQuery) -> OperationPage: ...

    def project(
        self,
        layer_id: str,
        valid_at: datetime,
        known_at: datetime,
        *,
        feed_id: str | None = None,
        entity_id: str | None = None,
    ) -> dict[str, EffectiveFieldProjection]: ...


class InMemoryOverrideOperationsStore:
    """Reference implementation used to keep backend implementations equivalent."""

    def __init__(self) -> None:
        self._operations: dict[str, ReplicatedOverrideOperation] = {}
        self._by_layer: dict[str, set[str]] = defaultdict(set)
        self._by_entity: dict[str, set[str]] = defaultdict(set)
        self._by_field: dict[str, set[str]] = defaultdict(set)
        self._by_recorded: list[tuple[datetime, str]] = []
        self._layers: dict[str, OverrideLayer] = {}
        self._compositions: list[CompositionRevision] = []
        self._purge_metadata: list[dict[str, object]] = []

    def create_layer(
        self,
        layer_id: str,
        owning_group: str,
        document_id: str | None = None,
        generation: int = 1,
    ) -> OverrideLayer:
        if not layer_id or not owning_group:
            raise ValueError("layer_id and owning_group are required")
        if layer_id in self._layers:
            raise ValueError("layer already exists")
        layer = OverrideLayer(
            layer_id, owning_group, document_id or layer_id, generation
        )
        self._layers[layer_id] = layer
        return layer

    def append(self, operation: ReplicatedOverrideOperation) -> None:
        layer = self._require_layer(operation.layer_id)
        generation = int(str((operation.metadata_json or {}).get("generation", 1)))
        if generation != layer.generation:
            raise ValueError("generation_changed")
        existing = self._operations.get(operation.operation_id)
        finalized = finalize_replicated_override_operation(operation)
        if existing is not None:
            if existing.payload_hash != finalized.payload_hash:
                raise ValueError("operation ID was reused with different content")
            return
        self._operations[operation.operation_id] = finalized
        self._by_layer[finalized.layer_id].add(finalized.operation_id)
        self._by_entity[finalized.entity_id].add(finalized.operation_id)
        self._by_field[finalized.field_path].add(finalized.operation_id)
        insort(self._by_recorded, _sort_key(finalized))

    def query(self, query: OperationQuery) -> OperationPage:
        if query.limit < 1 or query.limit > 500:
            raise ValueError("limit must be between 1 and 500")
        now = query.valid_at or datetime.now(UTC)
        candidate_ids = self._candidate_ids(
            layer_id=query.layer_id,
            entity_id=query.entity_id,
            field_path=query.field_path,
        )
        cursor = _decode_cursor(query.cursor) if query.cursor else None
        end = len(self._by_recorded)
        if cursor:
            end = min(end, bisect_left(self._by_recorded, cursor))
        if query.known_at:
            end = min(
                end,
                bisect_right(self._by_recorded, (query.known_at, "\U0010ffff")),
            )
        if query.recorded_to:
            end = min(
                end,
                bisect_left(self._by_recorded, (query.recorded_to, "")),
            )
        rows = []
        for index in range(end - 1, -1, -1):
            recorded_at, operation_id = self._by_recorded[index]
            if query.recorded_from and recorded_at < query.recorded_from:
                break
            if candidate_ids is not None and operation_id not in candidate_ids:
                continue
            operation = self._operations[operation_id]
            if self._matches(operation, query, now):
                rows.append(operation)
                if len(rows) > query.limit:
                    break
        page = rows[: query.limit]
        next_cursor = (
            _encode_cursor(_sort_key(page[-1])) if len(rows) > len(page) else None
        )
        return OperationPage(tuple(page), next_cursor)

    def add_composition(self, revision: CompositionRevision) -> None:
        _require_aware(revision.valid_from, "valid_from")
        _require_aware(revision.recorded_at, "recorded_at")
        if revision.valid_to is not None:
            _require_aware(revision.valid_to, "valid_to")
            if revision.valid_to <= revision.valid_from:
                raise ValueError("valid_to must be greater than valid_from")
        self._require_layer(revision.layer_id)
        if len(revision.child_layer_ids) != len(set(revision.child_layer_ids)):
            raise ValueError("composition contains duplicate child layers")
        if revision.supersedes_revision_id is not None:
            superseded = next(
                (
                    item
                    for item in self._compositions
                    if item.revision_id == revision.supersedes_revision_id
                ),
                None,
            )
            if superseded is None or superseded.layer_id != revision.layer_id:
                raise ValueError("superseded composition revision was not found")
        candidate = [*self._compositions, revision]
        effective = _effective_compositions(candidate, revision.recorded_at)
        for existing in effective:
            if existing.revision_id == revision.revision_id:
                continue
            if existing.layer_id == revision.layer_id and _overlaps(
                existing.valid_from,
                existing.valid_to,
                revision.valid_from,
                revision.valid_to,
            ):
                raise ValueError("composition schedules overlap")
        boundaries = {revision.valid_from}
        if revision.valid_to:
            boundaries.add(revision.valid_to)
        boundaries.update(
            boundary
            for item in candidate
            for boundary in (item.valid_from, item.valid_to)
            if boundary is not None
        )
        known_boundaries = {item.recorded_at for item in candidate}
        for boundary in boundaries:
            for known_at in known_boundaries:
                for root_layer_id in self._layers:
                    self._composition_order(
                        root_layer_id, boundary, known_at, candidate
                    )
        self._compositions.append(revision)

    def project(
        self,
        layer_id: str,
        valid_at: datetime,
        known_at: datetime,
        *,
        feed_id: str | None = None,
        entity_id: str | None = None,
    ) -> dict[str, EffectiveFieldProjection]:
        order = self._composition_order(layer_id, valid_at, known_at)
        per_layer = {}
        for current in order:
            per_layer[current] = project_replicated_override_operations(
                (
                    operation
                    for operation in self._candidate_operations(
                        layer_id=current, entity_id=entity_id
                    )
                    if operation.recorded_at <= known_at
                    and (feed_id is None or operation.feed_id == feed_id)
                ),
                valid_at,
            )
        fields = {field for frontiers in per_layer.values() for field in frontiers}
        result: dict[str, EffectiveFieldProjection] = {}
        for field in fields:
            nonempty = [
                (current, per_layer[current][field])
                for current in order
                if field in per_layer[current]
                and per_layer[current][field].state != "none"
            ]
            if nonempty:
                winner, frontier = nonempty[0]
                result[field] = EffectiveFieldProjection(
                    field,
                    frontier,
                    winner,
                    order[: order.index(winner) + 1],
                    tuple(nonempty[1:]),
                )
        return result

    def composition_order(
        self, layer_id: str, valid_at: datetime, known_at: datetime
    ) -> tuple[str, ...]:
        return self._composition_order(layer_id, valid_at, known_at)

    def preview_correction(
        self, proposal: CorrectionProposal, *, now: datetime | None = None
    ) -> CorrectionPreview:
        clock = now or datetime.now(UTC)
        target = self._require_operation(proposal.target_operation_id)
        field_operations = tuple(
            operation
            for operation in self._candidate_operations(
                layer_id=target.layer_id,
                entity_id=target.entity_id,
                field_path=target.field_path,
            )
            if operation.feed_id == target.feed_id
        )
        overlaps = tuple(
            operation.operation_id
            for operation in field_operations
            if operation.operation_type == "set"
            and _overlaps(
                operation.valid_from,
                operation.valid_to,
                proposal.valid_from,
                proposal.valid_to,
            )
        )
        before = project_replicated_override_operations(
            field_operations,
            proposal.valid_from,
        ).get(target.field_path, OverrideFrontier(target.field_path, "none", ()))
        preview_operation = self._correction_operation(
            proposal, target, overlaps, clock
        )
        after = project_replicated_override_operations(
            [
                *field_operations,
                preview_operation,
            ],
            proposal.valid_from,
        )[target.field_path]
        expires_at = clock + timedelta(minutes=5)
        token = _token(proposal, overlaps, target.payload_hash, expires_at)
        interval = ((proposal.valid_from, proposal.valid_to),)
        gaps_introduced = (
            interval if before.state != "none" and after.state == "none" else ()
        )
        gaps_removed = (
            interval if before.state == "none" and after.state != "none" else ()
        )
        return CorrectionPreview(
            interval,
            overlaps,
            gaps_introduced,
            gaps_removed,
            int(before.state != "conflict" and after.state == "conflict"),
            int(before.state == "conflict" and after.state != "conflict"),
            token,
            expires_at,
        )

    def commit_correction(
        self,
        proposal: CorrectionProposal,
        frontier_token: str,
        *,
        now: datetime | None = None,
    ) -> ReplicatedOverrideOperation:
        clock = now or datetime.now(UTC)
        preview = self.preview_correction(proposal, now=clock)
        if not _valid_token(
            frontier_token,
            proposal,
            preview.superseded_operation_ids,
            self._require_operation(proposal.target_operation_id).payload_hash,
            clock,
        ):
            raise ValueError("stale_frontier")
        operation = self._correction_operation(
            proposal,
            self._require_operation(proposal.target_operation_id),
            preview.superseded_operation_ids,
            clock,
        )
        self.append(operation)
        return self._operations[operation.operation_id]

    def preview_purge(
        self,
        document_id: str,
        operation_ids: Iterable[str],
        *,
        reason: str = "",
        now: datetime | None = None,
    ) -> PurgePreview:
        clock = now or datetime.now(UTC)
        layer = next(
            (item for item in self._layers.values() if item.document_id == document_id),
            None,
        )
        if layer is None:
            raise ValueError("document not found")
        targets = tuple(sorted(set(operation_ids)))
        if not targets or any(
            self._require_operation(item).layer_id != layer.layer_id for item in targets
        ):
            raise ValueError("purge targets must belong to the document")
        expires_at = clock + timedelta(minutes=5)
        return PurgePreview(
            document_id,
            layer.generation,
            len(targets),
            _purge_token(document_id, layer.generation, targets, reason, expires_at),
            expires_at,
        )

    def execute_purge(
        self,
        preview: PurgePreview,
        operation_ids: Iterable[str],
        *,
        actor_id: str,
        reason: str,
        now: datetime | None = None,
    ) -> PurgeResult:
        clock = now or datetime.now(UTC)
        targets = tuple(sorted(set(operation_ids)))
        if (
            not reason.strip()
            or preview.expires_at < clock
            or preview.token
            != _purge_token(
                preview.document_id,
                preview.generation,
                targets,
                reason,
                preview.expires_at,
            )
        ):
            raise ValueError("invalid or expired purge confirmation")
        layer = next(
            item
            for item in self._layers.values()
            if item.document_id == preview.document_id
        )
        if layer.generation != preview.generation:
            raise ValueError("generation_changed")
        old = list(self._candidate_operations(layer_id=layer.layer_id))
        survivors = [
            operation for operation in old if operation.operation_id not in targets
        ]
        remap = {operation.operation_id: str(uuid4()) for operation in survivors}
        rebuilt = [
            replace(
                operation,
                operation_id=remap[operation.operation_id],
                supersedes_operation_ids=tuple(
                    remap[item]
                    for item in operation.supersedes_operation_ids
                    if item in remap
                ),
                removes_operation_ids=tuple(
                    remap[item]
                    for item in operation.removes_operation_ids
                    if item in remap
                ),
                metadata_json={
                    **(operation.metadata_json or {}),
                    "generation": layer.generation + 1,
                },
                payload_hash=None,
            )
            for operation in survivors
        ]
        tombstones = [
            ReplicatedOverrideOperation(
                1,
                str(uuid4()),
                str(uuid4()),
                operation.layer_id,
                "system",
                operation.feed_id,
                operation.entity_id,
                operation.field_path,
                "remove",
                None,
                (),
                tuple(
                    remap[item]
                    for item in (
                        operation.supersedes_operation_ids
                        + operation.removes_operation_ids
                    )
                    if item in remap
                ),
                operation.valid_from,
                None,
                clock,
                metadata_json={
                    "generation": layer.generation + 1,
                    "system_tombstone": True,
                },
            )
            for operation in old
            if operation.operation_id in targets
            and any(
                item in remap
                for item in (
                    operation.supersedes_operation_ids + operation.removes_operation_ids
                )
            )
        ]
        for operation in old:
            self._remove(operation)
        self._layers[layer.layer_id] = replace(layer, generation=layer.generation + 1)
        for operation in [*rebuilt, *tombstones]:
            self.append(operation)
        self._purge_metadata.append(
            {
                "document_id": preview.document_id,
                "actor_id": actor_id,
                "reason": reason,
                "erased_count": len(targets),
                "completed_at": clock,
            }
        )
        return PurgeResult(
            preview.document_id,
            layer.generation,
            layer.generation + 1,
            len(targets),
            len(rebuilt),
            len(tombstones),
            clock,
        )

    def require_generation(self, document_id: str, generation: int) -> None:
        layer = next(
            (item for item in self._layers.values() if item.document_id == document_id),
            None,
        )
        if layer is None or layer.generation != generation:
            raise ValueError("generation_changed")

    def operations_for_layer(
        self, layer_id: str, *, cursor: str | None = None, limit: int = 100
    ) -> OperationPage:
        return self.query(
            OperationQuery(
                layer_id=layer_id, view="history", cursor=cursor, limit=limit
            )
        )

    def _candidate_ids(
        self,
        *,
        layer_id: str | None = None,
        entity_id: str | None = None,
        field_path: str | None = None,
    ) -> set[str] | None:
        candidates = None
        for value, index in (
            (layer_id, self._by_layer),
            (entity_id, self._by_entity),
            (field_path, self._by_field),
        ):
            if value:
                candidates = (
                    set(index.get(value, ()))
                    if candidates is None
                    else candidates.intersection(index.get(value, ()))
                )
        return candidates

    def _candidate_operations(
        self,
        *,
        layer_id: str | None = None,
        entity_id: str | None = None,
        field_path: str | None = None,
    ) -> Iterable[ReplicatedOverrideOperation]:
        return (
            self._operations[operation_id]
            for operation_id in self._candidate_ids(
                layer_id=layer_id,
                entity_id=entity_id,
                field_path=field_path,
            )
            or ()
        )

    def _remove(self, operation: ReplicatedOverrideOperation) -> None:
        self._operations.pop(operation.operation_id)
        for value, index in (
            (operation.layer_id, self._by_layer),
            (operation.entity_id, self._by_entity),
            (operation.field_path, self._by_field),
        ):
            operation_ids = index[value]
            operation_ids.remove(operation.operation_id)
            if not operation_ids:
                del index[value]
        self._by_recorded.pop(bisect_left(self._by_recorded, _sort_key(operation)))

    def _composition_order(
        self,
        layer_id: str,
        valid_at: datetime,
        known_at: datetime,
        revisions: list[CompositionRevision] | None = None,
    ) -> tuple[str, ...]:
        root = self._require_layer(layer_id)
        items = self._compositions if revisions is None else revisions
        items = list(_effective_compositions(items, known_at))
        order: list[str] = []
        visiting: set[str] = set()

        def visit(current: str) -> None:
            layer = self._require_layer(current)
            if layer.owning_group != root.owning_group:
                raise ValueError("composition layers must share one owning group")
            if current in visiting:
                raise ValueError("composition contains a cycle")
            if current in order:
                raise ValueError("composition contains a duplicate reachable layer")
            visiting.add(current)
            order.append(current)
            active = next(
                (
                    revision
                    for revision in reversed(items)
                    if revision.layer_id == current
                    and revision.recorded_at <= known_at
                    and revision.valid_from <= valid_at
                    and (revision.valid_to is None or valid_at < revision.valid_to)
                ),
                None,
            )
            if active:
                for child in active.child_layer_ids:
                    visit(child)
            visiting.remove(current)

        visit(layer_id)
        return tuple(order)

    def _matches(
        self,
        operation: ReplicatedOverrideOperation,
        query: OperationQuery,
        now: datetime,
    ) -> bool:
        if query.layer_id and operation.layer_id != query.layer_id:
            return False
        if query.entity_id and operation.entity_id != query.entity_id:
            return False
        if query.field_path and operation.field_path != query.field_path:
            return False
        if query.actor_id and operation.actor_id != query.actor_id:
            return False
        if (
            query.operation_types
            and operation.operation_type not in query.operation_types
        ):
            return False
        if query.known_at and operation.recorded_at > query.known_at:
            return False
        if query.recorded_from and operation.recorded_at < query.recorded_from:
            return False
        if query.recorded_to and operation.recorded_at >= query.recorded_to:
            return False
        drift = bool((operation.metadata_json or {}).get("source_drift", False))
        if query.source_drift is not None and drift != query.source_drift:
            return False
        active = operation.valid_from <= now and (
            operation.valid_to is None or now < operation.valid_to
        )
        if query.view == "history":
            return True
        return active if query.view == "active" else operation.valid_from > now

    def _correction_operation(
        self,
        proposal: CorrectionProposal,
        target: ReplicatedOverrideOperation,
        overlaps: tuple[str, ...],
        recorded_at: datetime,
    ) -> ReplicatedOverrideOperation:
        remove = proposal.value is None
        return ReplicatedOverrideOperation(
            1,
            str(uuid4()),
            str(uuid4()),
            target.layer_id,
            proposal.actor_id,
            target.feed_id,
            target.entity_id,
            target.field_path,
            "remove" if remove else "set",
            proposal.value,
            () if remove else overlaps,
            overlaps if remove else (),
            proposal.valid_from,
            None if remove else proposal.valid_to,
            recorded_at,
            target.observed_canonical_value_json,
            proposal.comment,
            {"generation": self._layers[target.layer_id].generation},
        )

    def _require_layer(self, layer_id: str) -> OverrideLayer:
        try:
            return self._layers[layer_id]
        except KeyError as exc:
            raise ValueError("layer not found") from exc

    def _require_operation(self, operation_id: str) -> ReplicatedOverrideOperation:
        try:
            return self._operations[operation_id]
        except KeyError as exc:
            raise ValueError("operation not found") from exc


def _effective_compositions(
    revisions: Iterable[CompositionRevision], known_at: datetime
) -> tuple[CompositionRevision, ...]:
    known = tuple(item for item in revisions if item.recorded_at <= known_at)
    superseded = {
        item.supersedes_revision_id
        for item in known
        if item.supersedes_revision_id is not None
    }
    return tuple(item for item in known if item.revision_id not in superseded)


def _overlaps(
    left_from: datetime,
    left_to: datetime | None,
    right_from: datetime,
    right_to: datetime | None,
) -> bool:
    return (left_to is None or right_from < left_to) and (
        right_to is None or left_from < right_to
    )


def _sort_key(operation: ReplicatedOverrideOperation) -> tuple[datetime, str]:
    return operation.recorded_at, operation.operation_id


def _encode_cursor(value: tuple[datetime, str]) -> str:
    return json.dumps([value[0].isoformat(), value[1]], separators=(",", ":"))


def _decode_cursor(value: str) -> tuple[datetime, str]:
    try:
        recorded_at, operation_id = json.loads(value)
        return datetime.fromisoformat(recorded_at), str(operation_id)
    except Exception as exc:
        raise ValueError("invalid cursor") from exc


def _token(
    proposal: CorrectionProposal,
    overlaps: tuple[str, ...],
    payload_hash: str | None,
    expires_at: datetime,
) -> str:
    payload = [
        proposal.target_operation_id,
        proposal.actor_id,
        proposal.value.value_json if proposal.value else None,
        proposal.valid_from.isoformat(),
        proposal.valid_to.isoformat() if proposal.valid_to else None,
        proposal.comment,
        overlaps,
        payload_hash,
        expires_at.isoformat(),
    ]
    return f"{expires_at.isoformat()}:{sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()}"


def _valid_token(
    token: str,
    proposal: CorrectionProposal,
    overlaps: tuple[str, ...],
    payload_hash: str | None,
    now: datetime,
) -> bool:
    try:
        expires_at = datetime.fromisoformat(token.rsplit(":", 1)[0])
    except ValueError:
        return False
    return expires_at >= now and token == _token(
        proposal, overlaps, payload_hash, expires_at
    )


def _purge_token(
    document_id: str,
    generation: int,
    targets: tuple[str, ...],
    reason: str,
    expires_at: datetime,
) -> str:
    digest = sha256(
        json.dumps(
            [document_id, generation, targets, reason, expires_at.isoformat()]
        ).encode()
    ).hexdigest()
    return f"{expires_at.isoformat()}:{digest}"


def _require_aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
