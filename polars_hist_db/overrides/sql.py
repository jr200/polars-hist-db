from __future__ import annotations

import base64
import json
from collections.abc import Sequence
from dataclasses import asdict, replace
from datetime import UTC, datetime
from hashlib import sha256
from typing import Any

from sqlalchemy import Connection, MetaData, Table, and_, or_, select
from sqlalchemy.exc import IntegrityError

from polars_hist_db.config import TableConfig

from .access import (
    AccessDocument,
    AccessGrant,
    AccessGrantInput,
    AccessMutationResult,
    DocumentAccessError,
    DocumentArchived,
    DocumentNotFound,
    DocumentRevisionConflict,
    IdempotencyConflict,
    _create_payload,
    _existing_result,
    _normalized,
    _payload,
    _require_time,
)
from .config import (
    build_crdt_document_table_config,
    build_crdt_update_table_config,
    build_document_access_table_configs,
    build_layer_composition_table_config,
    build_override_table_config,
)
from .crdt import (
    AtomicInsert,
    AtomicUpdate,
    CrdtCommitResult,
    CrdtDocument,
    CrdtPreconditionFailed,
    CrdtRevisionConflict,
    PreparedCrdtCommit,
    RowGuard,
    _doc,
)
from .operations import CompositionRevision
from .pagination import (
    Page,
    decode_cursor,
    encode_cursor,
    page_from_items,
    validate_limit,
)
from .types import (
    CrdtDocumentStoreConfig,
    DocumentAccessStoreConfig,
    LayerCompositionStoreConfig,
    OverrideLedgerConfig,
)


class MariaDbLayerCompositionStore:
    def __init__(
        self, connection: Connection, config: LayerCompositionStoreConfig
    ) -> None:
        self.connection = connection
        self.table = _table(connection, build_layer_composition_table_config(config))

    def append(self, revision: CompositionRevision, actor_id: str) -> None:
        self.connection.execute(
            self.table.insert().values(
                revision_id=revision.revision_id,
                layer_id=revision.layer_id,
                child_layer_ids_json=_json(list(revision.child_layer_ids)),
                valid_from=revision.valid_from,
                valid_to=revision.valid_to,
                recorded_at=revision.recorded_at,
                actor_id=actor_id,
                supersedes_revision_id=revision.supersedes_revision_id,
            )
        )

    def revisions(
        self,
        layer_id: str | None = None,
        *,
        cursor: str | None = None,
        limit: int = 100,
    ) -> Page[CompositionRevision]:
        validate_limit(limit)
        query = select(self.table)
        if layer_id is not None:
            query = query.where(self.table.c.layer_id == layer_id)
        if cursor is not None:
            recorded_at, revision_id = decode_cursor(cursor)
            query = query.where(
                or_(
                    self.table.c.recorded_at > recorded_at,
                    and_(
                        self.table.c.recorded_at == recorded_at,
                        self.table.c.revision_id > revision_id,
                    ),
                )
            )
        rows = list(
            self.connection.execute(
                query.order_by(
                    self.table.c.recorded_at, self.table.c.revision_id
                ).limit(limit + 1)
            ).mappings()
        )
        revisions = tuple(_composition_revision(row) for row in rows[:limit])
        next_cursor = (
            encode_cursor((revisions[-1].recorded_at, revisions[-1].revision_id))
            if len(rows) > limit
            else None
        )
        return Page(revisions, next_cursor)


def _composition_revision(row: Any) -> CompositionRevision:
    children = row["child_layer_ids_json"]
    if isinstance(children, str):
        children = json.loads(children)
    return CompositionRevision(
        str(row["revision_id"]),
        str(row["layer_id"]),
        tuple(str(item) for item in children),
        _aware_datetime(row["valid_from"]),
        None if row.get("valid_to") is None else _aware_datetime(row["valid_to"]),
        _aware_datetime(row["recorded_at"]),
        row.get("supersedes_revision_id"),
    )


def _aware_datetime(value: object) -> datetime:
    parsed = (
        value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    )
    return (
        parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)
    )


class MariaDbCrdtDocumentStore:
    """SQLAlchemy repository for the prepared CRDT document contract."""

    def __init__(
        self,
        connection: Connection,
        config: CrdtDocumentStoreConfig,
        projection_config: OverrideLedgerConfig,
    ) -> None:
        self.connection = connection
        self.config = config
        self.documents = _table(connection, build_crdt_document_table_config(config))
        self.updates = _table(connection, build_crdt_update_table_config(config))
        self.projection = _table(
            connection, build_override_table_config(projection_config)
        )

    def load_document(self, document_id: str) -> CrdtDocument | None:
        head = (
            self.connection.execute(
                select(self.documents).where(
                    self.documents.c.document_id == document_id
                )
            )
            .mappings()
            .first()
        )
        if head is None:
            return None
        document = _doc()
        rows = self.connection.execute(
            select(
                self.updates.c.update_base64,
                self.updates.c.accepted_update_hash,
            )
            .where(self.updates.c.document_id == document_id)
            .order_by(self.updates.c.revision)
        ).mappings()
        for row in rows:
            update = _decode(row["update_base64"])
            if sha256(update).hexdigest() != row["accepted_update_hash"]:
                raise ValueError(
                    "stored accepted CRDT update hash does not match update"
                )
            document.apply_update(update)
        return CrdtDocument(
            document_id=document_id,
            revision=head["revision"],
            state_vector=document.get_state(),
            update=document.get_update(),
        )

    def diff(self, document_id: str, state_vector: bytes) -> bytes:
        document = self.load_document(document_id)
        if document is None:
            raise KeyError(f"unknown CRDT document: {document_id}")
        current = _doc()
        current.apply_update(document.update)
        return current.get_update(state_vector)

    def erase_document(self, document_id: str) -> None:
        self.connection.execute(
            self.projection.delete().where(
                self.projection.c.crdt_document_id == document_id
            )
        )
        self.connection.execute(
            self.updates.delete().where(self.updates.c.document_id == document_id)
        )
        self.connection.execute(
            self.documents.delete().where(self.documents.c.document_id == document_id)
        )

    def commit(
        self,
        prepared: PreparedCrdtCommit,
        *,
        guards: Sequence[RowGuard] = (),
        inserts: Sequence[AtomicInsert] = (),
        updates: Sequence[AtomicUpdate] = (),
    ) -> CrdtCommitResult:
        if (
            sha256(prepared.accepted_update).hexdigest()
            != prepared.accepted_update_hash
        ):
            raise ValueError("accepted CRDT update hash does not match update")
        duplicate = self._duplicate_result(prepared)
        if duplicate is not None:
            return duplicate
        if prepared.is_noop:
            current = self.load_document(prepared.document_id)
            return CrdtCommitResult(
                current,
                0 if current is None else current.revision,
                b"",
                accepted=False,
                duplicate=True,
            )

        try:
            with self.connection.begin_nested():
                return self._commit(
                    prepared, guards=guards, inserts=inserts, updates=updates
                )
        except IntegrityError as exc:
            duplicate = self._duplicate_result(prepared)
            if duplicate is not None:
                return duplicate
            current = self.load_document(prepared.document_id)
            if (0 if current is None else current.revision) != prepared.base_revision:
                raise CrdtRevisionConflict(
                    "CRDT document revision changed during commit"
                ) from exc
            raise ValueError("CRDT commit conflicts with an immutable row") from exc

    def _commit(
        self,
        prepared: PreparedCrdtCommit,
        *,
        guards: Sequence[RowGuard],
        inserts: Sequence[AtomicInsert],
        updates: Sequence[AtomicUpdate],
    ) -> CrdtCommitResult:
        self._check_guards(guards)
        self._apply_updates(updates)
        current = self.load_document(prepared.document_id)
        revision = 0 if current is None else current.revision
        if revision != prepared.base_revision:
            raise CrdtRevisionConflict(
                "CRDT document revision does not match prepared revision"
            )
        next_revision = revision + 1
        if current is None:
            self.connection.execute(
                self.documents.insert().values(
                    document_id=prepared.document_id,
                    revision=next_revision,
                    generation=_generation(prepared),
                    head_state_vector_base64=_encode(prepared.state_vector),
                    updated_at=datetime.now(UTC),
                )
            )
        else:
            result = self.connection.execute(
                self.documents.update()
                .where(self.documents.c.document_id == prepared.document_id)
                .where(self.documents.c.revision == prepared.base_revision)
                .values(
                    revision=next_revision,
                    head_state_vector_base64=_encode(prepared.state_vector),
                    updated_at=datetime.now(UTC),
                )
            )
            if result.rowcount != 1:
                raise CrdtRevisionConflict(
                    "CRDT document revision changed during commit"
                )
        self.connection.execute(
            self.updates.insert().values(
                document_id=prepared.document_id,
                revision=next_revision,
                generation=_generation(prepared),
                source_update_hash=prepared.source_update_hash,
                accepted_update_hash=prepared.accepted_update_hash,
                update_base64=_encode(prepared.accepted_update),
                accepted_at=datetime.now(UTC),
            )
        )
        self._insert_operations(prepared, next_revision)
        for insert in inserts:
            self.connection.execute(
                _table(self.connection, insert.table_config).insert().values(insert.row)
            )
        return CrdtCommitResult(
            self.load_document(prepared.document_id),
            next_revision,
            prepared.accepted_update,
            accepted=True,
        )

    def _apply_updates(self, updates: Sequence[AtomicUpdate]) -> None:
        for update in updates:
            table = _table(self.connection, update.table_config)
            predicates = [
                table.c[name] == value
                for name, value in {
                    **update.key_values,
                    **update.expected_values,
                }.items()
            ]
            if (
                self.connection.execute(
                    table.update().where(and_(*predicates)).values(update.values)
                ).rowcount
                != 1
            ):
                raise CrdtPreconditionFailed("CRDT atomic update no longer matches")

    def _duplicate_result(
        self, prepared: PreparedCrdtCommit
    ) -> CrdtCommitResult | None:
        duplicate = (
            self.connection.execute(
                select(self.updates)
                .where(self.updates.c.document_id == prepared.document_id)
                .where(self.updates.c.source_update_hash == prepared.source_update_hash)
            )
            .mappings()
            .first()
        )
        if duplicate is None:
            return None
        return CrdtCommitResult(
            self.load_document(prepared.document_id),
            duplicate["revision"],
            _decode(duplicate["update_base64"]),
            accepted=False,
            duplicate=True,
        )

    def _check_guards(self, guards: Sequence[RowGuard]) -> None:
        for guard in guards:
            table = _table(self.connection, guard.table_config)
            predicates = [
                table.c[name] == value for name, value in guard.key_values.items()
            ]
            predicates.extend(
                table.c[name] == value for name, value in guard.expected_values.items()
            )
            if (
                self.connection.execute(
                    select(table).where(and_(*predicates)).with_for_update()
                ).first()
                is None
            ):
                raise CrdtPreconditionFailed("CRDT commit guard no longer matches")

    def _insert_operations(self, prepared: PreparedCrdtCommit, revision: int) -> None:
        for operation in prepared.operations:
            self.connection.execute(
                self.projection.insert().values(
                    operation_id=operation.operation_id,
                    change_set_id=operation.change_set_id,
                    owner_user_id=None,
                    actor_user_id=operation.actor_id,
                    feed_id=operation.feed_id,
                    entity_id=operation.entity_id,
                    field_path=operation.field_path,
                    operation_type=operation.operation_type,
                    value_type=None
                    if operation.value is None
                    else operation.value.value_type,
                    value_json=_json(
                        None if operation.value is None else operation.value.value_json
                    ),
                    unit=None if operation.value is None else operation.value.unit,
                    observed_canonical_value_json=_json(
                        operation.observed_canonical_value_json
                    ),
                    created_against_stale_source=False,
                    valid_from=operation.valid_from,
                    valid_to=operation.valid_to,
                    comment=operation.comment,
                    metadata_json=_json(operation.metadata_json or {}),
                    format_version=operation.format_version,
                    layer_id=operation.layer_id,
                    actor_id=operation.actor_id,
                    actor_display_name=(operation.metadata_json or {}).get(
                        "actor_display_name"
                    ),
                    supersedes_operation_ids_json=_json(
                        list(operation.supersedes_operation_ids)
                    ),
                    removes_operation_ids_json=_json(
                        list(operation.removes_operation_ids)
                    ),
                    recorded_at=operation.recorded_at,
                    payload_hash=operation.payload_hash,
                    crdt_document_id=prepared.document_id,
                    crdt_document_revision=revision,
                    generation=_generation(prepared),
                )
            )


def _generation(prepared: PreparedCrdtCommit) -> int:
    return prepared.generation


class MariaDbDocumentAccessStore:
    """MariaDB implementation of the document access contract."""

    def __init__(
        self, connection: Connection, config: DocumentAccessStoreConfig
    ) -> None:
        self.connection = connection
        document_config, grants_config, commands_config = (
            build_document_access_table_configs(config)
        )
        self.document_config = document_config
        self.documents = _table(connection, document_config)
        self.grants_table = _table(connection, grants_config)
        self.commands = _table(connection, commands_config)

    def get(self, document_id: str) -> AccessDocument | None:
        row = (
            self.connection.execute(
                select(self.documents).where(
                    self.documents.c.document_id == document_id
                )
            )
            .mappings()
            .first()
        )
        return _access_document(row) if row else None

    def grants(
        self,
        document_id: str,
        *,
        include_revoked: bool = False,
        cursor: str | None = None,
        limit: int = 100,
    ) -> Page[AccessGrant]:
        validate_limit(limit)
        query = select(self.grants_table).where(
            self.grants_table.c.document_id == document_id
        )
        if not include_revoked:
            query = query.where(self.grants_table.c.revoked_at.is_(None))
        if cursor is not None:
            granted_at, grant_id = decode_cursor(cursor)
            query = query.where(
                or_(
                    self.grants_table.c.granted_at > granted_at,
                    and_(
                        self.grants_table.c.granted_at == granted_at,
                        self.grants_table.c.grant_id > grant_id,
                    ),
                )
            )
        rows = self.connection.execute(
            query.order_by(
                self.grants_table.c.granted_at, self.grants_table.c.grant_id
            ).limit(limit + 1)
        ).mappings()
        return page_from_items(
            (_access_grant(row) for row in rows),
            lambda grant: (grant.granted_at, grant.grant_id),
            limit,
        )

    def _all_grants(
        self, document_id: str, *, include_revoked: bool = False
    ) -> tuple[AccessGrant, ...]:
        query = select(self.grants_table).where(
            self.grants_table.c.document_id == document_id
        )
        if not include_revoked:
            query = query.where(self.grants_table.c.revoked_at.is_(None))
        return tuple(
            _access_grant(row) for row in self.connection.execute(query).mappings()
        )

    def list_for_groups(
        self,
        groups: Sequence[str],
        *,
        include_archived: bool = False,
        cursor: str | None = None,
        limit: int = 100,
    ) -> Page[AccessDocument]:
        validate_limit(limit)
        if not groups:
            if cursor is not None:
                decode_cursor(cursor)
            return Page((), None)
        query = (
            select(self.documents)
            .distinct()
            .join(self.grants_table)
            .where(
                self.grants_table.c.group_name.in_(groups),
                self.grants_table.c.revoked_at.is_(None),
            )
        )
        if not include_archived:
            query = query.where(self.documents.c.status == "active")
        query = self._after_document_cursor(query, cursor)
        rows = self.connection.execute(
            query.order_by(
                self.documents.c.created_at, self.documents.c.document_id
            ).limit(limit + 1)
        ).mappings()
        return page_from_items(
            (_access_document(row) for row in rows),
            lambda document: (document.created_at, document.document_id),
            limit,
        )

    def list_all(
        self,
        *,
        include_archived: bool = False,
        cursor: str | None = None,
        limit: int = 100,
    ) -> Page[AccessDocument]:
        validate_limit(limit)
        query = select(self.documents)
        if not include_archived:
            query = query.where(self.documents.c.status == "active")
        query = self._after_document_cursor(query, cursor)
        rows = self.connection.execute(
            query.order_by(
                self.documents.c.created_at, self.documents.c.document_id
            ).limit(limit + 1)
        ).mappings()
        return page_from_items(
            (_access_document(row) for row in rows),
            lambda document: (document.created_at, document.document_id),
            limit,
        )

    def _after_document_cursor(self, query: Any, cursor: str | None) -> Any:
        if cursor is None:
            return query
        created_at, document_id = decode_cursor(cursor)
        return query.where(
            or_(
                self.documents.c.created_at > created_at,
                and_(
                    self.documents.c.created_at == created_at,
                    self.documents.c.document_id > document_id,
                ),
            )
        )

    def guard(self, document_id: str, expected_revision: int) -> RowGuard:
        return RowGuard(
            self.document_config,
            {"document_id": document_id},
            {"status": "active", "revision": expected_revision},
        )

    def begin_purge(self, document_id: str, expected_generation: int) -> AccessDocument:
        document = self.get(document_id)
        if document is None:
            raise DocumentNotFound("document not found")
        if document.generation != expected_generation or document.status not in {
            "active",
            "purging",
        }:
            raise DocumentRevisionConflict("document generation changed")
        self.connection.execute(
            self.documents.update()
            .where(self.documents.c.document_id == document_id)
            .where(self.documents.c.generation == expected_generation)
            .values(status="purging")
        )
        return replace(document, status="purging")

    def create(
        self,
        document_id: str,
        name: str,
        description: str | None,
        actor_id: str,
        recorded_at: datetime,
        *,
        initial_grants: Sequence[AccessGrantInput] = (),
        idempotency_key: str,
        owning_group: str | None = None,
        allow_existing: bool = False,
    ) -> AccessMutationResult:
        payload = _create_payload(
            document_id,
            name,
            description,
            actor_id,
            initial_grants,
            owning_group,
            allow_existing,
        )
        duplicate = self._duplicate(idempotency_key, payload)
        if duplicate:
            return duplicate
        _require_time(recorded_at)
        normalized_name = _normalized(name)
        if allow_existing:
            existing = self._by_group_and_name(owning_group, normalized_name)
            if existing is not None:
                return self._record_existing(
                    idempotency_key,
                    payload,
                    recorded_at,
                    existing,
                    owning_group,
                )
        document = AccessDocument(
            document_id,
            name,
            normalized_name,
            description,
            "active",
            1,
            actor_id,
            recorded_at,
            owning_group=owning_group,
        )
        try:
            with self.connection.begin_nested():
                self.connection.execute(
                    self.documents.insert().values(**asdict(document))
                )
                for grant in initial_grants:
                    self._insert_grant(document, grant, actor_id, recorded_at)
                return self._record(
                    idempotency_key, payload, "create", recorded_at, document
                )
        except IntegrityError as exc:
            duplicate = self._duplicate(idempotency_key, payload)
            if duplicate:
                return duplicate
            existing = self._by_group_and_name(owning_group, normalized_name)
            if existing is not None and allow_existing:
                return self._record_existing(
                    idempotency_key,
                    payload,
                    recorded_at,
                    existing,
                    owning_group,
                )
            raise DocumentAccessError("document or grant already exists") from exc

    def _by_group_and_name(
        self, owning_group: str | None, normalized_name: str
    ) -> AccessDocument | None:
        group_match = (
            self.documents.c.owning_group.is_(None)
            if owning_group is None
            else self.documents.c.owning_group == owning_group
        )
        row = (
            self.connection.execute(
                select(self.documents).where(
                    group_match,
                    self.documents.c.normalized_name == normalized_name,
                )
            )
            .mappings()
            .first()
        )
        return _access_document(row) if row else None

    def grant(
        self,
        document_id: str,
        grant: AccessGrantInput,
        actor_id: str,
        recorded_at: datetime,
        expected_revision: int,
        *,
        idempotency_key: str,
    ) -> AccessMutationResult:
        payload = _payload("grant", document_id, grant, actor_id, expected_revision)
        duplicate = self._duplicate(idempotency_key, payload)
        if duplicate:
            return duplicate
        _require_time(recorded_at)
        try:
            with self.connection.begin_nested():
                document = self._active(document_id, expected_revision)
                updated = self._advance(document)
                self._insert_grant(updated, grant, actor_id, recorded_at)
                return self._record(
                    idempotency_key, payload, "grant", recorded_at, updated
                )
        except IntegrityError as exc:
            duplicate = self._duplicate(idempotency_key, payload)
            if duplicate:
                return duplicate
            raise DocumentAccessError("grant already exists") from exc

    def archive(
        self,
        document_id: str,
        actor_id: str,
        recorded_at: datetime,
        expected_revision: int,
        *,
        idempotency_key: str,
    ) -> AccessMutationResult:
        payload = _payload("archive", document_id, actor_id, expected_revision)
        duplicate = self._duplicate(idempotency_key, payload)
        if duplicate:
            return duplicate
        _require_time(recorded_at)
        try:
            with self.connection.begin_nested():
                document = self._active(document_id, expected_revision)
                updated = self._advance(
                    document,
                    status="archived",
                    archived_by=actor_id,
                    archived_at=recorded_at,
                )
                return self._record(
                    idempotency_key, payload, "archive", recorded_at, updated
                )
        except IntegrityError as exc:
            duplicate = self._duplicate(idempotency_key, payload)
            if duplicate:
                return duplicate
            raise DocumentAccessError("archive conflicts with another command") from exc

    def revoke(
        self,
        document_id: str,
        group_name: str,
        actor_id: str,
        recorded_at: datetime,
        expected_revision: int,
        *,
        idempotency_key: str,
    ) -> AccessMutationResult:
        payload = _payload(
            "revoke", document_id, group_name, actor_id, expected_revision
        )
        duplicate = self._duplicate(idempotency_key, payload)
        if duplicate:
            return duplicate
        _require_time(recorded_at)
        try:
            with self.connection.begin_nested():
                document = self._active(document_id, expected_revision)
                row = (
                    self.connection.execute(
                        select(self.grants_table)
                        .where(
                            self.grants_table.c.document_id == document_id,
                            self.grants_table.c.group_name == group_name,
                            self.grants_table.c.revoked_at.is_(None),
                        )
                        .with_for_update()
                    )
                    .mappings()
                    .first()
                )
                if row is None:
                    raise DocumentAccessError("active group grant not found")
                updated = self._advance(document)
                self.connection.execute(
                    self.grants_table.update()
                    .where(self.grants_table.c.grant_id == row["grant_id"])
                    .values(
                        active_group_key=None,
                        revoked_by=actor_id,
                        revoked_at=recorded_at,
                        document_revision=updated.revision,
                    )
                )
                return self._record(
                    idempotency_key, payload, "revoke", recorded_at, updated
                )
        except IntegrityError as exc:
            duplicate = self._duplicate(idempotency_key, payload)
            if duplicate:
                return duplicate
            raise DocumentAccessError("revoke conflicts with another command") from exc

    def _active(self, document_id: str, revision: int) -> AccessDocument:
        row = (
            self.connection.execute(
                select(self.documents)
                .where(self.documents.c.document_id == document_id)
                .with_for_update()
            )
            .mappings()
            .first()
        )
        if row is None:
            raise DocumentNotFound("document not found")
        document = _access_document(row)
        if document.status != "active":
            raise DocumentArchived("document archived")
        if document.revision != revision:
            raise DocumentRevisionConflict("document revision changed")
        return document

    def _advance(self, document: AccessDocument, **extra: object) -> AccessDocument:
        values = {"revision": document.revision + 1, **extra}
        result = self.connection.execute(
            self.documents.update()
            .where(
                and_(
                    self.documents.c.document_id == document.document_id,
                    self.documents.c.revision == document.revision,
                    self.documents.c.status == "active",
                )
            )
            .values(values)
        )
        if result.rowcount != 1:
            raise DocumentRevisionConflict("document revision changed")
        return AccessDocument(**{**asdict(document), **values})

    def _insert_grant(
        self,
        document: AccessDocument,
        grant: AccessGrantInput,
        actor_id: str,
        recorded_at: datetime,
    ) -> None:
        self.connection.execute(
            self.grants_table.insert().values(
                grant_id=grant.grant_id,
                document_id=document.document_id,
                group_name=grant.group_name,
                active_group_key=f"{document.document_id}:{grant.group_name.casefold()}",
                role=grant.role,
                granted_by=actor_id,
                granted_at=recorded_at,
                document_revision=document.revision,
            )
        )

    def _duplicate(self, key: str, payload: str) -> AccessMutationResult | None:
        row = (
            self.connection.execute(
                select(self.commands).where(self.commands.c.idempotency_key == key)
            )
            .mappings()
            .first()
        )
        if row is None:
            return None
        if row["payload_hash"] != payload:
            raise IdempotencyConflict(
                "idempotency key was reused with different content"
            )
        value = row["result_json"]
        return _access_result(
            json.loads(value) if isinstance(value, str) else value, duplicate=True
        )

    def _record(
        self,
        key: str,
        payload: str,
        kind: str,
        recorded_at: datetime,
        document: AccessDocument,
        result: AccessMutationResult | None = None,
    ) -> AccessMutationResult:
        if result is None:
            result = AccessMutationResult(
                document,
                self._all_grants(document.document_id, include_revoked=True),
                accepted=True,
            )
        self.connection.execute(
            self.commands.insert().values(
                idempotency_key=key,
                payload_hash=payload,
                command_kind=kind,
                result_json=json.dumps(
                    _access_result_json(result), default=_json_default
                ),
                recorded_at=recorded_at,
            )
        )
        return result

    def _record_existing(
        self,
        key: str,
        payload: str,
        recorded_at: datetime,
        document: AccessDocument,
        owning_group: str | None,
    ) -> AccessMutationResult:
        result = _existing_result(
            document,
            self._all_grants(document.document_id),
            owning_group,
        )
        try:
            with self.connection.begin_nested():
                return self._record(
                    key, payload, "create", recorded_at, document, result
                )
        except IntegrityError:
            duplicate = self._duplicate(key, payload)
            if duplicate:
                return duplicate
            raise


def _access_document(row: Any) -> AccessDocument:
    return AccessDocument(**_datetime_fields(dict(row), "created_at", "archived_at"))


def _access_grant(row: Any) -> AccessGrant:
    values = dict(row)
    values.pop("active_group_key", None)
    return AccessGrant(**_datetime_fields(values, "granted_at", "revoked_at"))


def _access_result_json(result: AccessMutationResult) -> dict[str, Any]:
    return {
        "document": asdict(result.document),
        "grants": [asdict(grant) for grant in result.grants],
    }


def _access_result(value: dict[str, Any], *, duplicate: bool) -> AccessMutationResult:
    return AccessMutationResult(
        AccessDocument(
            **_datetime_fields(value["document"], "created_at", "archived_at")
        ),
        tuple(
            AccessGrant(**_datetime_fields(grant, "granted_at", "revoked_at"))
            for grant in value["grants"]
        ),
        accepted=not duplicate,
        duplicate=duplicate,
    )


def _datetime_fields(value: dict[str, Any], *names: str) -> dict[str, Any]:
    result = dict(value)
    for name in names:
        if isinstance(result.get(name), str):
            result[name] = datetime.fromisoformat(result[name])
    return result


def _json_default(value: object) -> object:
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"not JSON serializable: {type(value).__name__}")


def _table(connection: Connection, config: TableConfig) -> Table:
    return Table(config.name, MetaData(schema=config.schema), autoload_with=connection)


def _encode(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


def _decode(value: str) -> bytes:
    return base64.b64decode(value)


def _json(value: object | None) -> str | None:
    return None if value is None else json.dumps(value, separators=(",", ":"))
