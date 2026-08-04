from __future__ import annotations

import base64
import json
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import UTC, date, datetime
from hashlib import sha256
from typing import Any

from sqlalchemy import text

from polars_hist_db.backends.xtdb_arrow import (
    _xtdb_cast_type,
    _xtdb_document_id_cast_type,
    _xtdb_document_id_value,
)
from polars_hist_db.backends.xtdb_transport import (
    _execute_xtdb_transaction,
    _is_xtdb_table_not_found_error,
    _qualified_table_name,
    _rollback_xtdb_connection,
    _xtdb_column_identifier,
    _xtdb_sql_literal,
)
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


class XtdbLayerCompositionStore:
    def __init__(self, connection: Any, config: LayerCompositionStoreConfig) -> None:
        self.connection = connection
        self.table = build_layer_composition_table_config(config)

    def append(self, revision: CompositionRevision, actor_id: str) -> None:
        _execute_xtdb_transaction(
            self.connection,
            [
                _insert_statement(
                    self.table,
                    {
                        "revision_id": revision.revision_id,
                        "layer_id": revision.layer_id,
                        "child_layer_ids_json": list(revision.child_layer_ids),
                        "valid_from": revision.valid_from,
                        "valid_to": revision.valid_to,
                        "recorded_at": revision.recorded_at,
                        "actor_id": actor_id,
                        "supersedes_revision_id": revision.supersedes_revision_id,
                    },
                )
            ],
        )

    def revisions(
        self,
        layer_id: str | None = None,
        *,
        cursor: str | None = None,
        limit: int = 100,
    ) -> Page[CompositionRevision]:
        validate_limit(limit)
        predicates = []
        if layer_id is not None:
            predicates.append(f"layer_id = {_literal(layer_id, 'VARCHAR(128)')}")
        if cursor is not None:
            system_from, revision_id = decode_cursor(cursor)
            timestamp = _literal(system_from, "TIMESTAMP WITH TIME ZONE")
            revision = _literal(revision_id, "VARCHAR(128)")
            predicates.append(
                f"(_system_from > {timestamp} OR "
                f"(_system_from = {timestamp} AND revision_id > {revision}))"
            )
        predicate = " WHERE " + " AND ".join(predicates) if predicates else ""
        try:
            rows = list(
                self.connection.execute(
                    text(
                        "SELECT revision_id, layer_id, child_layer_ids_json, "
                        f"valid_from, valid_to, recorded_at, _system_from AS system_from, "
                        f"supersedes_revision_id "
                        f"FROM {_table_name(self.table)}"
                        f"{predicate} ORDER BY _system_from, revision_id LIMIT {limit + 1}"
                    )
                ).mappings()
            )
            revisions = tuple(_composition_revision(row) for row in rows[:limit])
            next_cursor = (
                encode_cursor(
                    (
                        revisions[-1].recorded_at,
                        revisions[-1].revision_id,
                    )
                )
                if len(rows) > limit
                else None
            )
            return Page(revisions, next_cursor)
        except Exception as exc:
            if not _is_xtdb_table_not_found_error(exc):
                raise
            _rollback_xtdb_connection(self.connection)
            return Page((), None)


def _composition_revision(row: Mapping[str, object]) -> CompositionRevision:
    children = row["child_layer_ids_json"]
    if isinstance(children, str):
        children = json.loads(children)
    if not isinstance(children, list):
        raise TypeError("stored composition children are invalid")
    return CompositionRevision(
        str(row["revision_id"]),
        str(row["layer_id"]),
        tuple(str(item) for item in children),
        _datetime(row["valid_from"]),
        None if row.get("valid_to") is None else _datetime(row["valid_to"]),
        _datetime(row.get("system_from") or row["recorded_at"]),
        None
        if row.get("supersedes_revision_id") is None
        else str(row["supersedes_revision_id"]),
    )


class XtdbCrdtDocumentStore:
    """XTDB repository for the backend-neutral prepared CRDT contract."""

    def __init__(
        self,
        connection: Any,
        config: CrdtDocumentStoreConfig,
        projection_config: OverrideLedgerConfig,
    ) -> None:
        self.connection = connection
        self.documents = build_crdt_document_table_config(config)
        self.updates = build_crdt_update_table_config(config)
        self.projection = build_override_table_config(projection_config)

    def load_document(self, document_id: str) -> CrdtDocument | None:
        document_id_sql = _literal(document_id, "VARCHAR(128)")
        head = self._rows(
            f"SELECT document_id, revision FROM {_table_name(self.documents)} "
            f"WHERE document_id = {document_id_sql}"
        )
        if not head:
            return None
        document = _doc()
        for row in self._rows(
            "SELECT update_base64, accepted_update_hash "
            f"FROM {_table_name(self.updates)} "
            f"WHERE document_id = {document_id_sql} ORDER BY revision"
        ):
            update = _decode(str(row["update_base64"]))
            if sha256(update).hexdigest() != row["accepted_update_hash"]:
                raise ValueError(
                    "stored accepted CRDT update hash does not match update"
                )
            document.apply_update(update)
        return CrdtDocument(
            document_id=document_id,
            revision=int(str(head[0]["revision"])),
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
        value = _literal(document_id, "VARCHAR(128)")
        _execute_xtdb_transaction(
            self.connection,
            [
                f"ERASE FROM {_table_name(self.projection)} WHERE crdt_document_id = {value}",
                f"ERASE FROM {_table_name(self.updates)} WHERE document_id = {value}",
                f"ERASE FROM {_table_name(self.documents)} WHERE document_id = {value}",
            ],
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
        self._ensure_tables()
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

        statements = [
            self._source_assertion(prepared),
            *(self._guard_assertion(guard) for guard in guards),
            *(self._update_assertion(update) for update in updates),
            self._revision_assertion(prepared),
            _insert_statement(
                self.documents,
                {
                    "document_id": prepared.document_id,
                    "revision": prepared.base_revision + 1,
                    "generation": _generation(prepared),
                    "head_state_vector_base64": _encode(prepared.state_vector),
                    "updated_at": _accepted_at(prepared),
                },
            ),
            _insert_statement(
                self.updates,
                {
                    "document_id": prepared.document_id,
                    "revision": prepared.base_revision + 1,
                    "generation": _generation(prepared),
                    "source_update_hash": prepared.source_update_hash,
                    "accepted_update_hash": prepared.accepted_update_hash,
                    "update_base64": _encode(prepared.accepted_update),
                    "accepted_at": _accepted_at(prepared),
                },
            ),
            *(
                self._operation_statement(operation, prepared)
                for operation in prepared.operations
            ),
            *(_insert_statement(insert.table_config, insert.row) for insert in inserts),
            *(_update_statement(update) for update in updates),
        ]
        try:
            _execute_xtdb_transaction(self.connection, statements)
        except Exception:
            duplicate = self._duplicate_result(prepared)
            if duplicate is not None:
                return duplicate
            current = self.load_document(prepared.document_id)
            if (0 if current is None else current.revision) != prepared.base_revision:
                raise CrdtRevisionConflict(
                    "CRDT document revision changed during commit"
                ) from None
            if any(not self._guard_matches(guard) for guard in guards) or any(
                not self._update_matches(update) for update in updates
            ):
                raise CrdtPreconditionFailed(
                    "CRDT commit guard no longer matches"
                ) from None
            raise

        return CrdtCommitResult(
            self.load_document(prepared.document_id),
            prepared.base_revision + 1,
            prepared.accepted_update,
            accepted=True,
        )

    def _duplicate_result(
        self, prepared: PreparedCrdtCommit
    ) -> CrdtCommitResult | None:
        rows = self._rows(
            "SELECT revision, update_base64 "
            f"FROM {_table_name(self.updates)} WHERE "
            f"document_id = {_literal(prepared.document_id, 'VARCHAR(128)')} AND "
            f"source_update_hash = {_literal(prepared.source_update_hash, 'VARCHAR(64)')}"
        )
        if not rows:
            return None
        return CrdtCommitResult(
            self.load_document(prepared.document_id),
            int(str(rows[0]["revision"])),
            _decode(str(rows[0]["update_base64"])),
            accepted=False,
            duplicate=True,
        )

    def _source_assertion(self, prepared: PreparedCrdtCommit) -> str:
        return (
            "ASSERT NOT EXISTS (SELECT 1 FROM "
            f"{_table_name(self.updates)} WHERE "
            f"document_id = {_literal(prepared.document_id, 'VARCHAR(128)')} AND "
            f"source_update_hash = {_literal(prepared.source_update_hash, 'VARCHAR(64)')}"
            "), 'CRDT source update already accepted'"
        )

    def _revision_assertion(self, prepared: PreparedCrdtCommit) -> str:
        document_id = _literal(prepared.document_id, "VARCHAR(128)")
        if prepared.base_revision == 0:
            predicate = f"NOT EXISTS (SELECT 1 FROM {_table_name(self.documents)} WHERE document_id = {document_id})"
        else:
            predicate = (
                "EXISTS (SELECT 1 FROM "
                f"{_table_name(self.documents)} WHERE document_id = {document_id} "
                f"AND revision = {prepared.base_revision}::BIGINT)"
            )
        return f"ASSERT {predicate}, 'CRDT document revision changed'"

    def _guard_assertion(self, guard: RowGuard) -> str:
        return f"ASSERT EXISTS (SELECT 1 FROM {_table_name(guard.table_config)} WHERE {_where(guard.table_config, {**guard.key_values, **guard.expected_values})}), 'CRDT commit guard failed'"

    def _guard_matches(self, guard: RowGuard) -> bool:
        return bool(
            self._rows(
                f"SELECT 1 FROM {_table_name(guard.table_config)} "
                f"WHERE {_where(guard.table_config, {**guard.key_values, **guard.expected_values})}"
            )
        )

    def _update_assertion(self, update: AtomicUpdate) -> str:
        return (
            "ASSERT EXISTS (SELECT 1 FROM "
            f"{_table_name(update.table_config)} WHERE "
            f"{_where(update.table_config, {**update.key_values, **update.expected_values})}), "
            "'CRDT atomic update failed'"
        )

    def _update_matches(self, update: AtomicUpdate) -> bool:
        return bool(
            self._rows(
                f"SELECT 1 FROM {_table_name(update.table_config)} WHERE "
                f"{_where(update.table_config, {**update.key_values, **update.expected_values})}"
            )
        )

    def _operation_statement(self, operation: Any, prepared: PreparedCrdtCommit) -> str:
        row = {
            "operation_id": operation.operation_id,
            "change_set_id": operation.change_set_id,
            "owner_user_id": None,
            "actor_user_id": operation.actor_id,
            "feed_id": operation.feed_id,
            "entity_id": operation.entity_id,
            "field_path": operation.field_path,
            "operation_type": operation.operation_type,
            "value_type": None
            if operation.value is None
            else operation.value.value_type,
            "value_json": None
            if operation.value is None
            else operation.value.value_json,
            "unit": None if operation.value is None else operation.value.unit,
            "observed_canonical_value_json": operation.observed_canonical_value_json,
            "created_against_stale_source": False,
            "valid_from": operation.valid_from,
            "valid_to": operation.valid_to,
            "comment": operation.comment,
            "metadata_json": operation.metadata_json or {},
            "format_version": operation.format_version,
            "layer_id": operation.layer_id,
            "actor_id": operation.actor_id,
            "actor_display_name": (operation.metadata_json or {}).get(
                "actor_display_name"
            ),
            "supersedes_operation_ids_json": list(operation.supersedes_operation_ids),
            "removes_operation_ids_json": list(operation.removes_operation_ids),
            "recorded_at": operation.recorded_at,
            "payload_hash": operation.payload_hash,
            "crdt_document_id": prepared.document_id,
            "crdt_document_revision": prepared.base_revision + 1,
            "generation": _generation(prepared),
        }
        return _insert_statement(
            self.projection,
            row,
            valid_from=operation.valid_from,
            valid_to=operation.valid_to,
        )

    def _ensure_tables(self) -> None:
        for config in (self.documents, self.updates, self.projection):
            if self._rows(
                "SELECT table_name FROM information_schema.tables WHERE "
                f"table_schema = {_literal(config.schema, 'TEXT')} AND "
                f"table_name = {_literal(config.name, 'TEXT')}"
            ):
                continue
            table_name = _table_name(config)
            bootstrap_row = {
                column.name: _bootstrap_value(column.data_type)
                for column in config.columns
            }
            _execute_xtdb_transaction(
                self.connection,
                [
                    _insert_statement(config, bootstrap_row),
                    f"ERASE FROM {table_name} WHERE {_where(config, {key: bootstrap_row[key] for key in config.primary_keys})}",
                ],
            )

    def _rows(self, sql: str) -> list[Mapping[str, object]]:
        try:
            return [dict(row) for row in self.connection.execute(text(sql)).mappings()]
        except Exception as exc:
            if not _is_xtdb_table_not_found_error(exc):
                raise
            _rollback_xtdb_connection(self.connection)
            return []


_ACCESS_ASSERTION_MESSAGES = (
    "idempotency key already exists",
    "document already exists",
    "document name already exists",
    "grant already exists",
    "group already has an active grant",
    "document revision changed",
    "active group grant not found",
)


def _access_assertion_message(exc: Exception) -> str | None:
    error = str(exc)
    return next(
        (message for message in _ACCESS_ASSERTION_MESSAGES if message in error), None
    )


def _owning_group_predicate(owning_group: str | None) -> str:
    if owning_group is None:
        return "owning_group IS NULL"
    return f"owning_group = {_literal(owning_group, 'VARCHAR(255)')}"


class XtdbDocumentAccessStore:
    """XTDB implementation of the backend-neutral document access contract."""

    def __init__(self, connection: Any, config: DocumentAccessStoreConfig) -> None:
        self.connection = connection
        self.documents, self.grants_table, self.commands = (
            build_document_access_table_configs(config)
        )

    def get(self, document_id: str) -> AccessDocument | None:
        rows = self._rows(
            f"SELECT {_access_document_columns()} FROM {_table_name(self.documents)} "
            f"WHERE document_id = {_literal(document_id, 'VARCHAR(128)')}"
        )
        return _access_document_from_row(rows[0]) if rows else None

    def grants(
        self,
        document_id: str,
        *,
        include_revoked: bool = False,
        cursor: str | None = None,
        limit: int = 100,
    ) -> Page[AccessGrant]:
        validate_limit(limit)
        predicates = [f"document_id = {_literal(document_id, 'VARCHAR(128)')}"]
        if not include_revoked:
            predicates.append("revoked_at IS NULL")
        if cursor is not None:
            granted_at, grant_id = decode_cursor(cursor)
            timestamp = _literal(granted_at, "TIMESTAMP WITH TIME ZONE")
            identifier = _literal(grant_id, "VARCHAR(128)")
            predicates.append(
                f"(granted_at > {timestamp} OR "
                f"(granted_at = {timestamp} AND grant_id > {identifier}))"
            )
        rows = self._rows(
            f"SELECT {_access_grant_columns()} FROM {_table_name(self.grants_table)} "
            f"WHERE {' AND '.join(predicates)} ORDER BY granted_at, grant_id "
            f"LIMIT {limit + 1}"
        )
        return page_from_items(
            (_access_grant_from_row(row) for row in rows),
            lambda grant: (grant.granted_at, grant.grant_id),
            limit,
        )

    def _all_grants(
        self, document_id: str, *, include_revoked: bool = False
    ) -> tuple[AccessGrant, ...]:
        predicate = f"document_id = {_literal(document_id, 'VARCHAR(128)')}"
        if not include_revoked:
            predicate += " AND revoked_at IS NULL"
        return tuple(
            _access_grant_from_row(row)
            for row in self._rows(
                f"SELECT {_access_grant_columns()} FROM {_table_name(self.grants_table)} "
                f"WHERE {predicate} ORDER BY granted_at, grant_id"
            )
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
        group_sql = ", ".join(_literal(group, "VARCHAR(255)") for group in groups)
        status = "" if include_archived else " AND d.status = 'active'"
        after = self._document_cursor_predicate(cursor, alias="d")
        rows = self._rows(
            f"SELECT DISTINCT {_access_document_columns('d')} "
            f"FROM {_table_name(self.documents)} d "
            f"JOIN {_table_name(self.grants_table)} g ON d.document_id = g.document_id "
            f"WHERE g.group_name IN ({group_sql}) AND g.revoked_at IS NULL{status}"
            f"{' AND ' + after if after else ''} "
            f"ORDER BY d.created_at, d.document_id LIMIT {limit + 1}"
        )
        return page_from_items(
            (_access_document_from_row(row) for row in rows),
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
        predicates = [] if include_archived else ["status = 'active'"]
        after = self._document_cursor_predicate(cursor)
        if after:
            predicates.append(after)
        predicate = " WHERE " + " AND ".join(predicates) if predicates else ""
        rows = self._rows(
            f"SELECT {_access_document_columns()} FROM {_table_name(self.documents)}"
            f"{predicate} ORDER BY created_at, document_id LIMIT {limit + 1}"
        )
        return page_from_items(
            (_access_document_from_row(row) for row in rows),
            lambda document: (document.created_at, document.document_id),
            limit,
        )

    @staticmethod
    def _document_cursor_predicate(cursor: str | None, alias: str = "") -> str:
        if cursor is None:
            return ""
        created_at, document_id = decode_cursor(cursor)
        prefix = f"{alias}." if alias else ""
        timestamp = _literal(created_at, "TIMESTAMP WITH TIME ZONE")
        identifier = _literal(document_id, "VARCHAR(128)")
        return (
            f"({prefix}created_at > {timestamp} OR "
            f"({prefix}created_at = {timestamp} "
            f"AND {prefix}document_id > {identifier}))"
        )

    def guard(self, document_id: str, expected_revision: int) -> RowGuard:
        return RowGuard(
            self.documents,
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
        _execute_xtdb_transaction(
            self.connection,
            [
                f"ASSERT EXISTS (SELECT 1 FROM {_table_name(self.documents)} WHERE document_id = {_literal(document_id, 'VARCHAR(128)')} AND generation = {expected_generation}::BIGINT), 'document generation changed'",
                _access_update(self.documents, document_id, {"status": "purging"}),
            ],
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
        grants = tuple(initial_grants)
        payload = _create_payload(
            document_id,
            name,
            description,
            actor_id,
            grants,
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
        if len({grant.grant_id for grant in grants}) != len(grants) or len(
            {grant.group_name.casefold() for grant in grants}
        ) != len(grants):
            raise DocumentAccessError("initial grants must be unique")
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
        result = AccessMutationResult(
            document,
            tuple(
                _new_grant(document, grant, actor_id, recorded_at) for grant in grants
            ),
            accepted=True,
        )
        statements = [
            self._command_assertion(idempotency_key),
            f"ASSERT NOT EXISTS (SELECT 1 FROM {_table_name(self.documents)} WHERE document_id = {_literal(document_id, 'VARCHAR(128)')}), 'document already exists'",
            f"ASSERT NOT EXISTS (SELECT 1 FROM {_table_name(self.documents)} WHERE {_owning_group_predicate(owning_group)} AND normalized_name = {_literal(normalized_name, 'VARCHAR(255)')}), 'document name already exists'",
            *(
                f"ASSERT NOT EXISTS (SELECT 1 FROM {_table_name(self.grants_table)} WHERE grant_id = {_literal(grant.grant_id, 'VARCHAR(128)')}), 'grant already exists'"
                for grant in grants
            ),
            _insert_statement(self.documents, _document_row(document)),
            *(
                _insert_statement(self.grants_table, _grant_row(grant))
                for grant in result.grants
            ),
            self._command_insert(
                idempotency_key, payload, "create", result, recorded_at
            ),
        ]
        try:
            duplicate = self._run(
                statements, idempotency_key, payload, document_id, None
            )
        except DocumentAccessError as exc:
            if allow_existing and str(exc) == "document name already exists":
                existing = self._by_group_and_name(owning_group, normalized_name)
                if existing is not None:
                    return self._record_existing(
                        idempotency_key,
                        payload,
                        recorded_at,
                        existing,
                        owning_group,
                    )
            raise
        return result if duplicate is None else duplicate

    def _by_group_and_name(
        self, owning_group: str | None, normalized_name: str
    ) -> AccessDocument | None:
        rows = self._rows(
            f"SELECT {_access_document_columns()} FROM {_table_name(self.documents)} "
            f"WHERE {_owning_group_predicate(owning_group)} AND "
            f"normalized_name = {_literal(normalized_name, 'VARCHAR(255)')}"
        )
        return _access_document_from_row(rows[0]) if rows else None

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
        document = self._active(document_id, expected_revision)
        updated = _updated_document(document)
        new_grant = _new_grant(updated, grant, actor_id, recorded_at)
        result = AccessMutationResult(
            updated, (*self._all_grants(document_id), new_grant), accepted=True
        )
        statements = [
            self._command_assertion(idempotency_key),
            self._active_assertion(document_id, expected_revision),
            f"ASSERT NOT EXISTS (SELECT 1 FROM {_table_name(self.grants_table)} WHERE grant_id = {_literal(grant.grant_id, 'VARCHAR(128)')}), 'grant already exists'",
            f"ASSERT NOT EXISTS (SELECT 1 FROM {_table_name(self.grants_table)} WHERE active_group_key = {_literal(_active_group_key(document_id, grant.group_name), 'VARCHAR(512)')}), 'group already has an active grant'",
            _access_update(self.documents, document_id, {"revision": updated.revision}),
            _insert_statement(self.grants_table, _grant_row(new_grant)),
            self._command_insert(
                idempotency_key, payload, "grant", result, recorded_at
            ),
        ]
        duplicate = self._run(
            statements, idempotency_key, payload, document_id, expected_revision
        )
        return result if duplicate is None else duplicate

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
        document = self._active(document_id, expected_revision)
        active_grant = self._active_grant(document_id, group_name)
        if active_grant is None:
            raise DocumentAccessError("active group grant not found")
        updated = _updated_document(document)
        revoked = AccessGrant(
            grant_id=active_grant.grant_id,
            document_id=active_grant.document_id,
            group_name=active_grant.group_name,
            role=active_grant.role,
            granted_by=active_grant.granted_by,
            granted_at=active_grant.granted_at,
            document_revision=updated.revision,
            revoked_by=actor_id,
            revoked_at=recorded_at,
        )
        grants = tuple(
            revoked if item.grant_id == revoked.grant_id else item
            for item in self._all_grants(document_id, include_revoked=True)
        )
        result = AccessMutationResult(updated, grants, accepted=True)
        statements = [
            self._command_assertion(idempotency_key),
            self._active_assertion(document_id, expected_revision),
            f"ASSERT EXISTS (SELECT 1 FROM {_table_name(self.grants_table)} WHERE grant_id = {_literal(active_grant.grant_id, 'VARCHAR(128)')} AND revoked_at IS NULL), 'active group grant not found'",
            _access_update(self.documents, document_id, {"revision": updated.revision}),
            _access_update(
                self.grants_table,
                active_grant.grant_id,
                {
                    "active_group_key": None,
                    "revoked_by": actor_id,
                    "revoked_at": recorded_at,
                    "document_revision": updated.revision,
                },
            ),
            self._command_insert(
                idempotency_key, payload, "revoke", result, recorded_at
            ),
        ]
        duplicate = self._run(
            statements, idempotency_key, payload, document_id, expected_revision
        )
        return result if duplicate is None else duplicate

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
        document = self._active(document_id, expected_revision)
        updated = _updated_document(
            document,
            status="archived",
            archived_by=actor_id,
            archived_at=recorded_at,
        )
        result = AccessMutationResult(
            updated, self._all_grants(document_id), accepted=True
        )
        statements = [
            self._command_assertion(idempotency_key),
            self._active_assertion(document_id, expected_revision),
            _access_update(
                self.documents,
                document_id,
                {
                    "revision": updated.revision,
                    "status": "archived",
                    "archived_by": actor_id,
                    "archived_at": recorded_at,
                },
            ),
            self._command_insert(
                idempotency_key, payload, "archive", result, recorded_at
            ),
        ]
        duplicate = self._run(
            statements, idempotency_key, payload, document_id, expected_revision
        )
        return result if duplicate is None else duplicate

    def _active(self, document_id: str, expected_revision: int) -> AccessDocument:
        document = self.get(document_id)
        if document is None:
            raise DocumentNotFound("document not found")
        if document.status != "active":
            raise DocumentArchived("document archived")
        if document.revision != expected_revision:
            raise DocumentRevisionConflict("document revision changed")
        return document

    def _active_grant(self, document_id: str, group_name: str) -> AccessGrant | None:
        rows = self._rows(
            f"SELECT {_access_grant_columns()} FROM {_table_name(self.grants_table)} "
            f"WHERE active_group_key = {_literal(_active_group_key(document_id, group_name), 'VARCHAR(512)')}"
        )
        return _access_grant_from_row(rows[0]) if rows else None

    def _duplicate(self, key: str, payload: str) -> AccessMutationResult | None:
        rows = self._rows(
            f"SELECT payload_hash, result_json FROM {_table_name(self.commands)} "
            f"WHERE idempotency_key = {_literal(key, 'VARCHAR(128)')}"
        )
        if not rows:
            return None
        if rows[0]["payload_hash"] != payload:
            raise IdempotencyConflict(
                "idempotency key was reused with different content"
            )
        return _access_result_from_json(rows[0]["result_json"], duplicate=True)

    def _command_assertion(self, key: str) -> str:
        return f"ASSERT NOT EXISTS (SELECT 1 FROM {_table_name(self.commands)} WHERE idempotency_key = {_literal(key, 'VARCHAR(128)')}), 'idempotency key already exists'"

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
        duplicate = self._run(
            [
                self._command_assertion(key),
                self._command_insert(key, payload, "create", result, recorded_at),
            ],
            key,
            payload,
            document.document_id,
            None,
        )
        return result if duplicate is None else duplicate

    def _active_assertion(self, document_id: str, revision: int) -> str:
        return (
            f"ASSERT EXISTS (SELECT 1 FROM {_table_name(self.documents)} WHERE "
            f"document_id = {_literal(document_id, 'VARCHAR(128)')} AND status = 'active' "
            f"AND revision = {revision}::BIGINT), 'document revision changed'"
        )

    def _command_insert(
        self,
        key: str,
        payload: str,
        kind: str,
        result: AccessMutationResult,
        recorded_at: datetime,
    ) -> str:
        return _insert_statement(
            self.commands,
            {
                "idempotency_key": key,
                "payload_hash": payload,
                "command_kind": kind,
                "result_json": _access_result_json(result),
                "recorded_at": recorded_at,
            },
        )

    def _run(
        self,
        statements: Sequence[str],
        key: str,
        payload: str,
        document_id: str,
        revision: int | None,
    ) -> AccessMutationResult | None:
        self._ensure_tables()
        try:
            _execute_xtdb_transaction(self.connection, statements)
        except Exception as exc:
            duplicate = self._duplicate(key, payload)
            if duplicate:
                return duplicate
            if revision is not None:
                self._active(document_id, revision)
            conflict = _access_assertion_message(exc)
            if conflict is not None:
                raise DocumentAccessError(conflict) from exc
            raise
        return None

    def _ensure_tables(self) -> None:
        for config in (self.documents, self.grants_table, self.commands):
            if self._rows(
                "SELECT table_name FROM information_schema.tables WHERE "
                f"table_schema = {_literal(config.schema, 'TEXT')} AND "
                f"table_name = {_literal(config.name, 'TEXT')}"
            ):
                continue
            bootstrap_row = {
                column.name: _bootstrap_value(column.data_type)
                for column in config.columns
            }
            _execute_xtdb_transaction(
                self.connection, [_insert_statement(config, bootstrap_row)]
            )
            _execute_xtdb_transaction(
                self.connection,
                [
                    f"ERASE FROM {_table_name(config)} WHERE {_where(config, {key: bootstrap_row[key] for key in config.primary_keys})}"
                ],
            )

    def _rows(self, sql: str) -> list[Mapping[str, object]]:
        try:
            return [dict(row) for row in self.connection.execute(text(sql)).mappings()]
        except Exception as exc:
            if not _is_xtdb_table_not_found_error(exc):
                raise
            _rollback_xtdb_connection(self.connection)
            return []


def _access_document_columns(prefix: str | None = None) -> str:
    names = (
        "document_id",
        "name",
        "normalized_name",
        "description",
        "status",
        "revision",
        "created_by",
        "created_at",
        "archived_by",
        "archived_at",
        "owning_group",
        "generation",
    )
    return ", ".join(f"{prefix}.{name}" if prefix else name for name in names)


def _access_grant_columns() -> str:
    return "grant_id, document_id, group_name, role, granted_by, granted_at, document_revision, revoked_by, revoked_at"


def _access_document_from_row(row: Mapping[str, object]) -> AccessDocument:
    return AccessDocument(
        document_id=str(row["document_id"]),
        name=str(row["name"]),
        normalized_name=str(row["normalized_name"]),
        description=None if row["description"] is None else str(row["description"]),
        status=str(row["status"]),
        revision=int(str(row["revision"])),
        created_by=str(row["created_by"]),
        created_at=_datetime(row["created_at"]),
        archived_by=None if row["archived_by"] is None else str(row["archived_by"]),
        archived_at=None
        if row["archived_at"] is None
        else _datetime(row["archived_at"]),
        owning_group=(
            None if row.get("owning_group") is None else str(row["owning_group"])
        ),
        generation=int(str(row.get("generation", 1))),
    )


def _access_grant_from_row(row: Mapping[str, object]) -> AccessGrant:
    return AccessGrant(
        grant_id=str(row["grant_id"]),
        document_id=str(row["document_id"]),
        group_name=str(row["group_name"]),
        role=str(row["role"]),
        granted_by=str(row["granted_by"]),
        granted_at=_datetime(row["granted_at"]),
        document_revision=int(str(row["document_revision"])),
        revoked_by=None if row["revoked_by"] is None else str(row["revoked_by"]),
        revoked_at=None if row["revoked_at"] is None else _datetime(row["revoked_at"]),
    )


def _datetime(value: object) -> datetime:
    return value if isinstance(value, datetime) else datetime.fromisoformat(str(value))


def _document_row(document: AccessDocument) -> dict[str, object]:
    return {
        "document_id": document.document_id,
        "name": document.name,
        "normalized_name": document.normalized_name,
        "description": document.description,
        "status": document.status,
        "revision": document.revision,
        "created_by": document.created_by,
        "created_at": document.created_at,
        "archived_by": document.archived_by,
        "archived_at": document.archived_at,
        "owning_group": document.owning_group,
        "generation": document.generation,
    }


def _grant_row(grant: AccessGrant) -> dict[str, object]:
    return {
        "grant_id": grant.grant_id,
        "document_id": grant.document_id,
        "group_name": grant.group_name,
        "active_group_key": None
        if grant.revoked_at is not None
        else _active_group_key(grant.document_id, grant.group_name),
        "role": grant.role,
        "granted_by": grant.granted_by,
        "granted_at": grant.granted_at,
        "document_revision": grant.document_revision,
        "revoked_by": grant.revoked_by,
        "revoked_at": grant.revoked_at,
    }


def _new_grant(
    document: AccessDocument,
    grant: AccessGrantInput,
    actor_id: str,
    recorded_at: datetime,
) -> AccessGrant:
    return AccessGrant(
        grant.grant_id,
        document.document_id,
        grant.group_name,
        grant.role,
        actor_id,
        recorded_at,
        document.revision,
    )


def _updated_document(
    document: AccessDocument,
    *,
    status: str | None = None,
    archived_by: str | None = None,
    archived_at: datetime | None = None,
) -> AccessDocument:
    return replace(
        document,
        revision=document.revision + 1,
        status=document.status if status is None else status,
        archived_by=document.archived_by if archived_by is None else archived_by,
        archived_at=document.archived_at if archived_at is None else archived_at,
    )


def _active_group_key(document_id: str, group_name: str) -> str:
    return f"{document_id}:{group_name.casefold()}"


def _access_update(
    config: TableConfig, document_id: str, values: Mapping[str, object]
) -> str:
    columns = {column.name: column.data_type for column in config.columns}
    assignments = ", ".join(
        f"{_xtdb_column_identifier(name)} = {_literal(value, columns[name])}"
        for name, value in values.items()
    )
    return (
        f"UPDATE {_table_name(config)} SET {assignments} WHERE "
        f"{_where(config, {next(iter(config.primary_keys)): document_id})}"
    )


def _access_result_json(result: AccessMutationResult) -> dict[str, object]:
    return {
        "document": {
            name: _json_value(value)
            for name, value in _document_row(result.document).items()
        },
        "grants": [
            {
                name: _json_value(value)
                for name, value in _grant_row(grant).items()
                if name != "active_group_key"
            }
            for grant in result.grants
        ],
    }


def _json_value(value: object) -> object:
    return value.isoformat() if isinstance(value, datetime) else value


def _access_result_from_json(value: object, *, duplicate: bool) -> AccessMutationResult:
    parsed = json.loads(value) if isinstance(value, str) else value
    if not isinstance(parsed, Mapping):
        raise TypeError("stored access command result is invalid")
    document = _access_document_from_row(parsed["document"])
    grants = tuple(_access_grant_from_row(grant) for grant in parsed["grants"])
    return AccessMutationResult(document, grants, accepted=False, duplicate=duplicate)


def _insert_statement(
    config: TableConfig,
    row: Mapping[str, object],
    *,
    valid_from: object | None = None,
    valid_to: object | None = None,
) -> str:
    columns = {column.name: column for column in config.columns}
    primary_keys = tuple(config.primary_keys)
    missing_keys = [key for key in primary_keys if key not in row]
    if missing_keys:
        raise ValueError("atomic rows require every configured primary key")
    document_id = _document_id(config, row)
    values = {"_id": document_id, **row}
    types = {
        "_id": _xtdb_document_id_cast_type(config),
        **{name: column.data_type for name, column in columns.items()},
    }
    if valid_from is not None:
        values["_valid_from"] = valid_from
        values["_valid_to"] = valid_to
        types.update(
            {
                "_valid_from": "TIMESTAMP WITH TIME ZONE",
                "_valid_to": "TIMESTAMP WITH TIME ZONE",
            }
        )
    column_sql = ", ".join(_xtdb_column_identifier(name) for name in values)
    value_sql = ", ".join(
        _literal(value, types[name]) for name, value in values.items()
    )
    return f"INSERT INTO {_table_name(config)} ({column_sql}) VALUES ({value_sql})"


def _update_statement(update: AtomicUpdate) -> str:
    columns = {column.name: column for column in update.table_config.columns}
    assignments = ", ".join(
        f"{_xtdb_column_identifier(name)} = {_literal(value, columns[name].data_type)}"
        for name, value in update.values.items()
    )
    return (
        f"UPDATE {_table_name(update.table_config)} SET {assignments} WHERE "
        f"{_where(update.table_config, {**update.key_values, **update.expected_values})}"
    )


def _document_id(config: TableConfig, row: Mapping[str, object]) -> object:
    return _xtdb_document_id_value(config, row)


def _where(config: TableConfig, values: Mapping[str, object]) -> str:
    types = {column.name: column.data_type for column in config.columns}
    predicates = []
    for name, value in values.items():
        if value is None:
            predicates.append(f"{_xtdb_column_identifier(name)} IS NULL")
        else:
            predicates.append(
                f"{_xtdb_column_identifier(name)} = {_literal(value, types[name])}"
            )
    return " AND ".join(predicates)


def _table_name(config: TableConfig) -> str:
    return _qualified_table_name(config.schema, config.name)


def _literal(value: object, data_type: str) -> str:
    if isinstance(value, (dict, list, tuple)):
        value = json.dumps(value, separators=(",", ":"))
    return _xtdb_sql_literal(value, _xtdb_cast_type(data_type))


def _encode(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


def _decode(value: str) -> bytes:
    return base64.b64decode(value)


def _accepted_at(prepared: PreparedCrdtCommit) -> datetime:
    if prepared.operations:
        return prepared.operations[0].recorded_at
    return datetime.now(UTC)


def _generation(prepared: PreparedCrdtCommit) -> int:
    return prepared.generation


def _bootstrap_value(data_type: str) -> object:
    normalized = data_type.upper()
    if normalized.startswith(("BINARY", "VARBINARY")) or normalized.endswith("BLOB"):
        return b""
    if normalized.startswith(
        (
            "BIGINT",
            "INT",
            "SMALLINT",
            "TINYINT",
            "DECIMAL",
            "NUMERIC",
            "FLOAT",
            "DOUBLE",
            "REAL",
        )
    ):
        return 0
    if normalized.startswith(("BOOL", "BOOLEAN")):
        return False
    if normalized.startswith(("DATETIME", "TIMESTAMP")):
        return datetime(1970, 1, 1, tzinfo=UTC)
    if normalized.startswith("DATE"):
        return date(1970, 1, 1)
    return ""
