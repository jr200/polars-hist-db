from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass, replace
from datetime import datetime
from hashlib import sha256

from .config import build_document_access_table_configs
from .crdt import RowGuard
from .pagination import Page, paginate
from .types import DocumentAccessStoreConfig


class DocumentAccessError(ValueError):
    pass


class DocumentNotFound(DocumentAccessError):
    pass


class DocumentArchived(DocumentAccessError):
    pass


class DocumentRevisionConflict(DocumentAccessError):
    pass


class IdempotencyConflict(DocumentAccessError):
    pass


@dataclass(frozen=True)
class AccessDocument:
    document_id: str
    name: str
    normalized_name: str
    description: str | None
    status: str
    revision: int
    created_by: str
    created_at: datetime
    archived_by: str | None = None
    archived_at: datetime | None = None
    owning_group: str | None = None
    generation: int = 1


@dataclass(frozen=True)
class AccessGrant:
    grant_id: str
    document_id: str
    group_name: str
    role: str
    granted_by: str
    granted_at: datetime
    document_revision: int
    revoked_by: str | None = None
    revoked_at: datetime | None = None

    @property
    def active(self) -> bool:
        return self.revoked_at is None


@dataclass(frozen=True)
class AccessGrantInput:
    grant_id: str
    group_name: str
    role: str


@dataclass(frozen=True)
class AccessMutationResult:
    document: AccessDocument
    grants: tuple[AccessGrant, ...]
    accepted: bool
    duplicate: bool = False


class InMemoryDocumentAccessStore:
    def __init__(self) -> None:
        self._documents: dict[str, AccessDocument] = {}
        self._grants: dict[str, AccessGrant] = {}
        self._commands: dict[str, tuple[str, AccessMutationResult]] = {}

    def create(
        self,
        document_id: str,
        name: str,
        description: str | None,
        actor_id: str,
        recorded_at: datetime,
        *,
        initial_grants: Iterable[AccessGrantInput] = (),
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
        if duplicate is not None:
            return duplicate
        normalized_name = _normalized(name)
        existing = next(
            (
                doc
                for doc in self._documents.values()
                if doc.owning_group == owning_group
                and doc.normalized_name == normalized_name
            ),
            None,
        )
        if existing is not None and allow_existing:
            result = _existing_result(
                existing,
                self._all_grants(existing.document_id),
                owning_group,
            )
            self._commands[idempotency_key] = (payload, result)
            return result
        if document_id in self._documents:
            raise DocumentAccessError("document already exists")
        if existing is not None:
            raise DocumentAccessError("document name already exists")
        _require_time(recorded_at)
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
        if len({grant.grant_id for grant in grants}) != len(grants) or len(
            {grant.group_name.casefold() for grant in grants}
        ) != len(grants):
            raise DocumentAccessError("initial grants must be unique")
        if any(grant.grant_id in self._grants for grant in grants):
            raise DocumentAccessError("grant already exists")
        self._documents[document_id] = document
        for grant in grants:
            self._insert_grant(document, grant, actor_id, recorded_at)
        return self._record(idempotency_key, payload, document)

    def get(self, document_id: str) -> AccessDocument | None:
        return self._documents.get(document_id)

    def guard(self, document_id: str, expected_revision: int) -> RowGuard:
        return RowGuard(
            build_document_access_table_configs(DocumentAccessStoreConfig())[0],
            {"document_id": document_id},
            {"status": "active", "revision": expected_revision},
        )

    def begin_purge(self, document_id: str, expected_generation: int) -> AccessDocument:
        document = self._documents.get(document_id)
        if document is None:
            raise DocumentNotFound("document not found")
        if document.generation != expected_generation or document.status not in {
            "active",
            "purging",
        }:
            raise DocumentRevisionConflict("document generation changed")
        updated = replace(document, status="purging")
        self._documents[document_id] = updated
        return updated

    def list_for_groups(
        self,
        groups: Iterable[str],
        *,
        include_archived: bool = False,
        cursor: str | None = None,
        limit: int = 100,
    ) -> Page[AccessDocument]:
        group_set = set(groups)
        document_ids = {
            grant.document_id
            for grant in self._grants.values()
            if grant.active and grant.group_name in group_set
        }
        return paginate(
            (
                doc
                for doc in self._documents.values()
                if doc.document_id in document_ids
                and (include_archived or doc.status == "active")
            ),
            _document_page_key,
            cursor=cursor,
            limit=limit,
        )

    def list_all(
        self,
        *,
        include_archived: bool = False,
        cursor: str | None = None,
        limit: int = 100,
    ) -> Page[AccessDocument]:
        return paginate(
            (
                doc
                for doc in self._documents.values()
                if include_archived or doc.status == "active"
            ),
            _document_page_key,
            cursor=cursor,
            limit=limit,
        )

    def grants(
        self,
        document_id: str,
        *,
        include_revoked: bool = False,
        cursor: str | None = None,
        limit: int = 100,
    ) -> Page[AccessGrant]:
        return paginate(
            self._all_grants(document_id, include_revoked=include_revoked),
            _grant_page_key,
            cursor=cursor,
            limit=limit,
        )

    def _all_grants(
        self, document_id: str, *, include_revoked: bool = False
    ) -> tuple[AccessGrant, ...]:
        return tuple(
            grant
            for grant in self._grants.values()
            if grant.document_id == document_id and (include_revoked or grant.active)
        )

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
        if duplicate is not None:
            return duplicate
        document = self._active(document_id, expected_revision)
        _require_time(recorded_at)
        if any(
            item.active and item.group_name.casefold() == grant.group_name.casefold()
            for item in self._all_grants(document_id)
        ):
            raise DocumentAccessError("group already has an active grant")
        if grant.grant_id in self._grants:
            raise DocumentAccessError("grant already exists")
        document = self._advance(document)
        self._documents[document_id] = document
        self._insert_grant(document, grant, actor_id, recorded_at)
        return self._record(idempotency_key, payload, document)

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
        if duplicate is not None:
            return duplicate
        document = self._active(document_id, expected_revision)
        _require_time(recorded_at)
        grant = next(
            (
                item
                for item in self._all_grants(document_id)
                if item.group_name == group_name
            ),
            None,
        )
        if grant is None:
            raise DocumentAccessError("active group grant not found")
        document = self._advance(document)
        self._documents[document_id] = document
        self._grants[grant.grant_id] = replace(
            grant,
            revoked_by=actor_id,
            revoked_at=recorded_at,
            document_revision=document.revision,
        )
        return self._record(idempotency_key, payload, document)

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
        if duplicate is not None:
            return duplicate
        document = self._active(document_id, expected_revision)
        _require_time(recorded_at)
        document = replace(
            self._advance(document),
            status="archived",
            archived_by=actor_id,
            archived_at=recorded_at,
        )
        self._documents[document_id] = document
        return self._record(idempotency_key, payload, document)

    def _active(self, document_id: str, expected_revision: int) -> AccessDocument:
        document = self._documents.get(document_id)
        if document is None:
            raise DocumentNotFound("document not found")
        if document.status != "active":
            raise DocumentArchived("document archived")
        if document.revision != expected_revision:
            raise DocumentRevisionConflict("document revision changed")
        return document

    def _insert_grant(
        self,
        document: AccessDocument,
        grant: AccessGrantInput,
        actor_id: str,
        recorded_at: datetime,
    ) -> None:
        if grant.grant_id in self._grants:
            raise DocumentAccessError("grant already exists")
        self._grants[grant.grant_id] = AccessGrant(
            grant.grant_id,
            document.document_id,
            grant.group_name,
            grant.role,
            actor_id,
            recorded_at,
            document.revision,
        )

    def _advance(self, document: AccessDocument) -> AccessDocument:
        return replace(document, revision=document.revision + 1)

    def _duplicate(
        self, idempotency_key: str, payload: str
    ) -> AccessMutationResult | None:
        prior = self._commands.get(idempotency_key)
        if prior is None:
            return None
        if prior[0] != payload:
            raise IdempotencyConflict(
                "idempotency key was reused with different content"
            )
        return replace(prior[1], accepted=False, duplicate=True)

    def _record(
        self, idempotency_key: str, payload: str, document: AccessDocument
    ) -> AccessMutationResult:
        result = AccessMutationResult(
            document,
            self._all_grants(document.document_id, include_revoked=True),
            accepted=True,
        )
        self._commands[idempotency_key] = (payload, result)
        return result


def _normalized(value: str) -> str:
    normalized = value.strip().casefold()
    if not normalized:
        raise DocumentAccessError("name is required")
    return normalized


def _document_page_key(document: AccessDocument) -> tuple[datetime, str]:
    return document.created_at, document.document_id


def _grant_page_key(grant: AccessGrant) -> tuple[datetime, str]:
    return grant.granted_at, grant.grant_id


def _create_payload(
    document_id: str,
    name: str,
    description: str | None,
    actor_id: str,
    grants: Iterable[AccessGrantInput],
    owning_group: str | None,
    allow_existing: bool,
) -> str:
    grants = tuple(grants)
    if allow_existing:
        return _payload(
            "create",
            name,
            description,
            actor_id,
            tuple((grant.group_name, grant.role) for grant in grants),
            owning_group,
            "allow_existing",
        )
    values: list[object] = [
        document_id,
        name,
        description,
        actor_id,
        grants,
        owning_group,
    ]
    return _payload("create", *values)


def _existing_result(
    document: AccessDocument,
    grants: Iterable[AccessGrant],
    owning_group: str | None,
) -> AccessMutationResult:
    if document.status != "active" or document.owning_group != owning_group:
        raise DocumentAccessError("document name already exists")
    return AccessMutationResult(
        document,
        tuple(grants),
        accepted=False,
        duplicate=True,
    )


def _require_time(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise DocumentAccessError("recorded_at must be timezone-aware")


def _payload(action: str, *values: object) -> str:
    return sha256(
        json.dumps(
            [action, *values], default=lambda value: value.__dict__, sort_keys=True
        ).encode()
    ).hexdigest()
