from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from typing import Protocol
from uuid import uuid4

from .pagination import Page, paginate
from .types import OverrideOperation, OverrideOperationType, OverrideTypedValue


class OverrideLedgerStore(Protocol):
    def append(self, operation: OverrideOperation) -> None: ...

    def history_for_entity(
        self,
        owner_user_id: str,
        feed_id: str,
        entity_id: str,
        *,
        cursor: str | None = None,
        limit: int = 100,
    ) -> Page[OverrideOperation]: ...


class InMemoryOverrideLedgerStore:
    def __init__(self) -> None:
        self._operations: list[OverrideOperation] = []

    def append(self, operation: OverrideOperation) -> None:
        self._operations.append(operation)

    def history_for_entity(
        self,
        owner_user_id: str,
        feed_id: str,
        entity_id: str,
        *,
        cursor: str | None = None,
        limit: int = 100,
    ) -> Page[OverrideOperation]:
        return paginate(
            (
                operation
                for operation in self._operations
                if operation.owner_user_id == owner_user_id
                and operation.feed_id == feed_id
                and operation.entity_id == entity_id
            ),
            _operation_page_key,
            cursor=cursor,
            limit=limit,
        )


class OverrideLedger:
    def __init__(self, store: OverrideLedgerStore) -> None:
        self.store = store

    def set_field(
        self,
        *,
        owner_user_id: str,
        actor_user_id: str,
        feed_id: str,
        entity_id: str,
        field_path: str,
        value: OverrideTypedValue,
        observed_canonical_value_json: dict[str, object] | None,
        valid_from: datetime,
        change_set_id: str | None = None,
        created_against_stale_source: bool = False,
        comment: str | None = None,
        metadata_json: dict[str, object] | None = None,
    ) -> OverrideOperation:
        self._require_timezone(valid_from, "valid_from")
        resolved_change_set_id = change_set_id or self._id("chg")
        active = self.active_for_field(owner_user_id, feed_id, entity_id, field_path)
        if active is not None:
            self.store.append(
                self._operation(
                    change_set_id=resolved_change_set_id,
                    owner_user_id=owner_user_id,
                    actor_user_id=actor_user_id,
                    feed_id=feed_id,
                    entity_id=entity_id,
                    field_path=field_path,
                    operation_type="close",
                    value=None,
                    observed_canonical_value_json=observed_canonical_value_json,
                    created_against_stale_source=created_against_stale_source,
                    valid_from=valid_from,
                    valid_to=valid_from,
                    reason="replaced",
                    comment=None,
                    metadata_json=metadata_json or {},
                )
            )

        operation = self._operation(
            change_set_id=resolved_change_set_id,
            owner_user_id=owner_user_id,
            actor_user_id=actor_user_id,
            feed_id=feed_id,
            entity_id=entity_id,
            field_path=field_path,
            operation_type="set",
            value=value,
            observed_canonical_value_json=observed_canonical_value_json,
            created_against_stale_source=created_against_stale_source,
            valid_from=valid_from,
            valid_to=None,
            reason=None,
            comment=comment,
            metadata_json=metadata_json or {},
        )
        self.store.append(operation)
        return operation

    def close_field(
        self,
        *,
        owner_user_id: str,
        actor_user_id: str,
        feed_id: str,
        entity_id: str,
        field_path: str,
        valid_to: datetime,
        reason: str,
        change_set_id: str | None = None,
        comment: str | None = None,
        system: bool = False,
        metadata_json: dict[str, object] | None = None,
    ) -> OverrideOperation:
        self._require_timezone(valid_to, "valid_to")
        operation = self._operation(
            change_set_id=change_set_id or self._id("chg"),
            owner_user_id=owner_user_id,
            actor_user_id=actor_user_id,
            feed_id=feed_id,
            entity_id=entity_id,
            field_path=field_path,
            operation_type="system_close" if system else "close",
            value=None,
            observed_canonical_value_json=None,
            created_against_stale_source=False,
            valid_from=valid_to,
            valid_to=valid_to,
            reason=reason,
            comment=comment,
            metadata_json=metadata_json or {},
        )
        self.store.append(operation)
        return operation

    def active_for_entity(
        self, owner_user_id: str, feed_id: str, entity_id: str
    ) -> list[OverrideOperation]:
        active: dict[str, OverrideOperation] = {}
        for operation in self._all_history(owner_user_id, feed_id, entity_id):
            if operation.operation_type == "set":
                active[operation.field_path] = operation
            elif operation.operation_type in {"close", "system_close"}:
                active.pop(operation.field_path, None)
        return list(active.values())

    def active_for_field(
        self, owner_user_id: str, feed_id: str, entity_id: str, field_path: str
    ) -> OverrideOperation | None:
        for operation in self.active_for_entity(owner_user_id, feed_id, entity_id):
            if operation.field_path == field_path:
                return operation
        return None

    def history_for_entity(
        self,
        owner_user_id: str,
        feed_id: str,
        entity_id: str,
        *,
        cursor: str | None = None,
        limit: int = 100,
    ) -> Page[OverrideOperation]:
        return self.store.history_for_entity(
            owner_user_id,
            feed_id,
            entity_id,
            cursor=cursor,
            limit=limit,
        )

    def projected_history_for_entity(
        self,
        owner_user_id: str,
        feed_id: str,
        entity_id: str,
        *,
        cursor: str | None = None,
        limit: int = 100,
    ) -> Page[OverrideOperation]:
        history = self._all_history(owner_user_id, feed_id, entity_id)
        projected: list[OverrideOperation] = []
        open_sets: dict[str, int] = {}
        for operation in history:
            if operation.operation_type == "set":
                open_sets[operation.field_path] = len(projected)
            elif operation.operation_type in {"close", "system_close"}:
                open_index = open_sets.pop(operation.field_path, None)
                if open_index is not None:
                    projected[open_index] = replace(
                        projected[open_index],
                        valid_to=operation.valid_from,
                    )
            projected.append(operation)
        return paginate(projected, _operation_page_key, cursor=cursor, limit=limit)

    def _all_history(
        self, owner_user_id: str, feed_id: str, entity_id: str
    ) -> tuple[OverrideOperation, ...]:
        operations: list[OverrideOperation] = []
        cursor = None
        while True:
            page = self.history_for_entity(
                owner_user_id,
                feed_id,
                entity_id,
                cursor=cursor,
                limit=500,
            )
            operations.extend(page.items)
            if page.next_cursor is None:
                return tuple(operations)
            cursor = page.next_cursor

    def _operation(
        self,
        *,
        change_set_id: str,
        owner_user_id: str,
        actor_user_id: str,
        feed_id: str,
        entity_id: str,
        field_path: str,
        operation_type: OverrideOperationType,
        value: OverrideTypedValue | None,
        observed_canonical_value_json: dict[str, object] | None,
        created_against_stale_source: bool,
        valid_from: datetime,
        valid_to: datetime | None,
        reason: str | None,
        comment: str | None,
        metadata_json: dict[str, object],
    ) -> OverrideOperation:
        if operation_type == "set" and value is None:
            raise ValueError("set override operations require a value")
        if operation_type in {"close", "system_close"} and value is not None:
            raise ValueError("close override operations must not carry a value")
        if valid_to is not None and valid_to < valid_from:
            raise ValueError(
                "override valid_to must be greater than or equal to valid_from"
            )
        return OverrideOperation(
            operation_id=self._id("op"),
            change_set_id=change_set_id,
            owner_user_id=owner_user_id,
            actor_user_id=actor_user_id,
            feed_id=feed_id,
            entity_id=entity_id,
            field_path=field_path,
            operation_type=operation_type,
            value=value,
            observed_canonical_value_json=observed_canonical_value_json,
            created_against_stale_source=created_against_stale_source,
            valid_from=valid_from,
            valid_to=valid_to,
            reason=reason,
            comment=comment,
            metadata_json=metadata_json,
            recorded_at=datetime.now(UTC),
            actor_display_name=(
                str(metadata_json["actor_display_name"])
                if metadata_json.get("actor_display_name")
                else None
            ),
            document_id=(
                str(metadata_json["document_id"])
                if metadata_json.get("document_id")
                else None
            ),
            generation=int(str(metadata_json.get("generation", 1))),
        )

    @staticmethod
    def _id(prefix: str) -> str:
        return f"{prefix}_{uuid4().hex}"

    @staticmethod
    def _require_timezone(value: datetime, name: str) -> None:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{name} must be timezone-aware")


def _operation_page_key(operation: OverrideOperation) -> tuple[datetime, str]:
    return operation.recorded_at or operation.valid_from, operation.operation_id
