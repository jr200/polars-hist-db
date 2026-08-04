from __future__ import annotations

from collections.abc import Iterable
from dataclasses import replace

from .types import OverrideOperation


def override_recorded_time_sql(db_backend: str) -> str:
    return (
        "_system_from" if db_backend == "xtdb" else "COALESCE(recorded_at, valid_from)"
    )


def override_recorded_order_sql(db_backend: str) -> str:
    recorded = override_recorded_time_sql(db_backend)
    return (
        f"{recorded}, "
        "CASE WHEN operation_type IN ('close', 'system_close') THEN 0 ELSE 1 END, "
        "operation_id"
    )


def project_personal_override_operations(
    operations: Iterable[OverrideOperation],
) -> list[OverrideOperation]:
    projected: list[OverrideOperation] = []
    open_sets: dict[tuple[str, str, str], int] = {}
    for operation in operations:
        key = (operation.feed_id, operation.entity_id, operation.field_path)
        if operation.operation_type == "set":
            open_sets[key] = len(projected)
        elif operation.operation_type in {"close", "system_close"}:
            index = open_sets.pop(key, None)
            if index is not None:
                projected[index] = replace(
                    projected[index],
                    valid_to=max(operation.valid_from, projected[index].valid_from),
                )
        projected.append(operation)
    return projected
