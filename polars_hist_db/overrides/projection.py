from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import polars as pl


@dataclass(frozen=True)
class ColumnOverride:
    entity_id: str
    column: str
    value: Any
    dtype: pl.DataType | None = None


def apply_columnar_overrides(
    frame: pl.DataFrame,
    *,
    entity_column: str,
    overrides: Iterable[ColumnOverride],
) -> pl.DataFrame:
    if entity_column not in frame.columns:
        raise ValueError(f"entity column {entity_column!r} does not exist")

    latest = {(override.entity_id, override.column): override for override in overrides}
    if not latest:
        return frame

    by_column: dict[str, list[ColumnOverride]] = {}
    for override in latest.values():
        by_column.setdefault(override.column, []).append(override)

    missing = [
        pl.lit(None, _override_dtype(values)).alias(column)
        for column, values in by_column.items()
        if column not in frame.columns
    ]
    if missing:
        frame = frame.with_columns(missing)

    entity_id = pl.col(entity_column).cast(pl.String)
    return frame.with_columns(
        [
            entity_id.replace_strict(
                [override.entity_id for override in values],
                [override.value for override in values],
                default=pl.col(column),
                return_dtype=frame.schema[column],
            ).alias(column)
            for column, values in by_column.items()
        ]
    )


def _override_dtype(overrides: list[ColumnOverride]) -> pl.DataType:
    declared = [override.dtype for override in overrides if override.dtype is not None]
    if declared:
        if any(dtype != declared[0] for dtype in declared[1:]):
            raise ValueError("conflicting dtypes for override column")
        return declared[0]

    inferred = pl.Series([override.value for override in overrides]).dtype
    if inferred == pl.Null:
        raise ValueError("dtype is required for an all-null override-only column")
    return inferred
