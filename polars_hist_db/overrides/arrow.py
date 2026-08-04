from __future__ import annotations

import logging
import math
import struct
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from datetime import time as datetime_time
from decimal import Decimal
from hashlib import sha256
from threading import RLock
from typing import Literal, Protocol, cast
from uuid import UUID

import polars as pl
import pyarrow as pa

from polars_hist_db.config import TableConfig
from polars_hist_db.observability import (
    arrow_override_sync_span,
    record_arrow_override_sync,
)
from polars_hist_db.utils.arrow import require_unique_arrow_field_names

from .crdt import RowGuard

LOGGER = logging.getLogger(__name__)

ARROW_OVERRIDE_FORMAT_VERSION = 1
_UUID = pa.binary(16)
_HASH = pa.binary(32)
_UTC_TIMESTAMP = pa.timestamp("us", tz="UTC")
_VALUE_FIELDS = {
    "null": None,
    "string": "string_value",
    "boolean": "boolean_value",
    "integer": "integer_value",
    "float": "float_value",
    "decimal": "decimal_value",
    "timestamp": "timestamp_value",
    "date": "date_value",
    "time": "time_value",
    "duration": "duration_value",
    "binary": "binary_value",
    "string_list": "string_list_value",
    "extension": "extension_payload",
}


def arrow_override_value_type() -> pa.StructType:
    return pa.struct(
        [
            pa.field("kind", pa.string()),
            pa.field("string_value", pa.string()),
            pa.field("boolean_value", pa.bool_()),
            pa.field("integer_value", pa.int64()),
            pa.field("float_value", pa.float64()),
            pa.field("decimal_value", pa.decimal128(38, 12)),
            pa.field("timestamp_value", _UTC_TIMESTAMP),
            pa.field("date_value", pa.date32()),
            pa.field("time_value", pa.time64("us")),
            pa.field("duration_value", pa.duration("us")),
            pa.field("binary_value", pa.binary()),
            pa.field("string_list_value", pa.list_(pa.string())),
            pa.field("extension_schema_id", pa.string()),
            pa.field("extension_payload", pa.binary()),
        ]
    )


def arrow_override_operation_schema() -> pa.Schema:
    """Canonical Arrow data-plane schema for proposed and committed operations."""
    fields: list[pa.Field] = [
        pa.field("format_version", pa.uint16(), nullable=False),
        pa.field("operation_id", _UUID, nullable=False),
        pa.field("change_set_id", _UUID, nullable=False),
        pa.field("layer_id", _UUID),
        pa.field("generation", pa.uint64()),
        pa.field("layer_revision", pa.uint64()),
        pa.field("feed_id", pa.string(), nullable=False),
        pa.field("entity_id", pa.string(), nullable=False),
        pa.field("field_path", pa.string(), nullable=False),
        pa.field("operation_type", pa.string(), nullable=False),
        pa.field("value", arrow_override_value_type()),
        pa.field("unit", pa.string()),
        pa.field("supersedes_ids", pa.list_(_UUID), nullable=False),
        pa.field("removes_ids", pa.list_(_UUID), nullable=False),
        pa.field("valid_from", _UTC_TIMESTAMP, nullable=False),
        pa.field("valid_to", _UTC_TIMESTAMP),
        pa.field("observed_value", arrow_override_value_type()),
        pa.field("source_drift", pa.bool_(), nullable=False),
        pa.field("comment", pa.string()),
        pa.field("actor_subject", pa.string()),
        pa.field("actor_display_name", pa.string()),
        pa.field("recorded_at", _UTC_TIMESTAMP),
        pa.field("payload_hash", _HASH),
    ]
    return pa.schema(fields, metadata={b"polars_hist_db.override_operations": b"1"})


def arrow_override_ack_schema() -> pa.Schema:
    fields: list[pa.Field] = [
        pa.field("operation_id", _UUID, nullable=False),
        pa.field("status", pa.string(), nullable=False),
        pa.field("layer_revision", pa.uint64(), nullable=False),
        pa.field("payload_hash", _HASH, nullable=False),
    ]
    return pa.schema(
        fields, metadata={b"polars_hist_db.override_acknowledgements": b"1"}
    )


def arrow_override_projection_schema() -> pa.Schema:
    fields: list[pa.Field] = [
        pa.field("layer_id", _UUID, nullable=False),
        pa.field("generation", pa.uint64(), nullable=False),
        pa.field("layer_revision", pa.uint64(), nullable=False),
        pa.field("feed_id", pa.string(), nullable=False),
        pa.field("entity_id", pa.string(), nullable=False),
        pa.field("field_path", pa.string(), nullable=False),
        pa.field("frontier_state", pa.string(), nullable=False),
        pa.field("value", arrow_override_value_type()),
        pa.field("unit", pa.string()),
        pa.field("winning_operation_ids", pa.list_(_UUID), nullable=False),
        pa.field("source_drift", pa.bool_(), nullable=False),
    ]
    return pa.schema(fields, metadata={b"polars_hist_db.override_projection": b"1"})


class ArrowOverrideContractError(ValueError):
    pass


class ArrowOverrideGenerationChanged(ArrowOverrideContractError):
    pass


class ArrowOverrideLayerNotFound(ArrowOverrideContractError):
    pass


class ArrowOverrideRevisionConflict(ArrowOverrideContractError):
    pass


class ArrowOverridePreconditionFailed(ArrowOverrideContractError):
    pass


@dataclass(frozen=True)
class ArrowOverrideSyncResult:
    """Typed acknowledgement plus complete operation history for changed scopes."""

    generation: int
    revision: int
    acknowledgements: pa.Table
    # Includes prior operations needed to reconstruct each changed CRDT frontier.
    operations_delta: pa.Table
    projection_delta: pa.Table


class ArrowOverrideOperationStore(Protocol):
    def sync(
        self,
        *,
        layer_id: UUID,
        generation: int,
        known_revision: int,
        pending: pa.Table,
        actor_subject: str,
        actor_display_name: str | None,
        recorded_at: datetime,
        valid_at: datetime | None = None,
        guards: Sequence[RowGuard] = (),
    ) -> ArrowOverrideSyncResult: ...


@dataclass(frozen=True)
class ArrowOverrideLayerHead:
    generation: int
    revision: int


class ArrowOverrideRepository(Protocol):
    """Bounded persistence primitives required by the Arrow CRDT service."""

    backend: str

    def create_layer(self, layer_id: UUID, *, generation: int = 1) -> None: ...

    def head(self, layer_id: UUID) -> ArrowOverrideLayerHead | None: ...

    def operations_by_ids(
        self, layer_id: UUID, generation: int, operation_ids: set[bytes]
    ) -> pa.Table: ...

    def changed_scopes(
        self,
        layer_id: UUID,
        generation: int,
        *,
        after_revision: int,
        through_revision: int,
    ) -> set[tuple[str, str, str]]: ...

    def operations_for_scopes(
        self,
        layer_id: UUID,
        generation: int,
        scopes: set[tuple[str, str, str]],
        *,
        through_revision: int,
    ) -> pa.Table: ...

    def append_if_revision(
        self,
        layer_id: UUID,
        generation: int,
        expected_revision: int,
        committed: pa.Table,
        updated_at: datetime,
        guards: Sequence[RowGuard] = (),
    ) -> bool: ...

    def reset_generation(self, layer_id: UUID, generation: int) -> None: ...


class RepositoryArrowOverrideOperationStore:
    """Backend-neutral Arrow CRDT service over an atomic repository."""

    def __init__(
        self, repository: ArrowOverrideRepository, *, commit_retries: int = 8
    ) -> None:
        if commit_retries < 1:
            raise ValueError("commit_retries must be positive")
        self.repository = repository
        self.commit_retries = commit_retries

    def sync(
        self,
        *,
        layer_id: UUID,
        generation: int,
        known_revision: int,
        pending: pa.Table,
        actor_subject: str,
        actor_display_name: str | None,
        recorded_at: datetime,
        valid_at: datetime | None = None,
        guards: Sequence[RowGuard] = (),
    ) -> ArrowOverrideSyncResult:
        backend = str(getattr(self.repository, "backend", "custom"))
        started = time.perf_counter()
        with arrow_override_sync_span(
            backend=backend, pending_rows=pending.num_rows
        ) as span:
            try:
                result = self._sync(
                    layer_id=layer_id,
                    generation=generation,
                    known_revision=known_revision,
                    pending=pending,
                    actor_subject=actor_subject,
                    actor_display_name=actor_display_name,
                    recorded_at=recorded_at,
                    valid_at=valid_at,
                    guards=guards,
                )
            except Exception:
                duration = time.perf_counter() - started
                record_arrow_override_sync(
                    backend=backend,
                    outcome="rejected",
                    duration_seconds=duration,
                    pending=pending.num_rows,
                    accepted=0,
                    duplicates=0,
                    projection_rows=0,
                    conflicts=0,
                )
                LOGGER.warning(
                    "Arrow override synchronization rejected backend=%s "
                    "pending=%d duration_seconds=%.6f",
                    backend,
                    pending.num_rows,
                    duration,
                )
                raise
            statuses = result.acknowledgements["status"].to_pylist()
            accepted = statuses.count("accepted")
            duplicates = statuses.count("duplicate")
            states = result.projection_delta["frontier_state"].to_pylist()
            conflicts = states.count("conflict")
            duration = time.perf_counter() - started
            record_arrow_override_sync(
                backend=backend,
                outcome="accepted",
                duration_seconds=duration,
                pending=pending.num_rows,
                accepted=accepted,
                duplicates=duplicates,
                projection_rows=result.projection_delta.num_rows,
                conflicts=conflicts,
            )
            if span is not None:
                span.set_attribute("override.accepted_rows", accepted)
                span.set_attribute("override.duplicate_rows", duplicates)
                span.set_attribute(
                    "override.projection_rows", result.projection_delta.num_rows
                )
                span.set_attribute("override.conflicts", conflicts)
                span.set_attribute("override.revision", result.revision)
            LOGGER.info(
                "Arrow override synchronization completed backend=%s pending=%d "
                "accepted=%d duplicates=%d projection_rows=%d conflicts=%d "
                "duration_seconds=%.6f",
                backend,
                pending.num_rows,
                accepted,
                duplicates,
                result.projection_delta.num_rows,
                conflicts,
                duration,
            )
            return result

    def _sync(
        self,
        *,
        layer_id: UUID,
        generation: int,
        known_revision: int,
        pending: pa.Table,
        actor_subject: str,
        actor_display_name: str | None,
        recorded_at: datetime,
        valid_at: datetime | None = None,
        guards: Sequence[RowGuard] = (),
    ) -> ArrowOverrideSyncResult:
        validate_arrow_override_operations(pending, authority="client")
        _require_utc(recorded_at, "recorded_at")
        if known_revision < 0:
            raise ArrowOverrideContractError("known revision cannot be negative")
        requested_ids = {
            pending["operation_id"][index].as_py() for index in range(pending.num_rows)
        } | {
            reference
            for index in range(pending.num_rows)
            for reference in (
                pending["supersedes_ids"][index].as_py()
                + pending["removes_ids"][index].as_py()
            )
        }
        for _ in range(self.commit_retries):
            head = self.repository.head(layer_id)
            if head is None:
                raise ArrowOverrideLayerNotFound("layer_not_found")
            if generation != head.generation:
                raise ArrowOverrideGenerationChanged("generation_changed")
            if known_revision > head.revision:
                raise ArrowOverrideContractError(
                    "known revision is ahead of the server"
                )
            existing = self.repository.operations_by_ids(
                layer_id, generation, requested_ids
            )
            accepted_indices, duplicate_indices = _plan_pending(pending, existing)
            accepted = pending.take(pa.array(accepted_indices, type=pa.int64()))
            revision = head.revision
            if accepted.num_rows:
                revision += 1
                committed = finalize_arrow_override_operations(
                    accepted,
                    layer_id=layer_id,
                    generation=generation,
                    layer_revision=revision,
                    actor_subject=actor_subject,
                    actor_display_name=actor_display_name,
                    recorded_at=recorded_at,
                )
                if not self.repository.append_if_revision(
                    layer_id,
                    generation,
                    head.revision,
                    committed,
                    recorded_at,
                    guards,
                ):
                    continue
            else:
                committed = empty_arrow_override_operations()
            acknowledged = pa.concat_tables([existing, committed])
            acknowledgements = _acknowledgements(
                acknowledged,
                pending,
                accepted_indices=accepted_indices,
                duplicate_indices=duplicate_indices,
            )
            changed = self.repository.changed_scopes(
                layer_id,
                generation,
                after_revision=known_revision,
                through_revision=revision,
            )
            operations = self.repository.operations_for_scopes(
                layer_id,
                generation,
                changed,
                through_revision=revision,
            )
            projection = project_arrow_override_operations(
                operations,
                affected_scopes=changed,
                valid_at=valid_at or recorded_at,
                revision=revision,
            )
            return ArrowOverrideSyncResult(
                generation, revision, acknowledgements, operations, projection
            )
        raise ArrowOverrideRevisionConflict(
            "layer revision changed repeatedly during override commit"
        )


def empty_arrow_override_operations() -> pa.Table:
    return pa.Table.from_batches([], schema=arrow_override_operation_schema())


def encode_arrow_override_operations(table: pa.Table) -> bytes:
    require_unique_arrow_field_names(
        table.schema, context="Arrow override operation schema"
    )
    validate_arrow_override_operations(table, authority="either")
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, arrow_override_operation_schema()) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def decode_arrow_override_operations(payload: bytes) -> pa.Table:
    try:
        table = pa.ipc.open_stream(payload).read_all()
    except pa.ArrowException as exc:
        raise ArrowOverrideContractError("invalid Arrow IPC operation batch") from exc
    require_unique_arrow_field_names(
        table.schema, context="Arrow override operation schema"
    )
    validate_arrow_override_operations(table, authority="either")
    return table


def encode_arrow_override_acknowledgements(table: pa.Table) -> bytes:
    _validate_acknowledgements(table)
    return _encode_arrow_table(table, arrow_override_ack_schema())


def decode_arrow_override_acknowledgements(payload: bytes) -> pa.Table:
    table = _decode_arrow_table(payload, arrow_override_ack_schema(), "acknowledgement")
    _validate_acknowledgements(table)
    return table


def encode_arrow_override_projection(table: pa.Table) -> bytes:
    _validate_projection(table)
    return _encode_arrow_table(table, arrow_override_projection_schema())


def decode_arrow_override_projection(payload: bytes) -> pa.Table:
    table = _decode_arrow_table(
        payload, arrow_override_projection_schema(), "projection"
    )
    _validate_projection(table)
    return table


def _encode_arrow_table(table: pa.Table, schema: pa.Schema) -> bytes:
    require_unique_arrow_field_names(table.schema, context="Arrow IPC schema")
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def _decode_arrow_table(
    payload: bytes, schema: pa.Schema, description: str
) -> pa.Table:
    try:
        table = pa.ipc.open_stream(payload).read_all()
    except pa.ArrowException as exc:
        raise ArrowOverrideContractError(
            f"invalid Arrow IPC override {description}"
        ) from exc
    require_unique_arrow_field_names(
        table.schema, context=f"Arrow {description} schema"
    )
    if not table.schema.equals(schema, check_metadata=True):
        raise ArrowOverrideContractError(
            f"Arrow override {description} schema mismatch"
        )
    return table


def _validate_acknowledgements(table: pa.Table) -> None:
    if not table.schema.equals(arrow_override_ack_schema(), check_metadata=True):
        raise ArrowOverrideContractError(
            "Arrow override acknowledgement schema mismatch"
        )
    if any(table[name].null_count for name in table.column_names):
        raise ArrowOverrideContractError(
            "Arrow override acknowledgements cannot be null"
        )
    if any(
        status not in {"accepted", "duplicate"}
        for status in table["status"].to_pylist()
    ):
        raise ArrowOverrideContractError(
            "unsupported Arrow override acknowledgement status"
        )
    operation_ids = cast(list[bytes], table["operation_id"].to_pylist())
    if len(operation_ids) != len(set(operation_ids)):
        raise ArrowOverrideContractError(
            "Arrow override acknowledgement operation IDs must be unique"
        )
    revisions = cast(list[int], table["layer_revision"].to_pylist())
    if any(revision < 1 for revision in revisions):
        raise ArrowOverrideContractError("acknowledgement revisions must be positive")


def _validate_projection(table: pa.Table) -> None:
    if not table.schema.equals(arrow_override_projection_schema(), check_metadata=True):
        raise ArrowOverrideContractError("Arrow override projection schema mismatch")
    required = [field.name for field in table.schema if not field.nullable]
    if any(table[name].null_count for name in required):
        raise ArrowOverrideContractError(
            "Arrow override projection is missing required values"
        )
    states = table["frontier_state"].to_pylist()
    if any(state not in {"none", "clean", "conflict"} for state in states):
        raise ArrowOverrideContractError("unsupported Arrow override frontier state")
    scopes: set[tuple[str, str, str]] = set()
    for index, state in enumerate(states):
        scope = cast(
            tuple[str, str, str],
            tuple(
                table[name][index].as_py()
                for name in ("feed_id", "entity_id", "field_path")
            ),
        )
        if scope in scopes:
            raise ArrowOverrideContractError(
                "Arrow override projection scopes must be unique"
            )
        scopes.add(scope)
        value_valid = table["value"][index].is_valid
        winners = table["winning_operation_ids"][index].as_py()
        if (state == "clean") != value_valid:
            raise ArrowOverrideContractError(
                "only clean Arrow override frontiers may contain a value"
            )
        if value_valid:
            _validate_value(table["value"][index].as_py())
        if (state == "none" and winners) or (state != "none" and not winners):
            raise ArrowOverrideContractError(
                "Arrow override frontier provenance does not match its state"
            )


def arrow_override_storage_frames(
    committed: pa.Table,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Flatten a committed batch into typed relational operation, reference and list rows."""
    validate_arrow_override_operations(committed, authority="committed")
    frame = pl.from_arrow(committed)
    assert isinstance(frame, pl.DataFrame)
    value_fields = [
        pl.col(source).struct.field(field).alias(f"{role}_{name}")
        for role, source in (("value", "value"), ("observed", "observed_value"))
        for name, field in (
            ("kind", "kind"),
            ("string", "string_value"),
            ("boolean", "boolean_value"),
            ("integer", "integer_value"),
            ("float", "float_value"),
            ("decimal", "decimal_value"),
            ("timestamp", "timestamp_value"),
            ("date", "date_value"),
            ("time", "time_value"),
            ("duration_us", "duration_value"),
            ("binary", "binary_value"),
            ("extension_schema_id", "extension_schema_id"),
            ("extension_payload", "extension_payload"),
        )
    ]
    operations = frame.select(
        "operation_id",
        "format_version",
        "change_set_id",
        "layer_id",
        "generation",
        "layer_revision",
        "feed_id",
        "entity_id",
        "field_path",
        "operation_type",
        *value_fields,
        "unit",
        "source_drift",
        "valid_from",
        "valid_to",
        "comment",
        "actor_subject",
        "actor_display_name",
        "recorded_at",
        "payload_hash",
    ).with_columns(
        pl.col("format_version").cast(pl.Int64),
        pl.col("generation").cast(pl.Int64),
        pl.col("layer_revision").cast(pl.Int64),
        pl.col("value_duration_us").cast(pl.Int64),
        pl.col("observed_duration_us").cast(pl.Int64),
    )
    references = pl.concat(
        [
            _explode_list_column(frame, "supersedes_ids", "referenced_operation_id")
            .with_columns(pl.lit("supersedes").alias("reference_kind"))
            .select(
                "operation_id",
                "reference_kind",
                "ordinal",
                "referenced_operation_id",
            ),
            _explode_list_column(frame, "removes_ids", "referenced_operation_id")
            .with_columns(pl.lit("removes").alias("reference_kind"))
            .select(
                "operation_id",
                "reference_kind",
                "ordinal",
                "referenced_operation_id",
            ),
        ],
        how="vertical_relaxed",
    )
    string_lists = pl.concat(
        [
            _explode_list_column(
                frame.select(
                    "operation_id",
                    pl.col(source).struct.field("string_list_value").alias("items"),
                ),
                "items",
                "value",
            )
            .with_columns(pl.lit(role).alias("value_role"))
            .select("operation_id", "value_role", "ordinal", "value")
            for role, source in (
                ("value", "value"),
                ("observed", "observed_value"),
            )
        ],
        how="vertical_relaxed",
    )
    return operations, references, string_lists


def arrow_override_operations_from_storage(
    operations: pl.DataFrame,
    references: pl.DataFrame,
    string_lists: pl.DataFrame,
) -> pa.Table:
    """Reconstruct the canonical Arrow batch without converting operation rows to dicts."""
    frame = operations
    for kind, column in (("supersedes", "supersedes_ids"), ("removes", "removes_ids")):
        grouped = (
            references.filter(pl.col("reference_kind") == kind)
            .sort("operation_id", "ordinal")
            .group_by("operation_id", maintain_order=True)
            .agg(pl.col("referenced_operation_id").alias(column))
        )
        frame = frame.join(grouped, on="operation_id", how="left")
    for role in ("value", "observed"):
        grouped = (
            string_lists.filter(pl.col("value_role") == role)
            .sort("operation_id", "ordinal")
            .group_by("operation_id", maintain_order=True)
            .agg(pl.col("value").alias(f"{role}_string_list_value"))
        )
        frame = frame.join(grouped, on="operation_id", how="left")
    empty_ids = pl.lit([], dtype=pl.List(pl.Binary))
    frame = frame.with_columns(
        pl.col("supersedes_ids").fill_null(empty_ids),
        pl.col("removes_ids").fill_null(empty_ids),
    )
    values = {
        role: pl.when(pl.col(f"{role}_kind").is_null())
        .then(pl.lit(None, dtype=_polars_value_type()))
        .otherwise(
            pl.struct(
                pl.col(f"{role}_kind").alias("kind"),
                pl.col(f"{role}_string").alias("string_value"),
                pl.col(f"{role}_boolean").alias("boolean_value"),
                pl.col(f"{role}_integer").alias("integer_value"),
                pl.col(f"{role}_float").alias("float_value"),
                pl.col(f"{role}_decimal").alias("decimal_value"),
                pl.col(f"{role}_timestamp").alias("timestamp_value"),
                pl.col(f"{role}_date").alias("date_value"),
                pl.col(f"{role}_time").alias("time_value"),
                pl.col(f"{role}_duration_us")
                .cast(pl.Duration("us"))
                .alias("duration_value"),
                pl.col(f"{role}_binary").alias("binary_value"),
                pl.col(f"{role}_string_list_value").alias("string_list_value"),
                pl.col(f"{role}_extension_schema_id").alias("extension_schema_id"),
                pl.col(f"{role}_extension_payload").alias("extension_payload"),
            )
        )
        .alias(role)
        for role in ("value", "observed")
    }
    restored = frame.select(
        "format_version",
        "operation_id",
        "change_set_id",
        "layer_id",
        "generation",
        "layer_revision",
        "feed_id",
        "entity_id",
        "field_path",
        "operation_type",
        values["value"],
        "unit",
        "supersedes_ids",
        "removes_ids",
        "valid_from",
        "valid_to",
        values["observed"].alias("observed_value"),
        "source_drift",
        "comment",
        "actor_subject",
        "actor_display_name",
        "recorded_at",
        "payload_hash",
    )
    table = restored.to_arrow().cast(arrow_override_operation_schema())
    validate_arrow_override_operations(table, authority="committed")
    return table


def _explode_list_column(frame: pl.DataFrame, source: str, output: str) -> pl.DataFrame:
    return (
        frame.select(
            "operation_id",
            pl.col(source).alias(output),
            pl.int_ranges(0, pl.col(source).list.len()).alias("ordinal"),
        )
        .explode(output, "ordinal", empty_as_null=True)
        .filter(pl.col(output).is_not_null())
        .with_columns(pl.col("ordinal").cast(pl.Int64))
    )


def _polars_value_type() -> pl.Struct:
    return pl.Struct(
        [
            pl.Field("kind", pl.String),
            pl.Field("string_value", pl.String),
            pl.Field("boolean_value", pl.Boolean),
            pl.Field("integer_value", pl.Int64),
            pl.Field("float_value", pl.Float64),
            pl.Field("decimal_value", pl.Decimal(38, 12)),
            pl.Field("timestamp_value", pl.Datetime("us", "UTC")),
            pl.Field("date_value", pl.Date),
            pl.Field("time_value", pl.Time),
            pl.Field("duration_value", pl.Duration("us")),
            pl.Field("binary_value", pl.Binary),
            pl.Field("string_list_value", pl.List(pl.String)),
            pl.Field("extension_schema_id", pl.String),
            pl.Field("extension_payload", pl.Binary),
        ]
    )


def validate_arrow_override_operations(
    table: pa.Table, *, authority: Literal["client", "committed", "either"]
) -> None:
    expected = arrow_override_operation_schema()
    if not table.schema.equals(expected, check_metadata=True):
        raise ArrowOverrideContractError(
            f"override operation schema mismatch: expected {expected}, received {table.schema}"
        )
    table = table.combine_chunks()
    required = [field.name for field in expected if not field.nullable]
    if any(table[name].null_count for name in required):
        raise ArrowOverrideContractError(
            "override operation batch is missing required values"
        )
    authoritative = (
        "layer_id",
        "generation",
        "layer_revision",
        "actor_subject",
        "actor_display_name",
        "recorded_at",
        "payload_hash",
    )
    is_client = all(table[name].null_count == table.num_rows for name in authoritative)
    required_committed = tuple(
        name for name in authoritative if name != "actor_display_name"
    )
    is_committed = all(table[name].null_count == 0 for name in required_committed)
    if authority == "either":
        if not is_client and not is_committed:
            raise ArrowOverrideContractError(
                "operation batch mixes proposal and committed authority"
            )
        authority = "client" if is_client else "committed"
    if authority == "client" and not is_client:
        raise ArrowOverrideContractError(
            "client operation batch contains authoritative fields"
        )
    if authority == "committed" and not is_committed:
        raise ArrowOverrideContractError(
            "committed operation batch is missing authoritative fields"
        )

    seen: set[bytes] = set()
    for index in range(table.num_rows):
        version = table["format_version"][index].as_py()
        if version != ARROW_OVERRIDE_FORMAT_VERSION:
            raise ArrowOverrideContractError(
                "unsupported Arrow override format version"
            )
        operation_id = table["operation_id"][index].as_py()
        if operation_id in seen:
            raise ArrowOverrideContractError(
                "operation IDs must be unique within a batch"
            )
        seen.add(operation_id)
        operation_type = table["operation_type"][index].as_py()
        if any(
            not table[name][index].as_py()
            for name in ("feed_id", "entity_id", "field_path")
        ):
            raise ArrowOverrideContractError("operation scope fields cannot be empty")
        value = table["value"][index]
        supersedes = table["supersedes_ids"][index].as_py()
        removes = table["removes_ids"][index].as_py()
        if operation_type == "set":
            if not value.is_valid or removes:
                raise ArrowOverrideContractError(
                    "set operations require a value and cannot remove operations"
                )
        elif operation_type == "remove":
            if value.is_valid or supersedes or table["valid_to"][index].is_valid:
                raise ArrowOverrideContractError(
                    "remove operations cannot carry a value, supersession, or valid_to"
                )
        else:
            raise ArrowOverrideContractError(
                "unsupported Arrow override operation type"
            )
        valid_from = table["valid_from"][index].as_py()
        valid_to = table["valid_to"][index].as_py()
        if valid_to is not None and valid_to <= valid_from:
            raise ArrowOverrideContractError("valid_to must be greater than valid_from")
        references = supersedes + removes
        if len(references) != len(set(references)) or operation_id in references:
            raise ArrowOverrideContractError(
                "operation references must be unique and cannot reference self"
            )
        if value.is_valid:
            _validate_value(value.as_py())
        observed = table["observed_value"][index]
        if observed.is_valid:
            _validate_value(observed.as_py())
        if authority == "committed":
            if (
                table["generation"][index].as_py() < 1
                or table["layer_revision"][index].as_py() < 1
            ):
                raise ArrowOverrideContractError(
                    "generation and layer revision must be positive"
                )
            if not table["actor_subject"][index].as_py():
                raise ArrowOverrideContractError(
                    "committed operations require actor_subject"
                )
            expected_hash = _committed_payload_hash(table, index)
            if table["payload_hash"][index].as_py() != expected_hash:
                raise ArrowOverrideContractError(
                    "operation payload hash does not match typed content"
                )


def finalize_arrow_override_operations(
    pending: pa.Table,
    *,
    layer_id: UUID,
    generation: int,
    layer_revision: int,
    actor_subject: str,
    actor_display_name: str | None,
    recorded_at: datetime,
) -> pa.Table:
    validate_arrow_override_operations(pending, authority="client")
    _require_utc(recorded_at, "recorded_at")
    count = pending.num_rows
    result = pending.combine_chunks()
    replacements: dict[str, pa.Array] = {
        "layer_id": pa.array([layer_id.bytes] * count, type=_UUID),
        "generation": pa.array([generation] * count, type=pa.uint64()),
        "layer_revision": pa.array([layer_revision] * count, type=pa.uint64()),
        "actor_subject": pa.array([actor_subject] * count, type=pa.string()),
        "actor_display_name": pa.array([actor_display_name] * count, type=pa.string()),
        "recorded_at": pa.array([recorded_at] * count, type=_UTC_TIMESTAMP),
    }
    for name, values in replacements.items():
        result = result.set_column(result.schema.get_field_index(name), name, values)
    hashes = pa.array(
        [_committed_payload_hash(result, index) for index in range(count)], type=_HASH
    )
    result = result.set_column(
        result.schema.get_field_index("payload_hash"), "payload_hash", hashes
    )
    validate_arrow_override_operations(result, authority="committed")
    return result


class InMemoryArrowOverrideRepository:
    """Thread-safe reference repository used for parity and contract tests."""

    def __init__(self) -> None:
        self.backend = "memory"
        self._heads: dict[bytes, ArrowOverrideLayerHead] = {}
        self._operations: dict[tuple[bytes, int], pa.Table] = {}
        self._guard_rows: dict[
            tuple[str, str, tuple[tuple[str, object], ...]], dict[str, object]
        ] = {}
        self._lock = RLock()

    def create_layer(self, layer_id: UUID, *, generation: int = 1) -> None:
        if generation < 1:
            raise ArrowOverrideContractError("generation must be positive")
        with self._lock:
            key = layer_id.bytes
            if key in self._heads:
                raise ArrowOverrideContractError("layer already exists")
            self._heads[key] = ArrowOverrideLayerHead(generation, 0)
            self._operations[(key, generation)] = empty_arrow_override_operations()

    def head(self, layer_id: UUID) -> ArrowOverrideLayerHead | None:
        with self._lock:
            return self._heads.get(layer_id.bytes)

    def operations_by_ids(
        self, layer_id: UUID, generation: int, operation_ids: set[bytes]
    ) -> pa.Table:
        return self._filter(
            layer_id,
            generation,
            lambda table, index: table["operation_id"][index].as_py() in operation_ids,
        )

    def changed_scopes(
        self,
        layer_id: UUID,
        generation: int,
        *,
        after_revision: int,
        through_revision: int,
    ) -> set[tuple[str, str, str]]:
        with self._lock:
            table = self._table(layer_id, generation)
            return {
                _scope(table, index)
                for index in range(table.num_rows)
                if after_revision
                < table["layer_revision"][index].as_py()
                <= through_revision
            }

    def operations_for_scopes(
        self,
        layer_id: UUID,
        generation: int,
        scopes: set[tuple[str, str, str]],
        *,
        through_revision: int,
    ) -> pa.Table:
        return self._filter(
            layer_id,
            generation,
            lambda table, index: (
                _scope(table, index) in scopes
                and table["layer_revision"][index].as_py() <= through_revision
            ),
        )

    def append_if_revision(
        self,
        layer_id: UUID,
        generation: int,
        expected_revision: int,
        committed: pa.Table,
        updated_at: datetime,
        guards: Sequence[RowGuard] = (),
    ) -> bool:
        del updated_at
        validate_arrow_override_operations(committed, authority="committed")
        with self._lock:
            self._validate_guards(guards)
            head = self._heads.get(layer_id.bytes)
            if head != ArrowOverrideLayerHead(generation, expected_revision):
                return False
            next_revision = expected_revision + 1
            if any(
                committed["layer_revision"][index].as_py() != next_revision
                for index in range(committed.num_rows)
            ):
                raise ArrowOverrideContractError(
                    "committed batch does not advance exactly one revision"
                )
            key = (layer_id.bytes, generation)
            self._operations[key] = pa.concat_tables([self._operations[key], committed])
            self._heads[layer_id.bytes] = ArrowOverrideLayerHead(
                generation, next_revision
            )
            return True

    def seed_guard_row(
        self, table_config: TableConfig, row: Mapping[str, object]
    ) -> None:
        with self._lock:
            self._guard_rows[self._guard_key(table_config, row)] = dict(row)

    def _validate_guards(self, guards: Sequence[RowGuard]) -> None:
        for guard in guards:
            row = self._guard_rows.get(
                self._guard_key(guard.table_config, guard.key_values)
            )
            if row is None or any(
                row.get(name) != value for name, value in guard.expected_values.items()
            ):
                raise ArrowOverridePreconditionFailed(
                    "override commit guard no longer matches"
                )

    @staticmethod
    def _guard_key(
        table_config: TableConfig, row: Mapping[str, object]
    ) -> tuple[str, str, tuple[tuple[str, object], ...]]:
        keys = tuple(table_config.primary_keys)
        if not keys or any(name not in row for name in keys):
            raise ValueError("guard rows require every configured primary key")
        return (
            table_config.schema,
            table_config.name,
            tuple((name, row[name]) for name in keys),
        )

    def reset_generation(self, layer_id: UUID, generation: int) -> None:
        with self._lock:
            key = layer_id.bytes
            head = self._heads.get(key)
            if head is None:
                raise ArrowOverrideLayerNotFound("layer_not_found")
            if generation <= head.generation:
                raise ArrowOverrideContractError("new generation must increase")
            self._heads[key] = ArrowOverrideLayerHead(generation, 0)
            self._operations = {
                operation_key: operations
                for operation_key, operations in self._operations.items()
                if operation_key[0] != key
            }
            self._operations[(key, generation)] = empty_arrow_override_operations()

    def _table(self, layer_id: UUID, generation: int) -> pa.Table:
        return self._operations.get(
            (layer_id.bytes, generation), empty_arrow_override_operations()
        )

    def _filter(
        self,
        layer_id: UUID,
        generation: int,
        predicate: Callable[[pa.Table, int], bool],
    ) -> pa.Table:
        with self._lock:
            table = self._table(layer_id, generation)
            keep = [index for index in range(table.num_rows) if predicate(table, index)]
            return table.take(pa.array(keep, type=pa.int64()))


class InMemoryArrowOverrideOperationStore(RepositoryArrowOverrideOperationStore):
    """Reference implementation of the immutable Arrow operation-set CRDT."""

    def __init__(self) -> None:
        repository = InMemoryArrowOverrideRepository()
        super().__init__(repository)
        self.in_memory_repository = repository

    def create_layer(self, layer_id: UUID, *, generation: int = 1) -> None:
        self.repository.create_layer(layer_id, generation=generation)

    def reset_generation(self, layer_id: UUID, generation: int) -> None:
        self.repository.reset_generation(layer_id, generation)


def project_arrow_override_operations(
    operations: pa.Table,
    *,
    affected_scopes: set[tuple[str, str, str]],
    valid_at: datetime,
    revision: int,
) -> pa.Table:
    validate_arrow_override_operations(operations, authority="committed")
    _require_utc(valid_at, "valid_at")
    layer_ids = cast(list[bytes], operations["layer_id"].to_pylist())
    generations = cast(list[int], operations["generation"].to_pylist())
    feed_ids = cast(list[str], operations["feed_id"].to_pylist())
    entity_ids = cast(list[str], operations["entity_id"].to_pylist())
    field_paths = cast(list[str], operations["field_path"].to_pylist())
    operation_types = cast(list[str], operations["operation_type"].to_pylist())
    operation_ids = cast(list[bytes], operations["operation_id"].to_pylist())
    values = cast(list[dict[str, object]], operations["value"].to_pylist())
    units = cast(list[str | None], operations["unit"].to_pylist())
    supersedes = cast(list[list[bytes]], operations["supersedes_ids"].to_pylist())
    removes = cast(list[list[bytes]], operations["removes_ids"].to_pylist())
    valid_from = cast(list[datetime], operations["valid_from"].to_pylist())
    valid_to = cast(list[datetime | None], operations["valid_to"].to_pylist())
    source_drift = cast(list[bool], operations["source_drift"].to_pylist())
    indices_by_scope: dict[tuple[str, str, str], list[int]] = {}
    for index in range(operations.num_rows):
        scope = (feed_ids[index], entity_ids[index], field_paths[index])
        if scope in affected_scopes and valid_from[index] <= valid_at:
            indices_by_scope.setdefault(scope, []).append(index)

    rows: list[dict[str, object]] = []
    for scope in sorted(affected_scopes):
        indices = indices_by_scope.get(scope, [])
        removed = {
            reference
            for index in indices
            for reference in supersedes[index] + removes[index]
        }
        active = sorted(
            [
                index
                for index in indices
                if operation_types[index] == "set"
                and operation_ids[index] not in removed
                and (
                    valid_to[index] is None
                    or valid_at < cast(datetime, valid_to[index])
                )
            ],
            key=operation_ids.__getitem__,
        )
        distinct_values = {_value_bytes(values[index]) for index in active}
        state = (
            "none"
            if not active
            else "clean"
            if len(distinct_values) == 1
            else "conflict"
        )
        selected = active[0] if state == "clean" else None
        first = indices[0] if indices else None
        if first is None:
            continue
        rows.append(
            {
                "layer_id": layer_ids[first],
                "generation": generations[first],
                "layer_revision": revision,
                "feed_id": scope[0],
                "entity_id": scope[1],
                "field_path": scope[2],
                "frontier_state": state,
                "value": None if selected is None else values[selected],
                "unit": None if selected is None else units[selected],
                "winning_operation_ids": [operation_ids[i] for i in active],
                "source_drift": any(source_drift[i] for i in active),
            }
        )
    return pa.Table.from_pylist(rows, schema=arrow_override_projection_schema())


def _plan_pending(pending: pa.Table, existing: pa.Table) -> tuple[list[int], list[int]]:
    validate_arrow_override_operations(existing, authority="committed")
    existing_indices = {
        existing["operation_id"][index].as_py(): index
        for index in range(existing.num_rows)
    }
    pending_indices = {
        pending["operation_id"][index].as_py(): index
        for index in range(pending.num_rows)
    }
    scopes = {
        operation_id: _scope(existing, index)
        for operation_id, index in existing_indices.items()
    } | {
        operation_id: _scope(pending, index)
        for operation_id, index in pending_indices.items()
    }
    accepted: list[int] = []
    duplicates: list[int] = []
    graph: dict[bytes, set[bytes]] = {}
    for operation_id, index in pending_indices.items():
        prior = existing_indices.get(operation_id)
        if prior is not None:
            layer_id = cast(bytes, existing["layer_id"][prior].as_py())
            if _proposal_bytes(pending, index, layer_id) != _proposal_bytes(
                existing, prior, layer_id
            ):
                raise ArrowOverrideContractError(
                    "operation ID was reused with different typed content"
                )
            duplicates.append(index)
            continue
        references = set(
            pending["supersedes_ids"][index].as_py()
            + pending["removes_ids"][index].as_py()
        )
        for reference in references:
            target_scope = scopes.get(reference)
            if target_scope is None:
                raise ArrowOverrideContractError(
                    "operation reference must already exist or be in the same batch"
                )
            if target_scope != scopes[operation_id]:
                raise ArrowOverrideContractError(
                    "operation reference must target the same override field"
                )
        graph[operation_id] = references & pending_indices.keys()
        accepted.append(index)
    _reject_reference_cycles(graph)
    return accepted, duplicates


def _reject_reference_cycles(graph: dict[bytes, set[bytes]]) -> None:
    visiting: set[bytes] = set()
    visited: set[bytes] = set()

    def visit(operation_id: bytes) -> None:
        if operation_id in visiting:
            raise ArrowOverrideContractError("operation references cannot form cycles")
        if operation_id in visited:
            return
        visiting.add(operation_id)
        for referenced_id in graph.get(operation_id, set()):
            visit(referenced_id)
        visiting.remove(operation_id)
        visited.add(operation_id)

    for operation_id in graph:
        visit(operation_id)


def _acknowledgements(
    current: pa.Table,
    pending: pa.Table,
    *,
    accepted_indices: list[int],
    duplicate_indices: list[int],
) -> pa.Table:
    committed = {
        current["operation_id"][index].as_py(): (
            current["layer_revision"][index].as_py(),
            current["payload_hash"][index].as_py(),
        )
        for index in range(current.num_rows)
    }
    accepted = set(accepted_indices)
    duplicates = set(duplicate_indices)
    rows = []
    for index in sorted(accepted | duplicates):
        operation_id = pending["operation_id"][index].as_py()
        revision, payload_hash = committed[operation_id]
        rows.append(
            {
                "operation_id": operation_id,
                "status": "accepted" if index in accepted else "duplicate",
                "layer_revision": revision,
                "payload_hash": payload_hash,
            }
        )
    return pa.Table.from_pylist(rows, schema=arrow_override_ack_schema())


def _validate_value(value: dict[str, object]) -> None:
    kind = value.get("kind")
    if str(kind) not in _VALUE_FIELDS:
        raise ArrowOverrideContractError("unsupported typed override value kind")
    field = _VALUE_FIELDS[str(kind)]
    populated = [
        name
        for name in _VALUE_FIELDS.values()
        if name is not None and value.get(name) is not None
    ]
    if (field is None and populated) or (field is not None and populated != [field]):
        raise ArrowOverrideContractError(
            "typed override value must populate exactly its declared slot"
        )
    if kind == "float" and not math.isfinite(cast(float, value["float_value"])):
        raise ArrowOverrideContractError("float override values must be finite")
    if kind == "string_list" and any(
        not isinstance(item, str)
        for item in cast(list[object], value["string_list_value"])
    ):
        raise ArrowOverrideContractError(
            "string-list override values cannot contain null or non-string items"
        )
    if kind == "time" and cast(datetime_time, value["time_value"]).tzinfo is not None:
        raise ArrowOverrideContractError(
            "time override values cannot contain a timezone; use timestamp instead"
        )
    if kind == "extension" and not value.get("extension_schema_id"):
        raise ArrowOverrideContractError("extension values require extension_schema_id")
    if kind != "extension" and value.get("extension_schema_id") is not None:
        raise ArrowOverrideContractError(
            "only extension values may declare extension_schema_id"
        )


def _scope(table: pa.Table, index: int) -> tuple[str, str, str]:
    return (
        table["feed_id"][index].as_py(),
        table["entity_id"][index].as_py(),
        table["field_path"][index].as_py(),
    )


def _operation_id(table: pa.Table, index: int) -> bytes:
    return cast(bytes, table["operation_id"][index].as_py())


def _proposal_bytes(table: pa.Table, index: int, layer_id: bytes) -> bytes:
    return b"".join(
        [
            _encode_int(table["format_version"][index].as_py()),
            _encode_bytes(table["operation_id"][index].as_py()),
            _encode_bytes(table["change_set_id"][index].as_py()),
            _encode_bytes(layer_id),
            _encode_text(table["feed_id"][index].as_py()),
            _encode_text(table["entity_id"][index].as_py()),
            _encode_text(table["field_path"][index].as_py()),
            _encode_text(table["operation_type"][index].as_py()),
            _encode_value(table["value"][index].as_py()),
            _encode_optional_text(table["unit"][index].as_py()),
            _encode_uuid_list(table["supersedes_ids"][index].as_py()),
            _encode_uuid_list(table["removes_ids"][index].as_py()),
            _encode_int(_timestamp_us(table["valid_from"][index])),
            _encode_optional_int(_optional_timestamp_us(table["valid_to"][index])),
            _encode_value(table["observed_value"][index].as_py()),
            b"\x01" if table["source_drift"][index].as_py() else b"\x00",
            _encode_optional_text(table["comment"][index].as_py()),
        ]
    )


def _committed_payload_hash(table: pa.Table, index: int) -> bytes:
    layer_id = table["layer_id"][index].as_py()
    payload = b"".join(
        [
            _proposal_bytes(table, index, layer_id),
            _encode_int(table["generation"][index].as_py()),
            _encode_int(table["layer_revision"][index].as_py()),
            _encode_text(table["actor_subject"][index].as_py()),
            _encode_optional_text(table["actor_display_name"][index].as_py()),
            _encode_int(_timestamp_us(table["recorded_at"][index])),
        ]
    )
    return sha256(payload).digest()


def _value_bytes(value: dict[str, object] | None) -> bytes:
    return _encode_value(value)


def _encode_value(value: dict[str, object] | None) -> bytes:
    if value is None:
        return b"\x00"
    kind = str(value["kind"])
    field = _VALUE_FIELDS[kind]
    item = None if field is None else value[field]
    body: bytes
    if kind == "null":
        body = b""
    elif kind == "string":
        body = _encode_text(str(item))
    elif kind == "boolean":
        body = b"\x01" if item else b"\x00"
    elif kind == "integer":
        body = struct.pack(">q", cast(int, item))
    elif kind == "float":
        body = struct.pack(">d", cast(float, item))
    elif kind == "decimal":
        unscaled = int(Decimal(str(item)).scaleb(12))
        body = unscaled.to_bytes(16, "big", signed=True)
    elif kind == "timestamp":
        assert isinstance(item, datetime)
        body = _encode_int(_datetime_us(item))
    elif kind == "date":
        assert isinstance(item, date)
        body = _encode_int((item - date(1970, 1, 1)).days)
    elif kind == "time":
        assert isinstance(item, datetime_time)
        body = _encode_int(
            ((item.hour * 60 + item.minute) * 60 + item.second) * 1_000_000
            + item.microsecond
        )
    elif kind == "duration":
        assert isinstance(item, timedelta)
        body = _encode_int(
            item.days * 86_400_000_000 + item.seconds * 1_000_000 + item.microseconds
        )
    elif kind == "binary":
        body = _encode_bytes(cast(bytes, item))
    elif kind == "string_list":
        items = cast(list[str], item)
        body = b"".join(
            [_encode_int(len(items)), *(_encode_text(entry) for entry in items)]
        )
    else:
        body = _encode_text(str(value["extension_schema_id"])) + _encode_bytes(
            cast(bytes, item)
        )
    return b"\x01" + _encode_text(kind) + body


def _timestamp_us(scalar: pa.Scalar) -> int:
    return cast(int, scalar.cast(pa.int64()).as_py())


def _optional_timestamp_us(scalar: pa.Scalar) -> int | None:
    return None if not scalar.is_valid else _timestamp_us(scalar)


def _encode_uuid_list(values: list[bytes]) -> bytes:
    return b"".join(
        [_encode_int(len(values)), *(_encode_bytes(value) for value in values)]
    )


def _encode_text(value: str) -> bytes:
    return _encode_bytes(value.encode())


def _encode_optional_text(value: str | None) -> bytes:
    return b"\x00" if value is None else b"\x01" + _encode_text(value)


def _encode_bytes(value: bytes) -> bytes:
    return struct.pack(">I", len(value)) + value


def _encode_int(value: int) -> bytes:
    return struct.pack(">q", value)


def _encode_optional_int(value: int | None) -> bytes:
    return b"\x00" if value is None else b"\x01" + _encode_int(value)


def _require_utc(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ArrowOverrideContractError(f"{name} must be timezone-aware")
    if value.utcoffset() != timedelta(0):
        raise ArrowOverrideContractError(f"{name} must be normalized to UTC")


def _datetime_us(value: datetime) -> int:
    _require_utc(value, "timestamp value")
    delta = value - datetime(1970, 1, 1, tzinfo=UTC)
    return delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds
