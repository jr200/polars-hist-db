from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import polars as pl
import pyarrow as pa
from sqlalchemy import text

from polars_hist_db.backends.xtdb_schema import _xtdb_table_exists
from polars_hist_db.backends.xtdb_transport import (
    _execute_xtdb_transaction,
    _xtdb_column_identifier,
)

from .arrow import (
    ArrowOverrideContractError,
    ArrowOverrideLayerHead,
    ArrowOverrideLayerNotFound,
    ArrowOverridePreconditionFailed,
    arrow_override_operations_from_storage,
    arrow_override_storage_frames,
    empty_arrow_override_operations,
    validate_arrow_override_operations,
)
from .config import build_arrow_override_table_configs
from .crdt import RowGuard
from .types import ArrowOverrideStoreConfig
from .xtdb import _insert_statement, _literal, _table_name, _where


class XtdbArrowOverrideRepository:
    """XTDB repository using assertions for atomic revision advancement."""

    def __init__(self, connection: Any, config: ArrowOverrideStoreConfig) -> None:
        self.connection = connection
        self.backend = "xtdb"
        (
            self.heads,
            self.operations,
            self.references,
            self.string_lists,
        ) = build_arrow_override_table_configs(config)

    def create_layer(self, layer_id: UUID, *, generation: int = 1) -> None:
        if generation < 1:
            raise ArrowOverrideContractError("generation must be positive")
        layer = _literal(layer_id.bytes, "BINARY(16)")
        statements = []
        if self._exists(self.heads):
            statements.append(
                f"ASSERT NOT EXISTS (SELECT 1 FROM {_table_name(self.heads)} "
                f"WHERE layer_id = {layer}), 'layer already exists'"
            )
        statements.append(
            _insert_statement(
                self.heads,
                {
                    "layer_id": layer_id.bytes,
                    "generation": generation,
                    "revision": 0,
                    "updated_at": datetime.now(UTC),
                },
            )
        )
        _execute_xtdb_transaction(
            self.connection,
            statements,
        )

    def head(self, layer_id: UUID) -> ArrowOverrideLayerHead | None:
        if not self._exists(self.heads):
            return None
        rows = self._read(
            f"SELECT generation, revision FROM {_table_name(self.heads)} "
            f"WHERE layer_id = {_literal(layer_id.bytes, 'BINARY(16)')}"
        )
        if rows.is_empty():
            return None
        return ArrowOverrideLayerHead(
            int(rows["generation"][0]), int(rows["revision"][0])
        )

    def operations_by_ids(
        self, layer_id: UUID, generation: int, operation_ids: set[bytes]
    ) -> pa.Table:
        if not operation_ids or not self._exists(self.operations):
            return empty_arrow_override_operations()
        ids = ", ".join(
            _literal(operation_id, "BINARY(16)") for operation_id in operation_ids
        )
        return self._read_operations(
            f"layer_id = {_literal(layer_id.bytes, 'BINARY(16)')} "
            f"AND generation = {generation}::BIGINT AND operation_id IN ({ids})"
        )

    def changed_scopes(
        self,
        layer_id: UUID,
        generation: int,
        *,
        after_revision: int,
        through_revision: int,
    ) -> set[tuple[str, str, str]]:
        if not self._exists(self.operations):
            return set()
        rows = self._read(
            "SELECT DISTINCT feed_id, entity_id, field_path FROM "
            f"{_table_name(self.operations)} WHERE "
            f"layer_id = {_literal(layer_id.bytes, 'BINARY(16)')} "
            f"AND generation = {generation}::BIGINT "
            f"AND layer_revision > {after_revision}::BIGINT "
            f"AND layer_revision <= {through_revision}::BIGINT"
        )
        return {
            (str(feed_id), str(entity_id), str(field_path))
            for feed_id, entity_id, field_path in rows.iter_rows()
        }

    def operations_for_scopes(
        self,
        layer_id: UUID,
        generation: int,
        scopes: set[tuple[str, str, str]],
        *,
        through_revision: int,
    ) -> pa.Table:
        if not scopes:
            return empty_arrow_override_operations()
        scope_sql = " OR ".join(
            "("
            + " AND ".join(
                (
                    f"feed_id = {_literal(feed_id, 'TEXT')}",
                    f"entity_id = {_literal(entity_id, 'TEXT')}",
                    f"field_path = {_literal(field_path, 'TEXT')}",
                )
            )
            + ")"
            for feed_id, entity_id, field_path in scopes
        )
        return self._read_operations(
            f"layer_id = {_literal(layer_id.bytes, 'BINARY(16)')} "
            f"AND generation = {generation}::BIGINT "
            f"AND layer_revision <= {through_revision}::BIGINT "
            f"AND ({scope_sql})"
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
        validate_arrow_override_operations(committed, authority="committed")
        operation_rows, reference_rows, string_list_rows = (
            arrow_override_storage_frames(committed)
        )
        layer = _literal(layer_id.bytes, "BINARY(16)")
        expected = (
            f"layer_id = {layer} AND generation = {generation}::BIGINT "
            f"AND revision = {expected_revision}::BIGINT"
        )
        operation_ids = ", ".join(
            _literal(value, "BINARY(16)")
            for value in operation_rows["operation_id"].to_list()
        )
        statements = [
            *(self._guard_assertion(guard) for guard in guards),
            (
                f"ASSERT EXISTS (SELECT 1 FROM {_table_name(self.heads)} WHERE "
                f"{expected}), 'layer revision changed'"
            ),
        ]
        if self._exists(self.operations):
            statements.append(
                f"ASSERT NOT EXISTS (SELECT 1 FROM {_table_name(self.operations)} "
                f"WHERE operation_id IN ({operation_ids})), "
                "'immutable operation conflict'"
            )
        statements.extend(
            [
                (
                    f"UPDATE {_table_name(self.heads)} SET "
                    f"revision = {expected_revision + 1}::BIGINT, "
                    f"updated_at = {_literal(updated_at, 'TIMESTAMP WITH TIME ZONE')} "
                    f"WHERE {expected}"
                ),
                *self._insert_frames(
                    (self.operations, operation_rows),
                    (self.references, reference_rows),
                    (self.string_lists, string_list_rows),
                ),
            ]
        )
        try:
            _execute_xtdb_transaction(self.connection, statements)
        except Exception as exc:
            if any(not self._guard_matches(guard) for guard in guards):
                raise ArrowOverridePreconditionFailed(
                    "override commit guard no longer matches"
                ) from exc
            if self.head(layer_id) != ArrowOverrideLayerHead(
                generation, expected_revision
            ):
                return False
            raise
        return True

    def _guard_assertion(self, guard: RowGuard) -> str:
        values = {**guard.key_values, **guard.expected_values}
        return (
            f"ASSERT EXISTS (SELECT 1 FROM {_table_name(guard.table_config)} "
            f"WHERE {_where(guard.table_config, values)}), "
            "'override commit guard failed'"
        )

    def _guard_matches(self, guard: RowGuard) -> bool:
        values = {**guard.key_values, **guard.expected_values}
        return not self._read(
            f"SELECT 1 FROM {_table_name(guard.table_config)} "
            f"WHERE {_where(guard.table_config, values)}"
        ).is_empty()

    def reset_generation(self, layer_id: UUID, generation: int) -> None:
        head = self.head(layer_id)
        if head is None:
            raise ArrowOverrideLayerNotFound("layer_not_found")
        if generation <= head.generation:
            raise ArrowOverrideContractError("new generation must increase")
        layer = _literal(layer_id.bytes, "BINARY(16)")
        operation_ids = (
            f"SELECT operation_id FROM {_table_name(self.operations)} "
            f"WHERE layer_id = {layer}"
        )
        expected = (
            f"layer_id = {layer} AND generation = {head.generation}::BIGINT "
            f"AND revision = {head.revision}::BIGINT"
        )
        statements = [
            (
                f"ASSERT EXISTS (SELECT 1 FROM {_table_name(self.heads)} WHERE "
                f"{expected}), 'layer revision changed'"
            )
        ]
        if self._exists(self.operations):
            if self._exists(self.references):
                statements.append(
                    f"ERASE FROM {_table_name(self.references)} WHERE operation_id "
                    f"IN ({operation_ids})"
                )
            if self._exists(self.string_lists):
                statements.append(
                    f"ERASE FROM {_table_name(self.string_lists)} WHERE operation_id "
                    f"IN ({operation_ids})"
                )
            statements.append(
                f"ERASE FROM {_table_name(self.operations)} WHERE layer_id = {layer}"
            )
        statements.append(
            f"UPDATE {_table_name(self.heads)} SET "
            f"generation = {generation}::BIGINT, revision = 0::BIGINT, "
            f"updated_at = {_literal(datetime.now(UTC), 'TIMESTAMP WITH TIME ZONE')} "
            f"WHERE {expected}"
        )
        _execute_xtdb_transaction(self.connection, statements)

    def _read_operations(self, predicate: str) -> pa.Table:
        if not self._exists(self.operations):
            return empty_arrow_override_operations()
        operation_rows = self._read(
            f"SELECT {self._columns(self.operations)} "
            f"FROM {_table_name(self.operations)} WHERE {predicate}"
        )
        if operation_rows.is_empty():
            return empty_arrow_override_operations()
        selected = (
            f"SELECT operation_id FROM {_table_name(self.operations)} WHERE {predicate}"
        )
        _, empty_references, empty_string_lists = arrow_override_storage_frames(
            empty_arrow_override_operations()
        )
        reference_rows = (
            self._read(
                f"SELECT {self._columns(self.references)} "
                f"FROM {_table_name(self.references)} WHERE operation_id IN ({selected})"
            )
            if self._exists(self.references)
            else empty_references
        )
        string_list_rows = (
            self._read(
                f"SELECT {self._columns(self.string_lists)} "
                f"FROM {_table_name(self.string_lists)} WHERE operation_id IN ({selected})"
            )
            if self._exists(self.string_lists)
            else empty_string_lists
        )
        return arrow_override_operations_from_storage(
            operation_rows,
            empty_references if reference_rows.is_empty() else reference_rows,
            empty_string_lists if string_list_rows.is_empty() else string_list_rows,
        )

    def _read(self, query: str) -> pl.DataFrame:
        return pl.read_database(text(query), self.connection)

    def _exists(self, config: Any) -> bool:
        return _xtdb_table_exists(self.connection, config.schema, config.name)

    @staticmethod
    def _columns(config: Any) -> str:
        return ", ".join(
            _xtdb_column_identifier(column.name) for column in config.columns
        )

    @staticmethod
    def _insert_frames(*items: tuple[Any, pl.DataFrame]) -> list[str]:
        return [
            _insert_statement(config, row)
            for config, frame in items
            for row in frame.iter_rows(named=True)
        ]
