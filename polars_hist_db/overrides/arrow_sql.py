from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import polars as pl
import pyarrow as pa
from sqlalchemy import Connection, MetaData, Table, and_, delete, distinct, or_, select
from sqlalchemy.exc import IntegrityError

from polars_hist_db.core import TableConfigOps

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


class MariaDbArrowOverrideRepository:
    """Atomic typed repository for the Arrow override CRDT tables."""

    def __init__(
        self, connection: Connection, config: ArrowOverrideStoreConfig
    ) -> None:
        self.connection = connection
        self.backend = "mariadb"
        self.config = config
        self._table_configs = build_arrow_override_table_configs(config)
        metadata = MetaData(schema=config.schema)
        self._table_tuple = tuple(
            Table(
                table_config.name,
                metadata,
                *table_config.build_sqlalchemy_columns(is_delta_table=False),
            )
            for table_config in self._table_configs
        )

    def create_tables(self) -> None:
        for config in self._table_configs:
            TableConfigOps(self.connection).create(config)

    def create_layer(self, layer_id: UUID, *, generation: int = 1) -> None:
        if generation < 1:
            raise ArrowOverrideContractError("generation must be positive")
        heads, _, _, _ = self._tables()
        try:
            with self.connection.begin_nested():
                self.connection.execute(
                    heads.insert().values(
                        layer_id=layer_id.bytes,
                        generation=generation,
                        revision=0,
                        updated_at=datetime.now(UTC),
                    )
                )
        except IntegrityError as exc:
            raise ArrowOverrideContractError("layer already exists") from exc

    def head(self, layer_id: UUID) -> ArrowOverrideLayerHead | None:
        heads, _, _, _ = self._tables()
        row = (
            self.connection.execute(
                select(heads.c.generation, heads.c.revision).where(
                    heads.c.layer_id == layer_id.bytes
                )
            )
            .mappings()
            .first()
        )
        return (
            None
            if row is None
            else ArrowOverrideLayerHead(int(row["generation"]), int(row["revision"]))
        )

    def operations_by_ids(
        self, layer_id: UUID, generation: int, operation_ids: set[bytes]
    ) -> pa.Table:
        if not operation_ids:
            return empty_arrow_override_operations()
        _, operations, _, _ = self._tables()
        return self._read_operations(
            and_(
                operations.c.layer_id == layer_id.bytes,
                operations.c.generation == generation,
                operations.c.operation_id.in_(operation_ids),
            )
        )

    def changed_scopes(
        self,
        layer_id: UUID,
        generation: int,
        *,
        after_revision: int,
        through_revision: int,
    ) -> set[tuple[str, str, str]]:
        _, operations, _, _ = self._tables()
        rows = self.connection.execute(
            select(
                distinct(operations.c.feed_id),
                operations.c.entity_id,
                operations.c.field_path,
            ).where(
                operations.c.layer_id == layer_id.bytes,
                operations.c.generation == generation,
                operations.c.layer_revision > after_revision,
                operations.c.layer_revision <= through_revision,
            )
        )
        return {(str(row[0]), str(row[1]), str(row[2])) for row in rows}

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
        _, operations, _, _ = self._tables()
        scope_predicate = or_(
            *(
                and_(
                    operations.c.feed_id == feed_id,
                    operations.c.entity_id == entity_id,
                    operations.c.field_path == field_path,
                )
                for feed_id, entity_id, field_path in scopes
            )
        )
        return self._read_operations(
            and_(
                operations.c.layer_id == layer_id.bytes,
                operations.c.generation == generation,
                operations.c.layer_revision <= through_revision,
                scope_predicate,
            )
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
        operations_frame, references_frame, string_lists_frame = (
            arrow_override_storage_frames(committed)
        )
        heads, operations, references, string_lists = self._tables()
        try:
            with self.connection.begin_nested():
                self._check_guards(guards)
                result = self.connection.execute(
                    heads.update()
                    .where(
                        heads.c.layer_id == layer_id.bytes,
                        heads.c.generation == generation,
                        heads.c.revision == expected_revision,
                    )
                    .values(revision=expected_revision + 1, updated_at=updated_at)
                )
                if result.rowcount != 1:
                    return False
                self._insert_frame(operations, operations_frame)
                self._insert_frame(references, references_frame)
                self._insert_frame(string_lists, string_lists_frame)
        except IntegrityError as exc:
            raise ArrowOverrideContractError(
                "immutable operation storage conflict"
            ) from exc
        return True

    def _check_guards(self, guards: Sequence[RowGuard]) -> None:
        for guard in guards:
            table = Table(
                guard.table_config.name,
                MetaData(schema=guard.table_config.schema),
                *guard.table_config.build_sqlalchemy_columns(is_delta_table=False),
            )
            predicates = [
                table.c[name] == value
                for name, value in {**guard.key_values, **guard.expected_values}.items()
            ]
            if (
                self.connection.execute(
                    select(table).where(and_(*predicates)).with_for_update()
                ).first()
                is None
            ):
                raise ArrowOverridePreconditionFailed(
                    "override commit guard no longer matches"
                )

    def reset_generation(self, layer_id: UUID, generation: int) -> None:
        heads, operations, references, string_lists = self._tables()
        with self.connection.begin_nested():
            head = self.head(layer_id)
            if head is None:
                raise ArrowOverrideLayerNotFound("layer_not_found")
            if generation <= head.generation:
                raise ArrowOverrideContractError("new generation must increase")
            operation_ids = select(operations.c.operation_id).where(
                operations.c.layer_id == layer_id.bytes
            )
            self.connection.execute(
                delete(references).where(references.c.operation_id.in_(operation_ids))
            )
            self.connection.execute(
                delete(string_lists).where(
                    string_lists.c.operation_id.in_(operation_ids)
                )
            )
            self.connection.execute(
                delete(operations).where(operations.c.layer_id == layer_id.bytes)
            )
            self.connection.execute(
                heads.update()
                .where(heads.c.layer_id == layer_id.bytes)
                .values(
                    generation=generation,
                    revision=0,
                    updated_at=datetime.now(UTC),
                )
            )

    def _read_operations(self, predicate: Any) -> pa.Table:
        _, operations, references, string_lists = self._tables()
        operation_query = select(operations).where(predicate)
        operation_frame = self._read(operation_query)
        if operation_frame.is_empty():
            return empty_arrow_override_operations()
        selected = select(operations.c.operation_id).where(predicate).subquery()
        references_frame = self._read(
            select(references).join(
                selected, references.c.operation_id == selected.c.operation_id
            )
        )
        string_lists_frame = self._read(
            select(string_lists).join(
                selected, string_lists.c.operation_id == selected.c.operation_id
            )
        )
        _, empty_references, empty_string_lists = arrow_override_storage_frames(
            empty_arrow_override_operations()
        )
        if references_frame.is_empty():
            references_frame = empty_references
        if string_lists_frame.is_empty():
            string_lists_frame = empty_string_lists
        return arrow_override_operations_from_storage(
            operation_frame, references_frame, string_lists_frame
        )

    def _tables(self) -> tuple[Table, Table, Table, Table]:
        return self._table_tuple  # type: ignore[return-value]

    def _read(self, query: Any) -> pl.DataFrame:
        return pl.read_database(query, self.connection)

    def _insert_frame(self, table: Table, frame: pl.DataFrame) -> None:
        if frame.is_empty():
            return
        columns = [column.name for column in table.columns if column.name in frame]
        self.connection.execute(
            table.insert(), list(frame.select(columns).iter_rows(named=True))
        )
