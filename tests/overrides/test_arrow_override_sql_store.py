from datetime import UTC, datetime, timezone
from uuid import uuid4

import pyarrow as pa
import pytest
from sqlalchemy import MetaData, Table, create_engine, text

from polars_hist_db.config import TableColumnConfig, TableConfig
from polars_hist_db.overrides import (
    ArrowOverridePreconditionFailed,
    ArrowOverrideStoreConfig,
    MariaDbArrowOverrideRepository,
    RepositoryArrowOverrideOperationStore,
    RowGuard,
    arrow_override_operation_schema,
    build_arrow_override_table_configs,
)


def _value(value: str) -> dict[str, object]:
    return {
        field.name: value
        if field.name == "string_value"
        else "string"
        if field.name == "kind"
        else None
        for field in arrow_override_operation_schema().field("value").type
    }


def _proposal(value: str) -> pa.Table:
    return pa.Table.from_pylist(
        [
            {
                "format_version": 1,
                "operation_id": uuid4().bytes,
                "change_set_id": uuid4().bytes,
                "feed_id": "records",
                "entity_id": "record-1",
                "field_path": "status",
                "operation_type": "set",
                "value": _value(value),
                "supersedes_ids": [],
                "removes_ids": [],
                "valid_from": datetime(2026, 7, 19, tzinfo=UTC),
                "source_drift": False,
            }
        ],
        schema=arrow_override_operation_schema(),
    )


def test_sql_repository_obeys_arrow_sync_contract() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    layer_id = uuid4()
    now = datetime(2026, 7, 19, 2, tzinfo=UTC)
    with engine.begin() as connection:
        connection.execute(text("ATTACH DATABASE ':memory:' AS overrides"))
        config = ArrowOverrideStoreConfig()
        metadata = MetaData(schema=config.schema)
        for table_config in build_arrow_override_table_configs(config):
            Table(
                table_config.name,
                metadata,
                *table_config.build_sqlalchemy_columns(is_delta_table=False),
            )
        access_config = TableConfig(
            name="access_guard",
            schema=config.schema,
            primary_keys=("resource_id",),
            columns=[
                TableColumnConfig(
                    "access_guard", "resource_id", "VARCHAR(128)", nullable=False
                ),
                TableColumnConfig(
                    "access_guard", "status", "VARCHAR(16)", nullable=False
                ),
                TableColumnConfig("access_guard", "revision", "BIGINT", nullable=False),
            ],
        )
        access = Table(
            access_config.name,
            metadata,
            *access_config.build_sqlalchemy_columns(is_delta_table=False),
        )
        metadata.create_all(connection)
        connection.execute(
            access.insert().values(
                resource_id=str(layer_id),
                status="active",
                revision=2,
            )
        )
        repository = MariaDbArrowOverrideRepository(connection, config)
        repository.create_layer(layer_id)
        store = RepositoryArrowOverrideOperationStore(repository)
        proposal = _proposal("ready")

        first = store.sync(
            layer_id=layer_id,
            generation=1,
            known_revision=0,
            pending=proposal,
            actor_subject="subject-1",
            actor_display_name="Verified Name",
            recorded_at=now,
        )
        duplicate = store.sync(
            layer_id=layer_id,
            generation=1,
            known_revision=1,
            pending=proposal,
            actor_subject="subject-1",
            actor_display_name="Verified Name",
            recorded_at=now,
        )

        assert first.revision == duplicate.revision == 1
        assert first.projection_delta["frontier_state"].to_pylist() == ["clean"]
        assert duplicate.acknowledgements["status"].to_pylist() == ["duplicate"]
        assert duplicate.projection_delta.num_rows == 0

        with pytest.raises(ArrowOverridePreconditionFailed, match="guard"):
            store.sync(
                layer_id=layer_id,
                generation=1,
                known_revision=1,
                pending=_proposal("blocked"),
                actor_subject="subject-1",
                actor_display_name="Verified Name",
                recorded_at=now,
                guards=(
                    RowGuard(
                        access_config,
                        {"resource_id": str(layer_id)},
                        {"status": "active", "revision": 1},
                    ),
                ),
            )
