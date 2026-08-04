from datetime import UTC, datetime, timezone
from typing import Any
from uuid import uuid4

import pytest
from pycrdt import Doc, Map
from sqlalchemy import select

from polars_hist_db.core import TableConfigOps
from polars_hist_db.overrides import (
    CrdtDocumentStoreConfig,
    MariaDbCrdtDocumentStore,
    OverrideLedgerConfig,
    build_crdt_document_table_config,
    build_crdt_update_table_config,
    build_override_table_config,
    prepare_crdt_update,
)

from ..utils.dsv_helper import mariadb_engine_test

pytestmark = pytest.mark.integration


def _operation_update(operation_id: str) -> bytes:
    document: Any = Doc()
    document["operations"] = Map(
        {
            operation_id: {
                "format_version": 1,
                "operation_id": operation_id,
                "change_set_id": str(uuid4()),
                "layer_id": "shared",
                "feed_id": "records",
                "entity_id": "record-1",
                "field_path": "status",
                "operation_type": "set",
                "value": {"value_type": "enum", "value_json": {"value": "open"}},
                "supersedes_operation_ids": [],
                "removes_operation_ids": [],
                "valid_from": "2026-07-12T10:00:00+00:00",
                "valid_to": None,
            }
        }
    )
    return document.get_update()


def test_mariadb_crdt_store_persists_prepared_commit_and_projection():
    document_config = CrdtDocumentStoreConfig(
        schema="test",
        documents_table="crdt_documents_store_test",
        updates_table="crdt_updates_store_test",
    )
    projection_config = OverrideLedgerConfig(
        schema="test", table="crdt_operations_store_test"
    )
    document_table = build_crdt_document_table_config(document_config)
    update_table = build_crdt_update_table_config(document_config)
    projection_table = build_override_table_config(projection_config)
    source_update = _operation_update(str(uuid4()))
    prepared = prepare_crdt_update(
        "document-1",
        None,
        source_update,
        actor_id="user-1",
        recorded_at=datetime(2026, 7, 12, 11, tzinfo=UTC),
    )

    engine = mariadb_engine_test()
    with engine.begin() as connection:
        table_ops = TableConfigOps(connection)
        for config in (projection_table, update_table, document_table):
            table_ops.drop(config)
        try:
            for config in (document_table, update_table, projection_table):
                table_ops.create(config)
            store = MariaDbCrdtDocumentStore(
                connection, document_config, projection_config
            )

            accepted = store.commit(prepared)
            duplicate = store.commit(prepared)
            caught_up = Doc()
            caught_up.apply_update(store.diff("document-1", b"\x00"))
            conflicting = prepare_crdt_update(
                "document-2",
                None,
                source_update,
                actor_id="user-1",
                recorded_at=datetime(2026, 7, 12, 11, tzinfo=UTC),
            )
            operation = (
                connection.execute(
                    select(store.projection).where(
                        store.projection.c.operation_id
                        == prepared.operations[0].operation_id
                    )
                )
                .mappings()
                .one()
            )

            assert accepted.accepted is True
            assert accepted.revision == 1
            assert (
                caught_up.get_state() == store.load_document("document-1").state_vector
            )
            assert duplicate.accepted is False
            assert duplicate.duplicate is True
            assert duplicate.revision == 1
            assert operation["owner_user_id"] is None
            assert operation["actor_id"] == "user-1"
            assert operation["crdt_document_id"] == "document-1"
            assert operation["crdt_document_revision"] == 1

            with pytest.raises(ValueError, match="immutable row"):
                store.commit(conflicting)
            assert store.load_document("document-2") is None
            assert (
                connection.execute(
                    select(store.updates.c.document_id).where(
                        store.updates.c.document_id == "document-2"
                    )
                ).first()
                is None
            )
        finally:
            for config in (projection_table, update_table, document_table):
                table_ops.drop(config)
