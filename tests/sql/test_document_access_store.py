from datetime import UTC, datetime, timezone

import pytest

from polars_hist_db.core import TableConfigOps
from polars_hist_db.overrides import (
    AccessGrantInput,
    DocumentAccessStoreConfig,
    DocumentArchived,
    MariaDbDocumentAccessStore,
    build_document_access_table_configs,
)

from ..utils.dsv_helper import mariadb_engine_test

pytestmark = pytest.mark.integration


def test_mariadb_access_store_persists_lifecycle_and_idempotency():
    config = DocumentAccessStoreConfig(
        schema="test",
        documents_table="access_documents_store_test",
        grants_table="access_grants_store_test",
        commands_table="access_commands_store_test",
    )
    tables = build_document_access_table_configs(config)
    now = datetime(2026, 7, 12, 11, tzinfo=UTC)
    engine = mariadb_engine_test()
    with engine.begin() as connection:
        table_ops = TableConfigOps(connection)
        for table in reversed(tables):
            table_ops.drop(table)
        try:
            for table in tables:
                table_ops.create(table)
            store = MariaDbDocumentAccessStore(connection, config)
            created = store.create(
                "document-1",
                "Shared corrections",
                None,
                "user-1",
                now,
                initial_grants=(AccessGrantInput("grant-1", "operators", "editor"),),
                idempotency_key="command-1",
                owning_group="operators",
            )
            duplicate = store.create(
                "document-1",
                "Shared corrections",
                None,
                "user-1",
                now,
                initial_grants=(AccessGrantInput("grant-1", "operators", "editor"),),
                idempotency_key="command-1",
                owning_group="operators",
            )
            existing = store.create(
                "document-2",
                " shared corrections ",
                None,
                "user-1",
                now,
                initial_grants=(AccessGrantInput("grant-2", "operators", "editor"),),
                idempotency_key="command-ensure",
                owning_group="operators",
                allow_existing=True,
            )
            other_group = store.create(
                "document-3",
                "Shared corrections",
                None,
                "user-2",
                now,
                initial_grants=(AccessGrantInput("grant-3", "reviewers", "editor"),),
                idempotency_key="command-other-group",
                owning_group="reviewers",
                allow_existing=True,
            )
            revoked = store.revoke(
                "document-1",
                "operators",
                "user-1",
                now,
                1,
                idempotency_key="command-2",
            )
            archived = store.archive(
                "document-1", "user-1", now, 2, idempotency_key="command-3"
            )

            assert created.document.revision == 1
            assert duplicate.duplicate is True
            assert duplicate.document.created_at == now
            assert existing.document.document_id == "document-1"
            assert existing.duplicate is True
            assert other_group.document.document_id == "document-3"
            assert revoked.grants[0].active is False
            assert archived.document.status == "archived"
            with pytest.raises(DocumentArchived):
                store.grant(
                    "document-1",
                    AccessGrantInput("grant-2", "operators", "editor"),
                    "user-1",
                    now,
                    3,
                    idempotency_key="command-4",
                )
        finally:
            for table in reversed(tables):
                table_ops.drop(table)
