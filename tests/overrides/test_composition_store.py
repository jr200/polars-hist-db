from datetime import UTC, datetime, timezone
from uuid import uuid4

import pytest

from polars_hist_db.overrides import (
    CompositionRevision,
    InMemoryLayerCompositionStore,
    LayerCompositionStoreConfig,
    OverridePurgeStoreConfig,
    build_layer_composition_table_config,
    build_override_purge_table_config,
)
from polars_hist_db.overrides.xtdb import XtdbLayerCompositionStore


def test_configurable_composition_and_purge_tables() -> None:
    composition = build_layer_composition_table_config(
        LayerCompositionStoreConfig("custom", "compositions")
    )
    purge = build_override_purge_table_config(
        OverridePurgeStoreConfig("custom", "purges")
    )

    assert (composition.schema, composition.name) == ("custom", "compositions")
    assert {column.name for column in composition.columns} >= {
        "child_layer_ids_json",
        "valid_from",
        "valid_to",
        "recorded_at",
        "supersedes_revision_id",
    }
    assert (purge.schema, purge.name) == ("custom", "purges")
    assert "value_json" not in {column.name for column in purge.columns}
    assert "comment" not in {column.name for column in purge.columns}


def test_in_memory_composition_store_preserves_revision_order() -> None:
    store = InMemoryLayerCompositionStore()
    first = CompositionRevision(
        str(uuid4()),
        "root",
        ("left", "right"),
        datetime(2026, 7, 13, tzinfo=UTC),
        None,
        datetime(2026, 7, 12, tzinfo=UTC),
    )
    store.append(first, "editor")
    second = CompositionRevision(
        str(uuid4()),
        "root",
        ("left",),
        datetime(2026, 7, 14, tzinfo=UTC),
        None,
        datetime(2026, 7, 13, tzinfo=UTC),
    )
    store.append(second, "editor")

    first_page = store.revisions("root", limit=1)
    second_page = store.revisions("root", cursor=first_page.next_cursor, limit=1)

    assert first_page.items == (first,)
    assert second_page.items == (second,)
    assert first_page.next_cursor is not None
    assert second_page.next_cursor is None
    with pytest.raises(ValueError, match="limit must be between"):
        store.revisions(limit=501)


def test_xtdb_composition_history_orders_by_system_time() -> None:
    class Connection:
        statement = ""

        def execute(self, statement):
            self.statement = str(statement)
            return self

        def mappings(self):
            return []

    connection = Connection()
    store = XtdbLayerCompositionStore(connection, LayerCompositionStoreConfig())

    assert store.revisions().items == ()
    assert "_system_from AS system_from" in connection.statement
    assert "ORDER BY _system_from, revision_id" in connection.statement
    assert "LIMIT 101" in connection.statement
