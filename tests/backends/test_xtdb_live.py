import importlib.util
import os
import subprocess
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime
from datetime import time as datetime_time
from decimal import Decimal
from uuid import uuid4

import polars as pl
import pytest
from sqlalchemy import Engine, text

from polars_hist_db.backends import DbEngineConfig, XtdbBackend
from polars_hist_db.config import (
    DatasetConfig,
    DeltaConfig,
    TableColumnConfig,
    TableConfig,
    TableConfigs,
)
from polars_hist_db.dataset.entrypoint import (
    _build_delta_table_config,
    _create_config_tables,
)
from polars_hist_db.dataset.scrape import _run_pipeline_as_transaction
from polars_hist_db.loaders.input_source import BatchFinalizer
from polars_hist_db.overrides import (
    AccessGrantInput,
    DocumentAccessStoreConfig,
    OverrideLedgerConfig,
    OverrideOperation,
    OverrideTypedValue,
    SqlOverrideLedgerStore,
    XtdbDocumentAccessStore,
    build_document_access_table_configs,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("POLARS_HIST_DB_XTDB_LIVE") != "1",
        reason="set POLARS_HIST_DB_XTDB_LIVE=1 to run live XTDB tests",
    ),
]


if importlib.util.find_spec("psycopg") is None:
    pytestmark = [
        *pytestmark,
        pytest.mark.skip(reason="install the xtdb extra to run live XTDB tests"),
    ]


def _docker(args: list[str]) -> str:
    return subprocess.check_output(["docker", *args], text=True).strip()


def _published_port(container_id: str) -> int:
    output = _docker(["port", container_id, "5432/tcp"])
    first_binding = output.splitlines()[0]
    return int(first_binding.rsplit(":", 1)[1])


@contextmanager
def _xtdb_engine() -> Iterator[Engine]:
    container_id = _docker(
        ["run", "--rm", "-d", "-p", "5432", "ghcr.io/xtdb/xtdb:nightly"]
    )
    engine = None
    try:
        config = DbEngineConfig(
            backend="xtdb",
            hostname="127.0.0.1",
            port=_published_port(container_id),
        )
        engine = XtdbBackend().create_engine(config)
        deadline = time.monotonic() + 60
        while True:
            try:
                with engine.connect() as connection:
                    connection.execute(text("SELECT 1"))
                break
            except Exception:
                if time.monotonic() > deadline:
                    raise
                time.sleep(1)

        yield engine
    finally:
        if engine is not None:
            engine.dispose()
        subprocess.run(["docker", "rm", "-f", container_id], check=False)


def test_xtdb_live_create_append_read_roundtrip():
    table_config = TableConfig(
        schema="public",
        name=f"live_records_{int(time.time())}",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
            TableColumnConfig("records", "amount_value", "DOUBLE"),
        ],
    )

    with _xtdb_engine() as engine, engine.connect() as connection:
        backend = XtdbBackend()
        backend.table_configs(connection).create(table_config)

        backend.dataframes(connection).table_insert(
            pl.DataFrame(
                {
                    "id": [1, 2],
                    "destination": ["Alpha", "Beta"],
                    "amount_value": [10.5, 20.25],
                }
            ),
            table_config.schema,
            table_config.name,
            table_config=table_config,
        )

        result = backend.dataframes(connection).from_table(
            table_config.schema,
            table_config.name,
        )

    assert result.sort("_id").select(["_id", "destination", "amount_value"]).to_dict(
        as_series=False
    ) == {
        "_id": [1, 2],
        "destination": ["Alpha", "Beta"],
        "amount_value": [10.5, 20.25],
    }


def test_xtdb_live_override_ledger_materializes_on_first_write():
    config = OverrideLedgerConfig(
        schema="public",
        table=f"override_operations_{uuid4().hex}",
    )
    operation = OverrideOperation(
        operation_id="operation-1",
        change_set_id="change-1",
        owner_user_id="owner-1",
        actor_user_id="actor-1",
        feed_id="feed-1",
        entity_id="entity-1",
        field_path="status",
        operation_type="set",
        value=OverrideTypedValue("enum", {"value": "active"}),
        observed_canonical_value_json={"value": "pending"},
        created_against_stale_source=False,
        valid_from=datetime(2026, 8, 8, tzinfo=UTC),
        valid_to=None,
    )

    with _xtdb_engine() as engine:
        backend = XtdbBackend()
        with backend.connection_scope(engine) as connection:
            store = SqlOverrideLedgerStore(connection, "xtdb", config)

            assert store.projected_history_for_owner("owner-1", "feed-1") == []

            store.append(operation)

            history = store.projected_history_for_owner("owner-1", "feed-1")

            assert len(history) == 1
            assert history[0].operation_id == "operation-1"
            assert history[0].value == OverrideTypedValue("enum", {"value": "active"})


def test_xtdb_live_document_access_create():
    suffix = int(time.time())
    document_id = "e9ca81c9-6116-4247-b951-185e82033440"
    config = DocumentAccessStoreConfig(
        schema="public",
        documents_table=f"access_documents_{suffix}",
        grants_table=f"access_grants_{suffix}",
        commands_table=f"access_commands_{suffix}",
    )

    with _xtdb_engine() as engine:
        backend = XtdbBackend()
        with backend.connection_scope(engine) as connection:
            tables = backend.table_configs(connection)
            for table_config in build_document_access_table_configs(config):
                tables.create(table_config)
            store = XtdbDocumentAccessStore(connection, config)
            result = store.create(
                document_id,
                "Shared layer",
                None,
                "user-1",
                datetime.now(UTC),
                initial_grants=(AccessGrantInput("grant-1", "group-1", "manager"),),
                idempotency_key="command-1",
                owning_group="group-1",
            )
            existing = store.create(
                "document-2",
                " shared layer ",
                None,
                "user-1",
                datetime.now(UTC),
                initial_grants=(AccessGrantInput("grant-2", "group-1", "manager"),),
                idempotency_key="command-2",
                owning_group="group-1",
                allow_existing=True,
            )
            other_group = store.create(
                "document-3",
                "Shared layer",
                None,
                "user-2",
                datetime.now(UTC),
                initial_grants=(AccessGrantInput("grant-3", "group-2", "manager"),),
                idempotency_key="command-3",
                owning_group="group-2",
                allow_existing=True,
            )
            documents = store.list_all().items
            loaded = store.get(document_id)

    assert result.accepted is True
    assert existing.document.document_id == document_id
    assert existing.duplicate is True
    assert other_group.document.document_id == "document-3"
    assert {document.document_id for document in documents} == {
        document_id,
        "document-3",
    }
    assert loaded == result.document


def test_xtdb_live_reflection_preserves_caller_transaction_without_metadata():
    table_name = f"live_reflection_{int(time.time())}"
    table_config = TableConfig(
        schema="public",
        name=table_name,
        primary_keys=["id"],
        columns=[
            TableColumnConfig(table_name, "id", "BIGINT", nullable=False),
            TableColumnConfig(table_name, "label", "VARCHAR(255)"),
            TableColumnConfig(table_name, "seen_at", "DATETIME"),
        ],
    )

    with _xtdb_engine() as engine:
        backend = XtdbBackend()
        with backend.connection_scope(engine) as connection:
            backend.dataframes(connection).table_insert(
                pl.DataFrame(
                    {
                        "id": [1],
                        "label": ["Alpha"],
                        "seen_at": [datetime(2026, 7, 18, tzinfo=UTC)],
                    }
                ),
                "public",
                table_name,
                table_config=table_config,
            )

        with engine.begin() as connection:
            reflected = backend.table_configs(connection).from_table(
                "public", table_name
            )
            assert connection.execute(text("SELECT 1")).scalar_one() == 1

    assert {column.name: column.data_type for column in reflected.columns} == {
        "_id": "BIGINT",
        "id": "BIGINT",
        "label": "VARCHAR(255)",
        "seen_at": "TIMESTAMP WITH TIME ZONE",
    }


def test_xtdb_live_validates_supported_physical_type_families():
    table_name = f"live_types_{int(time.time())}"
    table_config = TableConfig(
        schema="public",
        name=table_name,
        primary_keys=["id"],
        columns=[
            TableColumnConfig(table_name, "id", "BIGINT", nullable=False),
            TableColumnConfig(table_name, "enabled", "BOOL", nullable=False),
            TableColumnConfig(table_name, "count", "INT", nullable=False),
            TableColumnConfig(table_name, "ratio", "FLOAT", nullable=False),
            TableColumnConfig(table_name, "measurement", "DOUBLE", nullable=False),
            TableColumnConfig(table_name, "label", "VARCHAR(64)", nullable=False),
            TableColumnConfig(table_name, "amount", "DECIMAL(15,3)", nullable=False),
            TableColumnConfig(table_name, "event_date", "DATE", nullable=False),
            TableColumnConfig(table_name, "event_time", "TIME", nullable=False),
            TableColumnConfig(table_name, "seen_at_local", "TIMESTAMP", nullable=False),
            TableColumnConfig(table_name, "seen_at_utc", "DATETIME", nullable=False),
        ],
    )
    data = pl.DataFrame(
        {
            "id": [1],
            "enabled": [True],
            "count": pl.Series([2], dtype=pl.Int32),
            "ratio": pl.Series([1.5], dtype=pl.Float32),
            "measurement": [2.5],
            "label": ["value"],
            "amount": pl.Series([Decimal("3.125")], dtype=pl.Decimal(15, 3)),
            "event_date": [date(2026, 7, 18)],
            "event_time": [datetime_time(12, 34, 56)],
            "seen_at_local": [datetime.fromisoformat("2026-07-18T12:34:56")],
            "seen_at_utc": [datetime(2026, 7, 18, 12, 34, 56, tzinfo=UTC)],
        }
    )

    with _xtdb_engine() as engine:
        backend = XtdbBackend()
        with backend.connection_scope(engine) as connection:
            backend.table_configs(connection).create(table_config)
            backend.dataframes(connection).table_insert(
                data,
                table_config.schema,
                table_config.name,
                table_config=table_config,
            )
            assert (
                backend.table_configs(connection).create(table_config) == table_config
            )


def test_xtdb_live_insert_non_public_table_with_reserved_column_name():
    table_config = TableConfig(
        schema="source_a",
        name=f"live_entity_info_{int(time.time())}",
        primary_keys=["entity_id"],
        columns=[
            TableColumnConfig("entity_info", "entity_id", "INT", nullable=False),
            TableColumnConfig("entity_info", "name", "VARCHAR(64)"),
            TableColumnConfig("entity_info", "flag", "VARCHAR(64)"),
        ],
    )

    with _xtdb_engine() as engine, engine.connect() as connection:
        backend = XtdbBackend()
        backend.table_configs(connection).create(table_config)

        backend.dataframes(connection).table_insert(
            pl.DataFrame(
                {
                    "entity_id": pl.Series([311038700], dtype=pl.Int32),
                    "name": ["ALPHA"],
                    "flag": ["BS"],
                }
            ),
            table_config.schema,
            table_config.name,
            table_config=table_config,
        )

        result = backend.dataframes(connection).from_table(
            table_config.schema,
            table_config.name,
        )

    assert result.sort("_id").select(["_id", "name", "flag"]).to_dict(
        as_series=False
    ) == {
        "_id": [311038700],
        "name": ["ALPHA"],
        "flag": ["BS"],
    }


def test_xtdb_live_insert_column_with_slash_roundtrip():
    table_config = TableConfig(
        schema="source_a",
        name=f"live_encoded_columns_{int(time.time())}",
        primary_keys=["entity_id"],
        columns=[
            TableColumnConfig("entity_info", "entity_id", "INT", nullable=False),
            TableColumnConfig("entity_info", "capacity/bcm", "DECIMAL(15,3)"),
        ],
    )

    with _xtdb_engine() as engine, engine.connect() as connection:
        backend = XtdbBackend()
        backend.table_configs(connection).create(table_config)

        backend.dataframes(connection).table_insert(
            pl.DataFrame(
                {
                    "entity_id": pl.Series([1], dtype=pl.Int32),
                    "capacity/bcm": pl.Series(
                        [Decimal("4.080")], dtype=pl.Decimal(15, 3)
                    ),
                }
            ),
            table_config.schema,
            table_config.name,
            table_config=table_config,
        )

        result = backend.dataframes(connection).from_table(
            table_config.schema,
            table_config.name,
        )

    assert result.select(["_id", "capacity/bcm"]).to_dict(as_series=False) == {
        "_id": [1],
        "capacity/bcm": [Decimal("4.080")],
    }


def test_xtdb_live_normalizes_foreign_keys_end_to_end():
    tables = TableConfigs(
        items=[
            {
                "schema": "public",
                "name": "live_countries",
                "primary_keys": ["country_id"],
                "columns": [
                    {"name": "country_id", "data_type": "INT", "nullable": False},
                    {"name": "name", "data_type": "VARCHAR(64)", "nullable": False},
                ],
            },
            {
                "schema": "public",
                "name": "live_trades",
                "primary_keys": ["trade_id"],
                "foreign_keys": [
                    {
                        "name": "country_id",
                        "references": {
                            "schema": "public",
                            "table": "live_countries",
                            "column": "country_id",
                        },
                    }
                ],
                "columns": [
                    {"name": "trade_id", "data_type": "INT", "nullable": False},
                    {"name": "country_id", "data_type": "INT", "nullable": False},
                ],
            },
        ]
    )
    dataset = DatasetConfig(
        name="live_trade_upload",
        delta_table_schema="public",
        input_config={"type": "dsv", "search_paths": []},
        delta_config={"drop_unchanged_rows": True},
        pipeline=[
            {
                "schema": "public",
                "table": "live_countries",
                "columns": [
                    {
                        "source": "country_id",
                        "target": "country_id",
                        "deduce_foreign_key": True,
                    },
                    {"source": "country_name", "target": "name"},
                ],
            },
            {
                "schema": "public",
                "table": "live_trades",
                "type": "primary",
                "columns": [
                    {"source": "trade_id", "target": "trade_id"},
                    {"source": "country_id", "target": "country_id"},
                ],
            },
        ],
    )
    upload = pl.DataFrame(
        {
            "trade_id": pl.Series([1, 2, 3], dtype=pl.Int32),
            "country_id": pl.Series([None, None, None], dtype=pl.Int32),
            "country_name": ["Japan", "Korea", "Korea"],
        }
    )
    delta_table_config = _build_delta_table_config(tables, dataset)

    with _xtdb_engine() as engine:
        backend = XtdbBackend()
        _create_config_tables(engine, tables, backend)
        with engine.connect() as connection:
            backend.dataframes(connection).table_insert(
                pl.DataFrame(
                    {
                        "country_id": pl.Series([7], dtype=pl.Int32),
                        "name": ["Japan"],
                    }
                ),
                "public",
                "live_countries",
                table_config=tables["live_countries"],
            )

        for _ in range(2):
            _run_pipeline_as_transaction(
                [(datetime.now(UTC), upload)],
                dataset,
                tables,
                engine,
                BatchFinalizer(),
                delta_table_config=delta_table_config,
                backend=backend,
            )

        with engine.connect() as connection:
            countries = backend.dataframes(connection).from_table(
                "public", "live_countries"
            )
            trades = backend.dataframes(connection).from_table("public", "live_trades")

    country_ids = dict(countries.select("name", "country_id").iter_rows())
    assert country_ids["Japan"] == 7
    assert set(country_ids) == {"Japan", "Korea"}
    assert trades.sort("trade_id").select("trade_id", "country_id").rows() == [
        (1, 7),
        (2, country_ids["Korea"]),
        (3, country_ids["Korea"]),
    ]


def test_xtdb_live_composite_primary_key_roundtrip():
    table_config = TableConfig(
        schema="public",
        name=f"live_composite_records_{int(time.time())}",
        primary_keys=["entity_id", "record_id"],
        columns=[
            TableColumnConfig("records", "entity_id", "BIGINT", nullable=False),
            TableColumnConfig("records", "record_id", "VARCHAR(255)", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
        ],
    )

    with _xtdb_engine() as engine, engine.connect() as connection:
        backend = XtdbBackend()
        backend.table_configs(connection).create(table_config)

        backend.dataframes(connection).table_insert(
            pl.DataFrame(
                {
                    "entity_id": [10, 10],
                    "record_id": ["A", "B"],
                    "destination": ["Alpha", "Beta"],
                }
            ),
            table_config.schema,
            table_config.name,
            table_config=table_config,
        )

        result = backend.dataframes(connection).from_table(
            table_config.schema,
            table_config.name,
        )
        reflected_config = backend.table_configs(connection).from_table(
            table_config.schema,
            table_config.name,
        )

    assert result.sort("record_id").select(
        ["_id", "entity_id", "record_id", "destination"]
    ).to_dict(as_series=False) == {
        "_id": [
            'xtdb-pk-v1:[["entity_id",10],["record_id","A"]]',
            'xtdb-pk-v1:[["entity_id",10],["record_id","B"]]',
        ],
        "entity_id": [10, 10],
        "record_id": ["A", "B"],
        "destination": ["Alpha", "Beta"],
    }
    assert list(reflected_config.primary_keys) == ["entity_id", "record_id"]


def test_xtdb_live_temporal_upsert_honours_update_time_system_time():
    table_config = TableConfig(
        schema="public",
        name=f"live_temporal_records_{int(time.time())}",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
            TableColumnConfig("records", "amount_value", "DOUBLE"),
        ],
    )

    with _xtdb_engine() as engine, engine.connect() as connection:
        backend = XtdbBackend()
        backend.table_configs(connection).create(table_config)

        backend.temporal_upsert(
            pl.DataFrame(
                {
                    "id": [1],
                    "destination": ["Alpha"],
                    "amount_value": [10.5],
                }
            ),
            table_config.schema,
            table_config.name,
            connection=connection,
            table_config=table_config,
            update_time=datetime(2030, 1, 1, tzinfo=UTC),
        )
        backend.temporal_upsert(
            pl.DataFrame(
                {
                    "id": [1],
                    "destination": ["Beta"],
                    "amount_value": [20.25],
                }
            ),
            table_config.schema,
            table_config.name,
            connection=connection,
            table_config=table_config,
            update_time=datetime(2030, 1, 2, tzinfo=UTC),
        )

        table_sql = f"{table_config.schema}.{table_config.name}"

        def read_at(asof: datetime) -> pl.DataFrame:
            timestamp = asof.isoformat()
            return backend.dataframes(connection).from_raw_sql(
                "SELECT _id, destination, amount_value "
                f"FROM {table_sql} "
                f"FOR VALID_TIME AS OF TIMESTAMP '{timestamp}' "
                f"FOR SYSTEM_TIME AS OF TIMESTAMP '{timestamp}'"
            )

        asof_after_second = read_at(datetime(2030, 1, 3, tzinfo=UTC))
        asof_before_first = read_at(datetime(2029, 12, 31, 23, 59, tzinfo=UTC))
        asof_between_updates = read_at(datetime(2030, 1, 1, 12, 0, tzinfo=UTC))

    assert asof_before_first.is_empty()
    assert asof_after_second.select(["_id", "destination", "amount_value"]).to_dict(
        as_series=False
    ) == {
        "_id": [1],
        "destination": ["Beta"],
        "amount_value": [20.25],
    }
    assert asof_between_updates.select(["_id", "destination", "amount_value"]).to_dict(
        as_series=False
    ) == {
        "_id": [1],
        "destination": ["Alpha"],
        "amount_value": [10.5],
    }


def test_xtdb_live_delta_upsert_drops_unchanged_rows():
    table_config = TableConfig(
        schema="public",
        name=f"live_delta_records_{int(time.time())}",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
            TableColumnConfig("records", "amount_value", "DOUBLE"),
        ],
    )
    delta_config = DeltaConfig(
        drop_unchanged_rows=True,
        row_finality="disabled",
    )

    with _xtdb_engine() as engine, engine.connect() as connection:
        backend = XtdbBackend()
        backend.table_configs(connection).create(table_config)

        backend.temporal_upsert(
            pl.DataFrame(
                {
                    "id": [1],
                    "destination": ["Alpha"],
                    "amount_value": [10.5],
                }
            ),
            table_config.schema,
            table_config.name,
            connection=connection,
            table_config=table_config,
            delta_config=delta_config,
            update_time=datetime(2030, 1, 1, tzinfo=UTC),
        )
        unchanged_count = backend.temporal_upsert(
            pl.DataFrame(
                {
                    "id": [1],
                    "destination": ["Alpha"],
                    "amount_value": [10.5],
                }
            ),
            table_config.schema,
            table_config.name,
            connection=connection,
            table_config=table_config,
            delta_config=delta_config,
            update_time=datetime(2030, 1, 2, tzinfo=UTC),
        )
        changed_count = backend.temporal_upsert(
            pl.DataFrame(
                {
                    "id": [1],
                    "destination": ["Beta"],
                    "amount_value": [20.25],
                }
            ),
            table_config.schema,
            table_config.name,
            connection=connection,
            table_config=table_config,
            delta_config=delta_config,
            update_time=datetime(2030, 1, 3, tzinfo=UTC),
        )

        history = backend.dataframes(connection).from_raw_sql(
            "SELECT _id, destination, amount_value, _system_from "
            f"FROM {table_config.schema}.{table_config.name} "
            "FOR VALID_TIME ALL FOR SYSTEM_TIME ALL "
            "ORDER BY _system_from"
        )
        table_sql = f"{table_config.schema}.{table_config.name}"

        def read_at(asof: datetime) -> pl.DataFrame:
            timestamp = asof.isoformat()
            return backend.dataframes(connection).from_raw_sql(
                "SELECT _id, destination, amount_value "
                f"FROM {table_sql} "
                f"FOR VALID_TIME AS OF TIMESTAMP '{timestamp}' "
                f"FOR SYSTEM_TIME AS OF TIMESTAMP '{timestamp}'"
            )

        asof_after_unchanged = read_at(datetime(2030, 1, 2, 12, 0, tzinfo=UTC))
        asof_after_changed = read_at(datetime(2030, 1, 4, tzinfo=UTC))

    assert unchanged_count == 0
    assert changed_count == 1
    system_from_values = [str(value) for value in history["_system_from"].to_list()]
    assert not any(value.startswith("2030-01-02") for value in system_from_values)
    assert asof_after_unchanged.select(["_id", "destination", "amount_value"]).to_dict(
        as_series=False
    ) == {
        "_id": [1],
        "destination": ["Alpha"],
        "amount_value": [10.5],
    }
    assert asof_after_changed.select(["_id", "destination", "amount_value"]).to_dict(
        as_series=False
    ) == {
        "_id": [1],
        "destination": ["Beta"],
        "amount_value": [20.25],
    }


def test_xtdb_live_delta_upsert_takes_last_duplicate_source_key():
    table_config = TableConfig(
        schema="public",
        name=f"live_delta_duplicate_records_{int(time.time())}",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
            TableColumnConfig("records", "amount_value", "DOUBLE"),
        ],
    )
    delta_config = DeltaConfig(
        on_duplicate_key="take_last",
        row_finality="disabled",
    )

    with _xtdb_engine() as engine, engine.connect() as connection:
        backend = XtdbBackend()
        backend.table_configs(connection).create(table_config)

        row_count = backend.temporal_upsert(
            pl.DataFrame(
                {
                    "id": [1, 1],
                    "destination": ["Alpha", "Beta"],
                    "amount_value": [10.5, 20.25],
                }
            ),
            table_config.schema,
            table_config.name,
            connection=connection,
            table_config=table_config,
            delta_config=delta_config,
            update_time=datetime(2030, 1, 1, tzinfo=UTC),
        )

        timestamp = datetime(2030, 1, 2, tzinfo=UTC).isoformat()
        result = backend.dataframes(connection).from_raw_sql(
            "SELECT _id, destination, amount_value "
            f"FROM {table_config.schema}.{table_config.name} "
            f"FOR VALID_TIME AS OF TIMESTAMP '{timestamp}' "
            f"FOR SYSTEM_TIME AS OF TIMESTAMP '{timestamp}'"
        )

    assert row_count == 1
    assert result.select(["_id", "destination", "amount_value"]).to_dict(
        as_series=False
    ) == {
        "_id": [1],
        "destination": ["Beta"],
        "amount_value": [20.25],
    }


def test_xtdb_live_delta_upsert_dropout_deletes_missing_rows():
    table_config = TableConfig(
        schema="public",
        name=f"live_delta_dropout_records_{int(time.time())}",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
            TableColumnConfig("records", "amount_value", "DOUBLE"),
        ],
    )
    delta_config = DeltaConfig(row_finality="dropout")

    with _xtdb_engine() as engine, engine.connect() as connection:
        backend = XtdbBackend()
        backend.table_configs(connection).create(table_config)

        backend.temporal_upsert(
            pl.DataFrame(
                {
                    "id": [1, 2],
                    "destination": ["Alpha", "Beta"],
                    "amount_value": [10.5, 20.25],
                }
            ),
            table_config.schema,
            table_config.name,
            connection=connection,
            table_config=table_config,
            delta_config=delta_config,
            update_time=datetime(2030, 1, 1, tzinfo=UTC),
        )
        changed_count = backend.temporal_upsert(
            pl.DataFrame(
                {
                    "id": [1],
                    "destination": ["Alpha"],
                    "amount_value": [10.5],
                }
            ),
            table_config.schema,
            table_config.name,
            connection=connection,
            table_config=table_config,
            delta_config=delta_config,
            update_time=datetime(2030, 1, 2, tzinfo=UTC),
        )

        table_sql = f"{table_config.schema}.{table_config.name}"

        def read_at(asof: datetime) -> pl.DataFrame:
            timestamp = asof.isoformat()
            return backend.dataframes(connection).from_raw_sql(
                "SELECT _id, destination, amount_value "
                f"FROM {table_sql} "
                f"FOR VALID_TIME AS OF TIMESTAMP '{timestamp}' "
                f"FOR SYSTEM_TIME AS OF TIMESTAMP '{timestamp}' "
                "ORDER BY _id"
            )

        before_dropout = read_at(datetime(2030, 1, 1, 12, tzinfo=UTC))
        after_dropout = read_at(datetime(2030, 1, 3, tzinfo=UTC))

    assert changed_count == 2
    assert before_dropout.select(["_id", "destination", "amount_value"]).to_dict(
        as_series=False
    ) == {
        "_id": [1, 2],
        "destination": ["Alpha", "Beta"],
        "amount_value": [10.5, 20.25],
    }
    assert after_dropout.select(["_id", "destination", "amount_value"]).to_dict(
        as_series=False
    ) == {
        "_id": [1],
        "destination": ["Alpha"],
        "amount_value": [10.5],
    }


def test_xtdb_live_delta_upsert_dropout_closes_missing_rows_at_valid_time():
    table_config = TableConfig(
        schema="public",
        name=f"live_delta_dropout_valid_close_{int(time.time())}",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
        ],
    )
    delta_config = DeltaConfig(row_finality="dropout")

    with _xtdb_engine() as engine, engine.connect() as connection:
        backend = XtdbBackend()
        backend.table_configs(connection).create(table_config)

        backend.temporal_upsert(
            pl.DataFrame(
                {
                    "id": [1, 2],
                    "destination": ["Alpha", "Beta"],
                    "_valid_from": [datetime(1985, 1, 1, tzinfo=UTC)] * 2,
                }
            ),
            table_config.schema,
            table_config.name,
            connection=connection,
            table_config=table_config,
            delta_config=delta_config,
        )
        backend.temporal_upsert(
            pl.DataFrame(
                {
                    "id": [1],
                    "destination": ["Alpha"],
                    "_valid_from": [datetime(1986, 1, 1, tzinfo=UTC)],
                }
            ),
            table_config.schema,
            table_config.name,
            connection=connection,
            table_config=table_config,
            delta_config=delta_config,
        )

        history = backend.dataframes(connection).from_raw_sql(
            f"""
                SELECT _id, destination, _valid_from, _valid_to
                FROM {table_config.schema}.{table_config.name}
                FOR VALID_TIME ALL
                WHERE _id = 2
                ORDER BY _valid_from
                """
        )
        connection.commit()

    history = history.with_columns(
        pl.col(column).dt.replace_time_zone(None)
        for column in ["_valid_from", "_valid_to"]
    )
    assert history.select(["_id", "destination", "_valid_from", "_valid_to"]).to_dict(
        as_series=False
    ) == {
        "_id": [2],
        "destination": ["Beta"],
        "_valid_from": [datetime.fromisoformat("1985-01-01")],
        "_valid_to": [datetime.fromisoformat("1986-01-01")],
    }


def test_xtdb_live_temporal_upsert_honours_explicit_valid_time_window():
    table_config = TableConfig(
        schema="public",
        name=f"live_valid_window_records_{int(time.time())}",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
            TableColumnConfig("records", "amount_value", "DOUBLE"),
        ],
    )

    with _xtdb_engine() as engine, engine.connect() as connection:
        backend = XtdbBackend()
        backend.table_configs(connection).create(table_config)

        row_count = backend.temporal_upsert(
            pl.DataFrame(
                {
                    "id": [1],
                    "destination": ["Alpha"],
                    "amount_value": [10.5],
                    "_valid_from": [datetime(2030, 1, 10, tzinfo=UTC)],
                    "_valid_to": [datetime(2030, 1, 20, tzinfo=UTC)],
                }
            ),
            table_config.schema,
            table_config.name,
            connection=connection,
            table_config=table_config,
            update_time=datetime(2030, 1, 1, tzinfo=UTC),
        )

        table_sql = f"{table_config.schema}.{table_config.name}"

        def read_valid_at(valid_asof: datetime) -> pl.DataFrame:
            valid_timestamp = valid_asof.isoformat()
            system_timestamp = datetime(2030, 1, 2, tzinfo=UTC).isoformat()
            return backend.dataframes(connection).from_raw_sql(
                "SELECT _id, destination, amount_value "
                f"FROM {table_sql} "
                f"FOR VALID_TIME AS OF TIMESTAMP '{valid_timestamp}' "
                f"FOR SYSTEM_TIME AS OF TIMESTAMP '{system_timestamp}'"
            )

        before_window = read_valid_at(datetime(2030, 1, 5, tzinfo=UTC))
        inside_window = read_valid_at(datetime(2030, 1, 15, tzinfo=UTC))
        after_window = read_valid_at(datetime(2030, 1, 25, tzinfo=UTC))

    assert row_count == 1
    assert before_window.is_empty()
    assert inside_window.select(["_id", "destination", "amount_value"]).to_dict(
        as_series=False
    ) == {
        "_id": [1],
        "destination": ["Alpha"],
        "amount_value": [10.5],
    }
    assert after_window.is_empty()
