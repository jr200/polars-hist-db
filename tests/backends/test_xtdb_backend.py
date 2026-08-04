from contextlib import contextmanager
from datetime import UTC, date, datetime, time, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock

import polars as pl
import pytest

from polars_hist_db.backends import DbEngineConfig, XtdbBackend
from polars_hist_db.backends.xtdb import (
    _XTDB_NON_TEMPORAL_VALID_FROM,
    XtdbAdbcDataframeOps,
    XtdbDataframeOps,
    XtdbTableConfigOps,
    _execute_xtdb_dml,
    _execute_xtdb_transaction,
    _validate_xtdb_physical_types,
    _xtdb_cast_type,
    _xtdb_declared_columns,
    _xtdb_physical_type_family,
    _xtdb_transaction_scope,
    _xtdb_type_to_config_type,
)
from polars_hist_db.backends.xtdb_arrow import (
    _prepare_xtdb_insert_dataframe,
    _xtdb_insert_casts,
)
from polars_hist_db.backends.xtdb_query import _xtdb_single_primary_key_alias
from polars_hist_db.backends.xtdb_transport import (
    _xtdb_buffered_transaction_scope,
    _xtdb_buffering_paused,
)
from polars_hist_db.config import (
    DeltaConfig,
    TableColumnConfig,
    TableConfig,
    ValidTimeConfig,
)
from polars_hist_db.overrides import CrdtDocumentStoreConfig, OverrideLedgerConfig

_NON_TEMPORAL_VALID_FROM = _XTDB_NON_TEMPORAL_VALID_FROM


@contextmanager
def _uploaded_keys(dataframe_ops, df, table_schema):
    yield f"{table_schema}.__uploaded_keys"


def test_xtdb_casts_mediumtext_as_text():
    assert _xtdb_cast_type("MEDIUMTEXT") == "TEXT"


def test_xtdb_rejects_unknown_configured_column_type():
    with pytest.raises(ValueError, match="Unsupported XTDB column type: UUID"):
        _xtdb_cast_type("UUID")


def test_xtdb_encodes_binary_primary_keys_as_valid_text_document_ids():
    table = TableConfig(
        name="records",
        schema="test",
        primary_keys=("id",),
        columns=[TableColumnConfig("records", "id", "BINARY(16)")],
    )

    prepared = _prepare_xtdb_insert_dataframe(
        pl.DataFrame({"id": [b"\x01" * 16]}, schema={"id": pl.Binary}), table
    )

    assert prepared["_id"].item().startswith("xtdb-pk-v1:")
    assert _xtdb_insert_casts(prepared, table)[0] == "TEXT"
    assert _xtdb_single_primary_key_alias(table) is None


def test_xtdb_backend_builds_crdt_document_store(monkeypatch):
    class Store:
        def __init__(self, connection, document_store, projection):
            self.connection = connection
            self.document_store = document_store
            self.projection = projection

    monkeypatch.setattr("polars_hist_db.overrides.xtdb.XtdbCrdtDocumentStore", Store)

    store = XtdbBackend().crdt_documents(
        object(), CrdtDocumentStoreConfig(), OverrideLedgerConfig()
    )

    assert isinstance(store, Store)


def test_xtdb_transaction_uses_driver_autocommit_for_explicit_begin():
    driver_connection = Mock()
    driver_connection.autocommit = False
    connection = Mock()
    connection.connection.driver_connection = driver_connection
    connection.in_transaction.return_value = False

    _execute_xtdb_transaction(
        connection, ["ASSERT TRUE", "INSERT INTO test.x (_id) VALUES ('x')"]
    )

    assert driver_connection.execute.call_args_list[0].args == ("BEGIN READ WRITE",)
    assert driver_connection.execute.call_args_list[-1].args == ("COMMIT",)
    assert driver_connection.autocommit is False


def test_xtdb_transaction_scope_commits_all_dml_together():
    driver_connection = Mock()
    driver_connection.autocommit = False
    connection = Mock()
    connection.info = {}
    connection.connection.driver_connection = driver_connection

    with _xtdb_transaction_scope(connection):
        _execute_xtdb_dml(connection, "INSERT INTO test.x (_id) VALUES ('x')")
        _execute_xtdb_dml(connection, "INSERT INTO test.y (_id) VALUES ('y')")

    assert [call.args[0] for call in driver_connection.execute.call_args_list] == [
        "BEGIN READ WRITE",
        "INSERT INTO test.x (_id) VALUES ('x')",
        "INSERT INTO test.y (_id) VALUES ('y')",
        "COMMIT",
    ]


def test_xtdb_buffered_transaction_allows_reads_and_excludes_support_relations():
    driver_connection = Mock()
    driver_connection.autocommit = False
    connection = Mock()
    connection.info = {}
    connection.connection.driver_connection = driver_connection
    connection.in_transaction.return_value = False

    with _xtdb_buffered_transaction_scope(connection):
        _execute_xtdb_dml(connection, "INSERT INTO test.target (_id) VALUES ('x')")
        assert driver_connection.execute.call_count == 0
        with _xtdb_buffering_paused(connection):
            _execute_xtdb_dml(
                connection, "INSERT INTO test.lookup (_id) VALUES ('lookup')"
            )
        _execute_xtdb_dml(connection, "INSERT INTO test.audit (_id) VALUES ('audit')")

    assert [call.args[0] for call in driver_connection.execute.call_args_list] == [
        "BEGIN READ WRITE",
        "INSERT INTO test.lookup (_id) VALUES ('lookup')",
        "COMMIT",
        "BEGIN READ WRITE",
        "INSERT INTO test.target (_id) VALUES ('x')",
        "INSERT INTO test.audit (_id) VALUES ('audit')",
        "COMMIT",
    ]


def test_xtdb_buffered_transaction_retries_invalid_system_time_at_commit():
    driver_connection = Mock()
    driver_connection.autocommit = False
    commit_count = 0

    def execute(statement):
        nonlocal commit_count
        if statement == "COMMIT":
            commit_count += 1
            if commit_count == 1:
                raise RuntimeError("invalid-system-time: specified system-time older")

    driver_connection.execute.side_effect = execute
    connection = Mock()
    connection.info = {}
    connection.connection.driver_connection = driver_connection
    connection.in_transaction.return_value = False
    system_time = datetime(2026, 1, 1, tzinfo=UTC)

    with _xtdb_buffered_transaction_scope(connection, system_time):
        _execute_xtdb_dml(
            connection,
            "INSERT INTO test.target (_id) VALUES ('x')",
            system_time=system_time,
        )

    statements = [call.args[0] for call in driver_connection.execute.call_args_list]
    assert (
        sum(statement.startswith("BEGIN READ WRITE") for statement in statements) == 2
    )
    assert statements.count("INSERT INTO test.target (_id) VALUES ('x')") == 2
    assert statements.count("COMMIT") == 2
    assert statements.count("ROLLBACK") == 1


def test_xtdb_dml_uses_driver_autocommit_for_explicit_begin():
    driver_connection = Mock()
    driver_connection.autocommit = False
    connection = Mock()
    connection.connection.driver_connection = driver_connection
    connection.in_transaction.return_value = False

    _execute_xtdb_dml(connection, "INSERT INTO test.x (_id) VALUES ('x')")

    assert driver_connection.execute.call_args_list[0].args == ("BEGIN READ WRITE",)
    assert driver_connection.execute.call_args_list[-1].args == ("COMMIT",)
    assert driver_connection.autocommit is False


def test_xtdb_create_engine_includes_configured_credentials(monkeypatch):
    calls = []
    engine = Mock()
    engine.dialect = SimpleNamespace()

    def fake_create_engine(url, **kwargs):
        calls.append((url, kwargs))
        return engine

    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.create_engine", fake_create_engine
    )
    config = DbEngineConfig(
        backend="xtdb",
        hostname="xtdb.example.internal",
        port=5432,
        database="xtdb",
        username="xtdb",
        password="secret/pass",
    )

    result = XtdbBackend().create_engine(config)

    assert result is engine
    assert calls[0][0] == (
        "postgresql+psycopg://xtdb:secret%2Fpass@xtdb.example.internal:5432/xtdb"
    )
    assert calls[0][1]["connect_args"] == {"prepare_threshold": None}


def test_xtdb_temporal_upsert_delegates_to_dataframe_insert():
    backend = XtdbBackend()
    ops = Mock()
    ops.table_insert.return_value = 2
    df = pl.DataFrame({"id": [1, 2], "destination": ["Alpha", "Beta"]})
    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
        ],
    )

    result = backend.temporal_upsert(
        df,
        table_config.schema,
        table_config.name,
        dataframe_ops=ops,
        table_config=table_config,
    )

    assert result == 2
    written_df = ops.table_insert.call_args.args[0]
    assert written_df.to_dict(as_series=False) == {
        "id": [1, 2],
        "destination": ["Alpha", "Beta"],
        "_valid_from": [_NON_TEMPORAL_VALID_FROM, _NON_TEMPORAL_VALID_FROM],
    }
    assert ops.table_insert.call_args.kwargs == {"table_config": table_config}


def test_xtdb_temporal_upsert_passes_system_time_to_dataframe_insert():
    backend = XtdbBackend()
    ops = Mock()
    ops.table_insert.return_value = 1
    update_time = datetime(2030, 1, 1, tzinfo=UTC)
    df = pl.DataFrame({"id": [1]})

    result = backend.temporal_upsert(
        df,
        "test",
        "records",
        dataframe_ops=ops,
        update_time=update_time,
    )

    assert result == 1
    ops.table_insert.assert_called_once_with(
        df,
        "test",
        "records",
        table_config=None,
        update_time=update_time,
    )


def test_xtdb_temporal_upsert_rejects_manual_finality():
    backend = XtdbBackend()

    with pytest.raises(NotImplementedError, match="row_finality='disabled'"):
        backend.temporal_upsert(
            pl.DataFrame({"id": [1]}),
            "test",
            "records",
            dataframe_ops=Mock(),
            delta_config=DeltaConfig(row_finality="manual"),
        )


def test_xtdb_temporal_upsert_dropout_deletes_missing_current_keys(monkeypatch):
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb_delta._uploaded_xtdb_relation", _uploaded_keys
    )
    backend = XtdbBackend()
    driver_connection = Mock()
    connection = Mock()
    connection.connection.driver_connection = driver_connection
    connection.in_transaction.return_value = False
    ops = XtdbDataframeOps(connection)
    ops.from_raw_sql = Mock(return_value=pl.DataFrame({"missing_count": [1]}))
    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
        ],
    )

    result = backend.temporal_upsert(
        pl.DataFrame({"id": [1], "destination": ["Alpha"]}),
        "test",
        "records",
        dataframe_ops=ops,
        table_config=table_config,
        delta_config=DeltaConfig(row_finality="dropout"),
    )

    assert result == 2
    assert ops.from_raw_sql.call_args.args[0] == (
        "SELECT COUNT(*) AS missing_count FROM test.records "
        "WHERE _id NOT IN (SELECT _id FROM test.__uploaded_keys "
        "FOR VALID_TIME ALL FOR SYSTEM_TIME ALL)"
    )
    executed_sql = [call.args[0] for call in driver_connection.execute.call_args_list]
    assert executed_sql[1] == (
        "DELETE FROM test.records FOR PORTION OF VALID_TIME FROM "
        "TIMESTAMP WITH TIME ZONE '1970-01-01T00:00:00+00:00' TO NULL "
        "WHERE _id NOT IN (SELECT _id FROM test.__uploaded_keys "
        "FOR VALID_TIME ALL FOR SYSTEM_TIME ALL)"
    )
    insert_call = driver_connection.cursor.return_value.executemany.call_args
    assert insert_call.args[0] == (
        "INSERT INTO test.records (_id, id, destination, _valid_from) "
        "VALUES (%s::BIGINT, %s::BIGINT, %s::TEXT, "
        "%s::TIMESTAMP WITH TIME ZONE)"
    )
    assert insert_call.args[1] == [(1, 1, "Alpha", _NON_TEMPORAL_VALID_FROM)]


def test_xtdb_temporal_upsert_dropout_closes_missing_keys_at_valid_time(monkeypatch):
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb_delta._uploaded_xtdb_relation", _uploaded_keys
    )
    backend = XtdbBackend()
    driver_connection = Mock()
    connection = Mock()
    connection.connection.driver_connection = driver_connection
    connection.in_transaction.return_value = False
    ops = XtdbDataframeOps(connection)
    ops.from_raw_sql = Mock(return_value=pl.DataFrame({"missing_count": [1]}))
    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
        ],
    )

    result = backend.temporal_upsert(
        pl.DataFrame(
            {
                "id": [1],
                "destination": ["Alpha"],
                "_valid_from": [datetime(2030, 1, 2, tzinfo=UTC)],
            }
        ),
        "test",
        "records",
        dataframe_ops=ops,
        table_config=table_config,
        delta_config=DeltaConfig(row_finality="dropout"),
    )

    assert result == 2
    executed_sql = [call.args[0] for call in driver_connection.execute.call_args_list]
    assert executed_sql[1] == (
        "DELETE FROM test.records FOR PORTION OF VALID_TIME FROM "
        "TIMESTAMP WITH TIME ZONE '2030-01-02T00:00:00+00:00' TO NULL "
        "WHERE _id NOT IN (SELECT _id FROM test.__uploaded_keys "
        "FOR VALID_TIME ALL FOR SYSTEM_TIME ALL)"
    )


def test_xtdb_temporal_upsert_dropout_uses_explicit_close_time_for_empty_batches():
    backend = XtdbBackend()
    driver_connection = Mock()
    connection = Mock()
    connection.connection.driver_connection = driver_connection
    connection.in_transaction.return_value = False
    ops = XtdbDataframeOps(connection)
    ops.from_raw_sql = Mock(return_value=pl.DataFrame({"missing_count": [2]}))
    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
        ],
    )

    result = backend.temporal_upsert(
        pl.DataFrame(schema={"id": pl.Int64, "destination": pl.String}),
        "test",
        "records",
        dataframe_ops=ops,
        table_config=table_config,
        delta_config=DeltaConfig(row_finality="dropout"),
        dropout_close_time=datetime(2030, 1, 3, tzinfo=UTC),
    )

    assert result == 2
    executed_sql = [call.args[0] for call in driver_connection.execute.call_args_list]
    assert executed_sql[1] == (
        "DELETE FROM test.records FOR PORTION OF VALID_TIME FROM "
        "TIMESTAMP WITH TIME ZONE '2030-01-03T00:00:00+00:00' TO NULL "
        "WHERE TRUE"
    )


def test_xtdb_temporal_upsert_rejects_duplicate_source_keys_by_default():
    backend = XtdbBackend()
    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
        ],
    )

    with pytest.raises(ValueError, match="duplicate source keys"):
        backend.temporal_upsert(
            pl.DataFrame({"id": [1, 1], "destination": ["Alpha", "Beta"]}),
            "test",
            "records",
            dataframe_ops=Mock(),
            table_config=table_config,
            delta_config=DeltaConfig(),
        )


def test_xtdb_temporal_upsert_takes_first_duplicate_source_key():
    backend = XtdbBackend()
    ops = Mock()
    ops.table_insert.return_value = 1
    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
        ],
    )

    result = backend.temporal_upsert(
        pl.DataFrame({"id": [1, 1], "destination": ["Alpha", "Beta"]}),
        "test",
        "records",
        dataframe_ops=ops,
        table_config=table_config,
        delta_config=DeltaConfig(on_duplicate_key="take_first"),
    )

    assert result == 1
    written_df = ops.table_insert.call_args.args[0]
    assert written_df.to_dict(as_series=False) == {
        "id": [1],
        "destination": ["Alpha"],
        "_valid_from": [_NON_TEMPORAL_VALID_FROM],
    }


def test_xtdb_temporal_upsert_takes_last_duplicate_source_key():
    backend = XtdbBackend()
    ops = Mock()
    ops.table_insert.return_value = 1
    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
        ],
    )

    result = backend.temporal_upsert(
        pl.DataFrame({"id": [1, 1], "destination": ["Alpha", "Beta"]}),
        "test",
        "records",
        dataframe_ops=ops,
        table_config=table_config,
        delta_config=DeltaConfig(on_duplicate_key="take_last"),
    )

    assert result == 1
    written_df = ops.table_insert.call_args.args[0]
    assert written_df.to_dict(as_series=False) == {
        "id": [1],
        "destination": ["Beta"],
        "_valid_from": [_NON_TEMPORAL_VALID_FROM],
    }


def test_xtdb_temporal_upsert_drop_unchanged_treats_missing_table_as_empty():
    backend = XtdbBackend()
    ops = Mock()
    ops.table_query.side_effect = Exception("Table not found: test.records")
    ops.table_insert.return_value = 1
    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
        ],
    )

    result = backend.temporal_upsert(
        pl.DataFrame({"id": [1], "destination": ["Alpha"]}),
        "test",
        "records",
        dataframe_ops=ops,
        table_config=table_config,
        delta_config=DeltaConfig(drop_unchanged_rows=True),
    )

    assert result == 1
    written_df = ops.table_insert.call_args.args[0]
    assert written_df.to_dict(as_series=False) == {
        "id": [1],
        "destination": ["Alpha"],
        "_valid_from": [_NON_TEMPORAL_VALID_FROM],
    }


def test_xtdb_temporal_upsert_dropout_treats_missing_table_as_empty():
    backend = XtdbBackend()
    driver_connection = Mock()
    connection = Mock()
    connection.connection.driver_connection = driver_connection
    connection.in_transaction.return_value = False
    ops = XtdbDataframeOps(connection)
    ops.from_raw_sql = Mock(side_effect=Exception("Table not found: test.records"))
    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
        ],
    )

    result = backend.temporal_upsert(
        pl.DataFrame({"id": [1], "destination": ["Alpha"]}),
        "test",
        "records",
        dataframe_ops=ops,
        table_config=table_config,
        delta_config=DeltaConfig(row_finality="dropout"),
    )

    assert result == 1
    executed_sql = [call.args[0] for call in driver_connection.execute.call_args_list]
    assert not any(sql.startswith("DELETE FROM") for sql in executed_sql)


def test_xtdb_temporal_upsert_treats_explicit_valid_time_change_as_changed():
    backend = XtdbBackend()
    ops = Mock()
    ops.table_query.return_value = pl.DataFrame(
        {
            "_id": [1],
            "destination": ["Alpha"],
            "_valid_from": [datetime(2030, 1, 1, tzinfo=UTC)],
            "_valid_to": [datetime(2030, 2, 1, tzinfo=UTC)],
        }
    )
    ops.table_insert.return_value = 1
    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
        ],
    )

    result = backend.temporal_upsert(
        pl.DataFrame(
            {
                "id": [1],
                "destination": ["Alpha"],
                "_valid_from": [datetime(2030, 1, 1, tzinfo=UTC)],
                "_valid_to": [datetime(2030, 3, 1, tzinfo=UTC)],
            }
        ),
        "test",
        "records",
        dataframe_ops=ops,
        table_config=table_config,
        delta_config=DeltaConfig(drop_unchanged_rows=True),
        update_time=datetime(2030, 1, 2, tzinfo=UTC),
    )

    assert result == 1
    written_df = ops.table_insert.call_args.args[0]
    assert written_df.to_dict(as_series=False) == {
        "id": [1],
        "destination": ["Alpha"],
        "_valid_from": [datetime(2030, 1, 1, tzinfo=UTC)],
        "_valid_to": [datetime(2030, 3, 1, tzinfo=UTC)],
    }


def test_xtdb_temporal_upsert_ignores_valid_from_when_filtering_unchanged_rows():
    backend = XtdbBackend()
    ops = Mock()
    ops.table_query.return_value = pl.DataFrame(
        {
            "_id": [1],
            "destination": ["Alpha"],
            "_valid_from": [datetime(2030, 1, 1, tzinfo=UTC)],
        }
    )
    ops.table_insert.return_value = 0
    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
        ],
    )

    result = backend.temporal_upsert(
        pl.DataFrame(
            {
                "id": [1],
                "destination": ["Alpha"],
                "_valid_from": [datetime(2030, 2, 1, tzinfo=UTC)],
            }
        ),
        "test",
        "records",
        dataframe_ops=ops,
        table_config=table_config,
        delta_config=DeltaConfig(drop_unchanged_rows=True),
    )

    assert result == 0
    ops.table_insert.assert_called_once()
    assert ops.table_insert.call_args.args[0].is_empty()


def test_xtdb_temporal_upsert_normalizes_types_when_filtering_unchanged_rows():
    backend = XtdbBackend()
    ops = Mock()
    ops.table_query.return_value = pl.DataFrame(
        {
            "_id": [1],
            "destination": ["Alpha"],
            "float_col": [12.34],
        }
    )
    ops.table_insert.return_value = 0
    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "INT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
            TableColumnConfig("records", "float_col", "FLOAT"),
        ],
    )

    result = backend.temporal_upsert(
        pl.DataFrame(
            {
                "id": [1],
                "destination": ["Alpha"],
                "float_col": [12.34],
            },
            schema_overrides={"float_col": pl.Float32},
        ),
        "test",
        "records",
        dataframe_ops=ops,
        table_config=table_config,
        delta_config=DeltaConfig(drop_unchanged_rows=True),
    )

    assert result == 0
    ops.table_insert.assert_called_once()
    assert ops.table_insert.call_args.args[0].is_empty()


def test_xtdb_temporal_upsert_prefills_configured_defaults_before_insert():
    backend = XtdbBackend()
    ops = Mock()
    ops.table_insert.return_value = 1
    table_config = TableConfig(
        schema="test",
        name="defaults",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("defaults", "id", "INT", nullable=False),
            TableColumnConfig("defaults", "enabled", "BOOLEAN", default_value="0"),
            TableColumnConfig("defaults", "as_of", "DATE", default_value="1985-10-26"),
            TableColumnConfig("defaults", "cutoff", "TIME", default_value="01:20:00"),
            TableColumnConfig(
                "defaults",
                "price",
                "DECIMAL(10,2)",
                default_value="2.71",
            ),
        ],
    )

    result = backend.temporal_upsert(
        pl.DataFrame(
            {
                "id": [1],
                "enabled": [None],
                "as_of": [None],
                "cutoff": [None],
                "price": [None],
            },
            schema_overrides={
                "id": pl.Int32,
                "enabled": pl.Boolean,
                "as_of": pl.Date,
                "cutoff": pl.Time,
                "price": pl.Decimal(10, 2),
            },
        ),
        "test",
        "defaults",
        dataframe_ops=ops,
        table_config=table_config,
        delta_config=DeltaConfig(prefill_nulls_with_default=True),
    )

    assert result == 1
    written_df = ops.table_insert.call_args.args[0]
    assert written_df.to_dict(as_series=False) == {
        "id": [1],
        "enabled": [False],
        "as_of": [date(1985, 10, 26)],
        "cutoff": [time(1, 20)],
        "price": [Decimal("2.71")],
        "_valid_from": [_NON_TEMPORAL_VALID_FROM],
    }


def test_xtdb_temporal_upsert_materializes_missing_configured_columns():
    backend = XtdbBackend()
    ops = Mock()
    ops.table_insert.return_value = 1
    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "INT", nullable=False),
            TableColumnConfig("records", "description", "VARCHAR(255)"),
            TableColumnConfig("records", "amount", "DECIMAL(10,2)"),
            TableColumnConfig("records", "observed_at", "DATETIME"),
        ],
    )

    result = backend.temporal_upsert(
        pl.DataFrame({"id": pl.Series([1], dtype=pl.Int32)}),
        "test",
        "records",
        dataframe_ops=ops,
        table_config=table_config,
        delta_config=DeltaConfig(prefill_nulls_with_default=False),
    )

    assert result == 1
    written_df = ops.table_insert.call_args.args[0]
    assert written_df.columns == [
        "id",
        "description",
        "amount",
        "observed_at",
        "_valid_from",
    ]
    assert written_df.schema == {
        "id": pl.Int32,
        "description": pl.String,
        "amount": pl.Decimal(10, 2),
        "observed_at": pl.Datetime("us"),
        "_valid_from": pl.Datetime("us", "UTC"),
    }
    assert written_df.select("description", "amount", "observed_at").null_count().row(
        0
    ) == (1, 1, 1)


def test_xtdb_temporal_upsert_treats_null_to_value_as_changed():
    backend = XtdbBackend()
    ops = Mock()
    ops.table_query.return_value = pl.DataFrame(
        {
            "_id": [1],
            "amount_value": [None],
        },
        schema_overrides={"amount_value": pl.Float64},
    )
    ops.table_insert.return_value = 1
    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "amount_value", "DOUBLE"),
        ],
    )

    result = backend.temporal_upsert(
        pl.DataFrame({"id": [1], "amount_value": [330.33]}),
        "test",
        "records",
        dataframe_ops=ops,
        table_config=table_config,
        delta_config=DeltaConfig(drop_unchanged_rows=True),
    )

    assert result == 1
    written_df = ops.table_insert.call_args.args[0]
    assert written_df.to_dict(as_series=False) == {
        "id": [1],
        "amount_value": [330.33],
        "_valid_from": [_NON_TEMPORAL_VALID_FROM],
    }


def test_xtdb_temporal_upsert_maps_configured_valid_time_columns():
    backend = XtdbBackend()
    ops = Mock()
    ops.table_insert.return_value = 1
    asof_time = datetime(2030, 1, 1, 12, 0, tzinfo=UTC)
    expiry_time = datetime(2030, 2, 1, 12, 0, tzinfo=UTC)

    result = backend.temporal_upsert(
        pl.DataFrame(
            {
                "id": [1],
                "destination": ["Alpha"],
                "msg_timestamp": [asof_time],
                "valid_until": [expiry_time],
            }
        ),
        "test",
        "records",
        dataframe_ops=ops,
        valid_time=ValidTimeConfig(
            table="records",
            from_column="msg_timestamp",
            to_column="valid_until",
        ),
    )

    assert result == 1
    written_df = ops.table_insert.call_args.args[0]
    assert written_df.to_dict(as_series=False) == {
        "id": [1],
        "destination": ["Alpha"],
        "msg_timestamp": [asof_time],
        "valid_until": [expiry_time],
        "_valid_from": [asof_time],
        "_valid_to": [expiry_time],
    }


def test_xtdb_temporal_upsert_rejects_missing_valid_time_source_column():
    backend = XtdbBackend()

    with pytest.raises(ValueError, match="missing source column"):
        backend.temporal_upsert(
            pl.DataFrame({"id": [1], "destination": ["Alpha"]}),
            "test",
            "records",
            dataframe_ops=Mock(),
            valid_time=ValidTimeConfig(
                table="records",
                from_column="msg_timestamp",
            ),
        )


@pytest.mark.parametrize(
    ("data", "valid_time", "missing_column"),
    [
        (
            {
                "id": [1],
                "destination": ["Alpha"],
                "msg_timestamp": [None],
            },
            ValidTimeConfig(table="records", from_column="msg_timestamp"),
            "msg_timestamp",
        ),
        (
            {
                "id": [1],
                "destination": ["Alpha"],
                "msg_timestamp": [datetime(2030, 1, 1, tzinfo=UTC)],
                "valid_until": [None],
            },
            ValidTimeConfig(
                table="records",
                from_column="msg_timestamp",
                to_column="valid_until",
            ),
            "valid_until",
        ),
    ],
)
def test_xtdb_temporal_upsert_rejects_null_valid_time_source_column(
    data, valid_time, missing_column
):
    backend = XtdbBackend()

    with pytest.raises(ValueError, match=f"null source value.*{missing_column}"):
        backend.temporal_upsert(
            pl.DataFrame(data),
            "test",
            "records",
            dataframe_ops=Mock(),
            valid_time=valid_time,
        )


def test_xtdb_temporal_upsert_rejects_valid_time_target_conflict():
    backend = XtdbBackend()

    with pytest.raises(ValueError, match="already contains that column"):
        backend.temporal_upsert(
            pl.DataFrame(
                {
                    "id": [1],
                    "msg_timestamp": [datetime(2030, 1, 1, tzinfo=UTC)],
                    "_valid_from": [datetime(2030, 1, 2, tzinfo=UTC)],
                }
            ),
            "test",
            "records",
            dataframe_ops=Mock(),
            valid_time=ValidTimeConfig(
                table="records",
                from_column="msg_timestamp",
            ),
        )


def test_xtdb_table_creation_maps_mysql_compatibility_types(monkeypatch):
    connection = Mock()
    connection.connection = None
    ops = XtdbTableConfigOps(connection)
    monkeypatch.setattr(ops, "table_exists", Mock(return_value=False))

    table_config = TableConfig(
        schema="test",
        name="compat_types",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("compat_types", "id", "INT", nullable=False),
            TableColumnConfig("compat_types", "bool_col", "BOOL"),
            TableColumnConfig("compat_types", "bit_col", "BIT"),
            TableColumnConfig("compat_types", "tinyint_col", "TINYINT"),
            TableColumnConfig("compat_types", "mediumint_col", "MEDIUMINT"),
            TableColumnConfig("compat_types", "datetime_col", "DATETIME"),
            TableColumnConfig("compat_types", "time_col", "TIME"),
        ],
    )

    ops.create(table_config)

    executed_sql = [call.args[0].text for call in connection.execute.call_args_list]
    assert all(not sql.startswith("CREATE TABLE") for sql in executed_sql)
    assert executed_sql == [
        (
            "INSERT INTO test.__polars_hist_db_xtdb_table_configs "
            "(_id, table_schema, table_name, primary_keys_json, id_policy, "
            "columns_json, foreign_keys_json, is_temporal) "
            "VALUES ('test.compat_types'::TEXT, 'test'::TEXT, 'compat_types'::TEXT, "
            "'[\"id\"]'::TEXT, 'single-key'::TEXT, "
            '\'[{"table":"compat_types","name":"id","data_type":"INT",'
            '"default_value":null,"autoincrement":false,"nullable":false,'
            '"unique_constraint":[]},{"table":"compat_types","name":"bool_col",'
            '"data_type":"BOOL","default_value":null,"autoincrement":false,'
            '"nullable":true,"unique_constraint":[]},{"table":"compat_types",'
            '"name":"bit_col","data_type":"BIT","default_value":null,'
            '"autoincrement":false,"nullable":true,"unique_constraint":[]},'
            '{"table":"compat_types","name":"tinyint_col","data_type":"TINYINT",'
            '"default_value":null,"autoincrement":false,"nullable":true,'
            '"unique_constraint":[]},{"table":"compat_types","name":"mediumint_col",'
            '"data_type":"MEDIUMINT","default_value":null,"autoincrement":false,'
            '"nullable":true,"unique_constraint":[]},{"table":"compat_types",'
            '"name":"datetime_col","data_type":"DATETIME","default_value":null,'
            '"autoincrement":false,"nullable":true,"unique_constraint":[]},'
            '{"table":"compat_types","name":"time_col","data_type":"TIME",'
            '"default_value":null,"autoincrement":false,"nullable":true,'
            "\"unique_constraint\":[]}]'::TEXT, '[]'::TEXT, FALSE::BOOLEAN)"
        ),
    ]
    assert _xtdb_declared_columns(table_config) == [
        "_id",
        "id",
        "bool_col",
        "bit_col",
        "tinyint_col",
        "mediumint_col",
        "datetime_col",
        "time_col",
    ]
    assert [column.data_type for column in table_config.columns[1:]] == [
        "BOOL",
        "BIT",
        "TINYINT",
        "MEDIUMINT",
        "DATETIME",
        "TIME",
    ]


def test_xtdb_backend_returns_explicit_table_config_ops():
    connection = object()
    ops = XtdbBackend().table_configs(connection)

    assert isinstance(ops, XtdbTableConfigOps)


def test_xtdb_table_config_ops_drop_all_erases_configured_tables(monkeypatch):
    executed = []

    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb_schema._execute_xtdb_dml",
        lambda _connection, sql, **_kwargs: executed.append(sql),
    )

    table_config = TableConfig(
        schema="market",
        name="prices",
        primary_keys=["id"],
        columns=[TableColumnConfig("prices", "id", "INT", nullable=False)],
    )

    ops = XtdbTableConfigOps(object())
    monkeypatch.setattr(ops, "table_exists", Mock(return_value=True))

    ops.drop_all(Mock(items=[table_config]))

    assert executed == [
        "ERASE FROM market.prices WHERE TRUE",
        (
            "DELETE FROM market.__polars_hist_db_xtdb_table_configs "
            "WHERE _id = 'market.prices'::TEXT"
        ),
    ]


def test_xtdb_table_config_ops_drop_removes_metadata_without_data_table(monkeypatch):
    executed = []

    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb_schema._execute_xtdb_dml",
        lambda _connection, sql, **_kwargs: executed.append(sql),
    )

    table_config = TableConfig(
        schema="market",
        name="prices",
        primary_keys=["id"],
        columns=[TableColumnConfig("prices", "id", "INT", nullable=False)],
    )

    ops = XtdbTableConfigOps(object())
    monkeypatch.setattr(
        ops,
        "table_exists",
        Mock(
            side_effect=lambda _schema, table: (
                table == "__polars_hist_db_xtdb_table_configs"
            )
        ),
    )

    ops.drop(table_config)

    assert executed == [
        (
            "DELETE FROM market.__polars_hist_db_xtdb_table_configs "
            "WHERE _id = 'market.prices'::TEXT"
        ),
    ]


def test_xtdb_backend_create_engine_targets_xtdb_database_by_default(monkeypatch):
    calls = []
    engine = Mock()
    engine.dialect = SimpleNamespace()

    def fake_create_engine(url, **kwargs):
        calls.append((url, kwargs))
        return engine

    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.create_engine", fake_create_engine
    )

    XtdbBackend().create_engine(
        DbEngineConfig(backend="xtdb", hostname="127.0.0.1", port=15432)
    )

    assert calls[0][0] == "postgresql+psycopg://127.0.0.1:15432/xtdb"


def test_xtdb_backend_create_engine_disables_psycopg_prepared_statements(
    monkeypatch,
):
    calls = []
    engine = Mock()
    engine.dialect = SimpleNamespace()

    def fake_create_engine(url, **kwargs):
        calls.append((url, kwargs))
        return engine

    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.create_engine", fake_create_engine
    )

    result = XtdbBackend().create_engine(
        DbEngineConfig(backend="xtdb", hostname="127.0.0.1", port=15432)
    )

    assert result is engine
    assert calls[0][1]["connect_args"] == {"prepare_threshold": None}


def test_xtdb_backend_create_engine_uses_configured_database(monkeypatch):
    calls = []
    engine = Mock()
    engine.dialect = SimpleNamespace()

    def fake_create_engine(url, **kwargs):
        calls.append((url, kwargs))
        return engine

    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.create_engine", fake_create_engine
    )

    XtdbBackend().create_engine(
        DbEngineConfig(
            backend="xtdb",
            hostname="127.0.0.1",
            port=15432,
            database="analytics",
        )
    )

    assert calls[0][0] == "postgresql+psycopg://127.0.0.1:15432/analytics"


def test_xtdb_table_creation_records_configured_columns_without_ddl(monkeypatch):
    connection = Mock()
    connection.connection = None
    ops = XtdbTableConfigOps(connection)
    monkeypatch.setattr(ops, "table_exists", Mock(return_value=False))

    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
            TableColumnConfig("records", "amount_value", "DECIMAL(20,6)"),
        ],
    )
    result = ops.create(table_config)

    assert result is table_config
    executed_sql = [call.args[0].text for call in connection.execute.call_args_list]
    assert all(not sql.startswith("CREATE TABLE") for sql in executed_sql)
    assert executed_sql == [
        (
            "INSERT INTO test.__polars_hist_db_xtdb_table_configs "
            "(_id, table_schema, table_name, primary_keys_json, id_policy, "
            "columns_json, foreign_keys_json, is_temporal) "
            "VALUES ('test.records'::TEXT, 'test'::TEXT, 'records'::TEXT, "
            "'[\"id\"]'::TEXT, 'single-key'::TEXT, "
            '\'[{"table":"records","name":"id","data_type":"BIGINT",'
            '"default_value":null,"autoincrement":false,"nullable":false,'
            '"unique_constraint":[]},{"table":"records","name":"destination",'
            '"data_type":"VARCHAR(255)","default_value":null,"autoincrement":false,'
            '"nullable":true,"unique_constraint":[]},{"table":"records",'
            '"name":"amount_value","data_type":"DECIMAL(20,6)","default_value":null,'
            '"autoincrement":false,"nullable":true,"unique_constraint":[]}]'
            "'::TEXT, '[]'::TEXT, FALSE::BOOLEAN)"
        ),
    ]


def test_xtdb_table_creation_is_idempotent_when_table_exists(monkeypatch):
    connection = Mock()
    ops = XtdbTableConfigOps(connection)
    monkeypatch.setattr(ops, "table_exists", Mock(return_value=True))
    existing_config = TableConfig(schema="test", name="records", columns=[])
    monkeypatch.setattr(ops, "from_table", Mock(return_value=existing_config))

    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["_id"],
        columns=[TableColumnConfig("records", "_id", "BIGINT", nullable=False)],
    )

    result = ops.create(table_config)

    assert result is existing_config
    connection.execute.assert_not_called()
    ops.from_table.assert_called_once_with("test", "records")


def test_xtdb_table_creation_records_composite_primary_key_columns(monkeypatch):
    connection = Mock()
    connection.connection = None
    ops = XtdbTableConfigOps(connection)
    monkeypatch.setattr(ops, "table_exists", Mock(return_value=False))

    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["entity_id", "record_id"],
        columns=[
            TableColumnConfig("records", "entity_id", "BIGINT", nullable=False),
            TableColumnConfig("records", "record_id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
        ],
    )

    result = ops.create(table_config)

    assert result is table_config
    executed_sql = [call.args[0].text for call in connection.execute.call_args_list]
    assert all(not sql.startswith("CREATE TABLE") for sql in executed_sql)
    assert executed_sql == [
        (
            "INSERT INTO test.__polars_hist_db_xtdb_table_configs "
            "(_id, table_schema, table_name, primary_keys_json, id_policy, "
            "columns_json, foreign_keys_json, is_temporal) "
            "VALUES ('test.records'::TEXT, 'test'::TEXT, 'records'::TEXT, "
            "'[\"entity_id\",\"record_id\"]'::TEXT, 'xtdb-pk-v1'::TEXT, "
            '\'[{"table":"records","name":"entity_id","data_type":"BIGINT",'
            '"default_value":null,"autoincrement":false,"nullable":false,'
            '"unique_constraint":[]},{"table":"records","name":"record_id",'
            '"data_type":"BIGINT","default_value":null,"autoincrement":false,'
            '"nullable":false,"unique_constraint":[]},{"table":"records",'
            '"name":"destination","data_type":"VARCHAR(255)","default_value":null,'
            '"autoincrement":false,"nullable":true,"unique_constraint":[]}]'
            "'::TEXT, '[]'::TEXT, FALSE::BOOLEAN)"
        ),
    ]


def test_xtdb_table_creation_records_primary_key_metadata(monkeypatch):
    connection = Mock()
    connection.connection = None
    ops = XtdbTableConfigOps(connection)
    monkeypatch.setattr(ops, "table_exists", Mock(return_value=False))

    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["entity_id", "record_id"],
        columns=[
            TableColumnConfig("records", "entity_id", "BIGINT", nullable=False),
            TableColumnConfig("records", "record_id", "VARCHAR(255)", nullable=False),
        ],
    )

    ops.create(table_config)

    executed_sql = [call.args[0].text for call in connection.execute.call_args_list]
    assert executed_sql == [
        (
            "INSERT INTO test.__polars_hist_db_xtdb_table_configs "
            "(_id, table_schema, table_name, primary_keys_json, id_policy, "
            "columns_json, foreign_keys_json, is_temporal) "
            "VALUES ('test.records'::TEXT, 'test'::TEXT, 'records'::TEXT, "
            "'[\"entity_id\",\"record_id\"]'::TEXT, 'xtdb-pk-v1'::TEXT, "
            '\'[{"table":"records","name":"entity_id","data_type":"BIGINT",'
            '"default_value":null,"autoincrement":false,"nullable":false,'
            '"unique_constraint":[]},{"table":"records","name":"record_id",'
            '"data_type":"VARCHAR(255)","default_value":null,"autoincrement":false,'
            '"nullable":false,"unique_constraint":[]}]\'::TEXT, \'[]\'::TEXT, '
            "FALSE::BOOLEAN)"
        ),
    ]


def test_xtdb_table_reflection_builds_table_config_from_information_schema(
    monkeypatch,
):
    read_database = Mock(
        return_value=pl.DataFrame(
            {
                "column_name": [
                    "_id",
                    "destination",
                    "amount_value",
                    "_system_from",
                    "_system_to",
                ],
                "data_type": [
                    ":i64",
                    ":utf8",
                    "DECIMAL(10,4)",
                    "TIMESTAMP",
                    "TIMESTAMP",
                ],
                "is_nullable": ["NO", "YES", "YES", "YES", "YES"],
            }
        )
    )
    monkeypatch.setattr(pl, "read_database", read_database)

    connection = object()
    ops = XtdbTableConfigOps(connection)

    table_config = ops.from_table("test", "records")

    assert table_config.schema == "test"
    assert table_config.name == "records"
    assert list(table_config.primary_keys) == ["_id"]
    assert [
        (col.name, col.data_type, col.nullable) for col in table_config.columns
    ] == [
        ("_id", "BIGINT", False),
        ("destination", "VARCHAR(255)", True),
        ("amount_value", "DECIMAL(10,4)", True),
    ]
    assert read_database.call_args_list[0].args == (
        """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'test'
              AND table_name = 'records'
            ORDER BY ordinal_position
        """,
        connection,
    )


def test_xtdb_table_reflection_does_not_query_missing_config_metadata(monkeypatch):
    read_database = Mock(
        side_effect=[
            pl.DataFrame(
                {
                    "column_name": ["_id", "destination"],
                    "data_type": [":i64", "[:? :UTF8]"],
                    "is_nullable": ["NO", "YES"],
                }
            ),
            pl.DataFrame({"table_name": []}, schema={"table_name": pl.String}),
        ]
    )
    monkeypatch.setattr(pl, "read_database", read_database)

    table_config = XtdbTableConfigOps(object()).from_table("test", "records")

    assert [(column.name, column.data_type) for column in table_config.columns] == [
        ("_id", "BIGINT"),
        ("destination", "VARCHAR(255)"),
    ]
    assert read_database.call_count == 2


def test_xtdb_table_reflection_prefers_configured_column_metadata(monkeypatch):
    columns_json = (
        '[{"table":"all_types","name":"id","data_type":"INT",'
        '"default_value":null,"autoincrement":true,"nullable":false,'
        '"unique_constraint":[]},{"table":"all_types","name":"decimal_col",'
        '"data_type":"DECIMAL(10,2)","default_value":null,'
        '"autoincrement":false,"nullable":true,"unique_constraint":[]},'
        '{"table":"all_types","name":"real_col","data_type":"REAL",'
        '"default_value":null,"autoincrement":false,"nullable":true,'
        '"unique_constraint":[]}]'
    )
    read_database = Mock(
        side_effect=[
            pl.DataFrame(
                {
                    "column_name": ["_id", "id", "decimal_col", "real_col"],
                    "data_type": [":i32", ":i32", "[:DECIMAL 38 2]", ":f64"],
                    "is_nullable": ["NO", "NO", "YES", "YES"],
                }
            ),
            pl.DataFrame({"table_name": ["__polars_hist_db_xtdb_table_configs"]}),
            pl.DataFrame(
                {
                    "primary_keys_json": ['["id"]'],
                    "id_policy": ["single-key"],
                    "columns_json": [columns_json],
                    "is_temporal": [False],
                }
            ),
            pl.DataFrame(
                {
                    "primary_keys_json": ['["id"]'],
                    "id_policy": ["single-key"],
                    "columns_json": [columns_json],
                }
            ),
        ]
    )
    monkeypatch.setattr(pl, "read_database", read_database)

    table_config = XtdbTableConfigOps(object()).from_table("test", "all_types")

    assert list(table_config.primary_keys) == ["id"]
    assert [(col.name, col.data_type) for col in table_config.columns] == [
        ("id", "INT"),
        ("decimal_col", "DECIMAL(10,2)"),
        ("real_col", "REAL"),
    ]
    assert table_config.is_temporal is False


def test_xtdb_table_reflection_returns_recorded_config_before_physical_rows(
    monkeypatch,
):
    columns_json = (
        '[{"table":"records","name":"id","data_type":"INT",'
        '"default_value":null,"autoincrement":false,"nullable":false,'
        '"unique_constraint":[]}]'
    )
    read_database = Mock(
        side_effect=[
            pl.DataFrame(),
            pl.DataFrame({"table_name": ["__polars_hist_db_xtdb_table_configs"]}),
            pl.DataFrame(
                {
                    "primary_keys_json": ['["id"]'],
                    "columns_json": [columns_json],
                    "is_temporal": [False],
                }
            ),
        ]
    )
    monkeypatch.setattr(pl, "read_database", read_database)

    table_config = XtdbTableConfigOps(object()).from_table("test", "records")

    assert table_config.primary_keys == ["id"]
    assert table_config.is_temporal is False
    assert [(column.name, column.data_type) for column in table_config.columns] == [
        ("id", "INT")
    ]


def test_xtdb_table_reflection_restores_foreign_key_metadata(monkeypatch):
    columns_json = (
        '[{"table":"trading_pairs","name":"id","data_type":"INT",'
        '"default_value":null,"autoincrement":true,"nullable":false,'
        '"unique_constraint":[]},{"table":"trading_pairs",'
        '"name":"exchange_id","data_type":"INT","default_value":null,'
        '"autoincrement":false,"nullable":false,"unique_constraint":[]}]'
    )
    foreign_keys_json = (
        '[{"name":"exchange_id","references":{"schema":"test",'
        '"table":"exchanges","column":"id"}}]'
    )
    read_database = Mock(
        side_effect=[
            pl.DataFrame(
                {
                    "column_name": ["_id", "id", "exchange_id"],
                    "data_type": [":i32", ":i32", ":i32"],
                    "is_nullable": ["NO", "NO", "NO"],
                }
            ),
            pl.DataFrame({"table_name": ["__polars_hist_db_xtdb_table_configs"]}),
            pl.DataFrame(
                {
                    "primary_keys_json": ['["id"]'],
                    "id_policy": ["single-key"],
                    "columns_json": [columns_json],
                    "foreign_keys_json": [foreign_keys_json],
                }
            ),
        ]
    )
    monkeypatch.setattr(pl, "read_database", read_database)

    table_config = XtdbTableConfigOps(object()).from_table("test", "trading_pairs")

    assert list(table_config.primary_keys) == ["id"]
    assert [fk.name for fk in table_config.foreign_keys] == ["exchange_id"]
    fk = table_config.foreign_keys[0]
    assert fk.references.schema == "test"
    assert fk.references.table == "exchanges"
    assert fk.references.column == "id"


def test_xtdb_table_reflection_recovers_composite_primary_keys_from_metadata(
    monkeypatch,
):
    read_database = Mock(
        side_effect=[
            pl.DataFrame(
                {
                    "column_name": [
                        "_id",
                        "entity_id",
                        "record_id",
                        "destination",
                    ],
                    "data_type": [":utf8", ":i64", ":utf8", ":utf8"],
                    "is_nullable": ["NO", "NO", "NO", "YES"],
                }
            ),
            pl.DataFrame({"table_name": ["__polars_hist_db_xtdb_table_configs"]}),
            pl.DataFrame(
                {
                    "primary_keys_json": ['["entity_id","record_id"]'],
                    "id_policy": ["xtdb-pk-v1"],
                }
            ),
        ]
    )
    monkeypatch.setattr(pl, "read_database", read_database)

    table_config = XtdbTableConfigOps(object()).from_table("test", "records")

    assert table_config.schema == "test"
    assert table_config.name == "records"
    assert list(table_config.primary_keys) == ["entity_id", "record_id"]
    assert [
        (col.name, col.data_type, col.nullable) for col in table_config.columns
    ] == [
        ("_id", "VARCHAR(255)", False),
        ("entity_id", "BIGINT", False),
        ("record_id", "VARCHAR(255)", False),
        ("destination", "VARCHAR(255)", True),
    ]


def test_xtdb_table_reflection_errors_when_table_has_no_metadata(monkeypatch):
    monkeypatch.setattr(pl, "read_database", Mock(return_value=pl.DataFrame()))
    ops = XtdbTableConfigOps(object())

    with pytest.raises(
        ValueError, match="XTDB table metadata not found for test.records"
    ):
        ops.from_table("test", "records")


def test_xtdb_physical_schema_rejects_heterogeneous_scalar_union():
    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "seen_at", "DATETIME"),
        ],
    )
    metadata = pl.DataFrame(
        {
            "column_name": ["_id", "id", "seen_at"],
            "data_type": [
                ":i64",
                ":i64",
                '[:union #{:utf8 [:timestamp-tz :micro "UTC"] :null}]',
            ],
            "is_nullable": ["NO", "NO", "YES"],
        }
    )

    with pytest.raises(TypeError, match="physical schema"):
        _validate_xtdb_physical_types(metadata, table_config)


def test_xtdb_physical_schema_accepts_nullable_type_shorthand():
    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "label", "VARCHAR(255)"),
            TableColumnConfig("records", "seen_at", "DATETIME"),
        ],
    )
    metadata = pl.DataFrame(
        {
            "column_name": ["_id", "id", "label", "seen_at"],
            "data_type": [
                ":i64",
                ":i64",
                "[:? :UTF8]",
                '[:? [:timestamp-tz :micro "UTC"]]',
            ],
            "is_nullable": ["NO", "NO", "YES", "YES"],
        }
    )

    _validate_xtdb_physical_types(metadata, table_config)


def test_xtdb_physical_schema_validates_configured_columns_not_unrelated_columns():
    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "optional_note", "TEXT", nullable=True),
        ],
    )
    metadata = pl.DataFrame(
        {
            "column_name": ["_id", "id", "legacy_event_time"],
            "data_type": [":i64", ":i64", "[:? :instant]"],
        }
    )

    _validate_xtdb_physical_types(metadata, table_config)


def test_xtdb_physical_schema_rejects_missing_configured_column():
    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["id"],
        columns=[TableColumnConfig("records", "id", "BIGINT", nullable=False)],
    )

    with pytest.raises(TypeError, match="expected BIGINT, received MISSING"):
        _validate_xtdb_physical_types(
            pl.DataFrame({"column_name": ["other"], "data_type": [":utf8"]}),
            table_config,
        )


@pytest.mark.parametrize(
    ("data_type", "family"),
    [
        (":null", "NULL"),
        (":nothing", "NOTHING"),
        (":bool", "BOOLEAN"),
        (":i8", "INTEGER"),
        (":i16", "INTEGER"),
        (":i32", "INTEGER"),
        (":i64", "BIGINT"),
        (":f32", "FLOAT"),
        (":f64", "DOUBLE PRECISION"),
        (":utf8", "TEXT"),
        (":varbinary", "VARBINARY"),
        ("[:fixed-size-binary 16]", "FIXED-SIZE-BINARY"),
        ("[:decimal 15 3 128]", "DECIMAL"),
        (":instant", "TIMESTAMP WITH TIME ZONE"),
        ('[:timestamp-tz :micro "GMT"]', "TIMESTAMP WITH TIME ZONE"),
        ("[:timestamp-local :nano]", "TIMESTAMP"),
        ("[:date :day]", "DATE"),
        ("[:time-local :micro]", "TIME"),
        ("[:duration :nano]", "DURATION"),
        ("[:interval :month-day-nano]", "INTERVAL"),
        (":tstz-range", "TSTZ-RANGE"),
        (":keyword", "KEYWORD"),
        (":oid", "OID"),
        (":regclass", "REGCLASS"),
        (":regproc", "REGPROC"),
        (":uuid", "UUID"),
        (":uri", "URI"),
        (":transit", "TRANSIT"),
        ("[:list :utf8]", "LIST"),
        ("[:fixed-size-list 3 :utf8]", "FIXED-SIZE-LIST"),
        ("[:set :utf8]", "SET"),
        ("[:map {:sorted? false} [:struct {key :utf8 value :i64}]]", "MAP"),
        ("[:struct {name :utf8}]", "STRUCT"),
    ],
)
def test_xtdb_physical_type_family_covers_render_type_grammar(data_type, family):
    assert _xtdb_physical_type_family(data_type) == family


@pytest.mark.parametrize(
    ("data_type", "family"),
    [
        ("[:? :utf8]", "TEXT"),
        ("[:? :date :day]", "DATE"),
        ("[:? :decimal 15 3 128]", "DECIMAL"),
        ('[:? :timestamp-tz :micro "UTC"]', "TIMESTAMP WITH TIME ZONE"),
        ("#{:null :i8 :i32}", "INTEGER"),
        ('#{:null [:timestamp-tz :micro "UTC"]}', "TIMESTAMP WITH TIME ZONE"),
        ("#{:null [:list :utf8]}", "LIST"),
        ("#{:i64 :utf8}", "UNION"),
    ],
)
def test_xtdb_physical_type_family_handles_nullable_and_polymorphic_types(
    data_type, family
):
    assert _xtdb_physical_type_family(data_type) == family


@pytest.mark.parametrize(
    ("data_type", "config_type"),
    [
        ("[:? :date :day]", "DATE"),
        ("[:? :decimal 15 3 128]", "DECIMAL(15,3)"),
        (":instant", "TIMESTAMP WITH TIME ZONE"),
        ('[:? :timestamp-tz :micro "GMT"]', "TIMESTAMP WITH TIME ZONE"),
        ("[:timestamp-local :micro]", "TIMESTAMP"),
        ("[:time-local :micro]", "TIME"),
    ],
)
def test_xtdb_reflection_maps_parameterized_scalar_types(data_type, config_type):
    assert _xtdb_type_to_config_type(data_type) == config_type


def test_xtdb_declared_columns_quotes_non_identifier_column_names():
    from polars_hist_db.backends.xtdb import _xtdb_declared_columns

    table_config = TableConfig(
        schema="sample",
        name="__record_stage",
        primary_keys=["stage_run_id", "stage_row_index"],
        columns=[
            TableColumnConfig("__record_stage", "stage_run_id", "VARCHAR(128)"),
            TableColumnConfig("__record_stage", "stage_row_index", "BIGINT"),
            TableColumnConfig("__record_stage", "Entity", "VARCHAR(64)"),
            TableColumnConfig("__record_stage", "External Ref (entity)", "VARCHAR(16)"),
            TableColumnConfig("__record_stage", "timestamp", "DATETIME"),
        ],
    )

    assert _xtdb_declared_columns(table_config) == [
        "_id",
        "stage_run_id",
        "stage_row_index",
        "entity",
        '"external ref (entity)"',
        '"timestamp"',
    ]


def test_xtdb_temporal_upsert_pins_valid_from_to_epoch_for_non_temporal_tables():
    """Reference tables (is_temporal=False) must be readable as-of any past
    timestamp — otherwise every asof-mode query joining a reference table
    silently returns zero rows. Pin _valid_from to the unix epoch on write."""
    backend = XtdbBackend()
    ops = Mock()
    ops.table_insert.return_value = 1
    table_config = TableConfig(
        schema="reference",
        name="entity_info",
        primary_keys=["entity_number"],
        is_temporal=False,
        columns=[
            TableColumnConfig("entity_info", "entity_number", "BIGINT", nullable=False),
            TableColumnConfig("entity_info", "status", "VARCHAR(16)"),
        ],
    )

    backend.temporal_upsert(
        pl.DataFrame({"entity_number": [1234], "status": ["ACTIVE"]}),
        "reference",
        "entity_info",
        dataframe_ops=ops,
        table_config=table_config,
    )

    written_df = ops.table_insert.call_args.args[0]
    assert "_valid_from" in written_df.columns
    assert written_df.get_column("_valid_from").to_list() == [_NON_TEMPORAL_VALID_FROM]


def test_xtdb_temporal_upsert_keeps_temporal_tables_off_epoch():
    """Bitemporal fact tables (is_temporal=True) must NOT get the epoch
    sentinel — their _valid_from comes from the transaction system_time or an
    explicit valid_time mapping. Injecting epoch would collapse history."""
    backend = XtdbBackend()
    ops = Mock()
    ops.table_insert.return_value = 1
    update_time = datetime(2026, 1, 1, tzinfo=UTC)
    table_config = TableConfig(
        schema="fact",
        name="records",
        primary_keys=["record_id"],
        is_temporal=True,
        columns=[
            TableColumnConfig("records", "record_id", "BIGINT", nullable=False),
        ],
    )

    backend.temporal_upsert(
        pl.DataFrame({"record_id": [1]}),
        "fact",
        "records",
        dataframe_ops=ops,
        table_config=table_config,
        update_time=update_time,
    )

    written_df = ops.table_insert.call_args.args[0]
    assert "_valid_from" not in written_df.columns


def test_xtdb_temporal_upsert_respects_configured_valid_time_over_epoch():
    """When the config provides a valid_time mapping, use the mapped column
    rather than the epoch sentinel — even on a non-temporal table."""
    backend = XtdbBackend()
    ops = Mock()
    ops.table_insert.return_value = 1
    mapped_from = datetime(2030, 6, 1, tzinfo=UTC)
    table_config = TableConfig(
        schema="reference",
        name="rates",
        primary_keys=["code"],
        is_temporal=False,
        columns=[
            TableColumnConfig("rates", "code", "VARCHAR(3)", nullable=False),
            TableColumnConfig("rates", "value", "DOUBLE"),
            TableColumnConfig("rates", "effective_from", "DATETIME"),
        ],
    )

    backend.temporal_upsert(
        pl.DataFrame(
            {
                "code": ["USD"],
                "value": [1.0],
                "effective_from": [mapped_from],
            }
        ),
        "reference",
        "rates",
        dataframe_ops=ops,
        table_config=table_config,
        valid_time=ValidTimeConfig(table="rates", from_column="effective_from"),
    )

    written_df = ops.table_insert.call_args.args[0]
    assert written_df.get_column("_valid_from").to_list() == [mapped_from]


def test_xtdb_temporal_upsert_preserves_user_provided_valid_from():
    """If the caller already put _valid_from in the dataframe, honour it and
    do not overwrite with the epoch sentinel."""
    backend = XtdbBackend()
    ops = Mock()
    ops.table_insert.return_value = 1
    explicit_from = datetime(2028, 1, 1, tzinfo=UTC)
    table_config = TableConfig(
        schema="reference",
        name="rates",
        primary_keys=["code"],
        is_temporal=False,
        columns=[
            TableColumnConfig("rates", "code", "VARCHAR(3)", nullable=False),
        ],
    )

    backend.temporal_upsert(
        pl.DataFrame({"code": ["USD"], "_valid_from": [explicit_from]}),
        "reference",
        "rates",
        dataframe_ops=ops,
        table_config=table_config,
    )

    written_df = ops.table_insert.call_args.args[0]
    assert written_df.get_column("_valid_from").to_list() == [explicit_from]


def test_xtdb_temporal_upsert_falls_back_to_pgwire_when_adbc_ingest_unavailable(
    monkeypatch,
):
    """XTDB Flight SQL does not implement ExecuteIngest; the ADBC bulk-write
    surface raises with 'Not implemented' / 'ExecuteIngest' in the message.
    temporal_upsert must fall back to the pgwire path rather than bubbling
    the error and stalling the ingest pipeline."""
    backend = XtdbBackend()

    class NotSupportedError(Exception):
        pass

    adbc_ops = XtdbAdbcDataframeOps.__new__(XtdbAdbcDataframeOps)
    adbc_ops.table_insert = Mock(  # type: ignore[method-assign]
        side_effect=NotSupportedError(
            "NOT_IMPLEMENTED: [FlightSQL] Not implemented. "
            "(Unimplemented; ExecuteIngest)"
        )
    )

    pgwire_ops = Mock()
    pgwire_ops.table_insert.return_value = 7
    monkeypatch.setattr(
        XtdbBackend, "dataframes", lambda self, conn: pgwire_ops, raising=True
    )

    table_config = TableConfig(
        schema="reference",
        name="entity_info",
        primary_keys=["entity_number"],
        is_temporal=False,
        columns=[
            TableColumnConfig("entity_info", "entity_number", "BIGINT", nullable=False),
        ],
    )

    result = backend.temporal_upsert(
        pl.DataFrame({"entity_number": [1, 2]}),
        "reference",
        "entity_info",
        connection=Mock(),
        dataframe_ops=adbc_ops,
        table_config=table_config,
    )

    assert result == 7
    adbc_ops.table_insert.assert_called_once()  # type: ignore[attr-defined]
    pgwire_ops.table_insert.assert_called_once()
