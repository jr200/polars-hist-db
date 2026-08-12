import builtins
from datetime import UTC, date, datetime, time
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import polars as pl
import pyarrow as pa
import pytest

from polars_hist_db.backends.xtdb import (
    XtdbDataframeOps,
    XtdbTableConfigOps,
    _execute_xtdb_arrow_copy,
    _execute_xtdb_dml,
)
from polars_hist_db.config import TableColumnConfig, TableConfig
from polars_hist_db.core import TimeHint
from polars_hist_db.types import TypeContractError


def _single_executemany_call(driver_connection: Mock):
    return driver_connection.cursor.return_value.executemany.call_args


def test_xtdb_dataframe_ops_reads_raw_sql_with_schema_overrides(monkeypatch):
    expected_df = pl.DataFrame({"id": [1]})
    read_database = Mock(return_value=expected_df)
    monkeypatch.setattr(pl, "read_database", read_database)

    connection = object()
    ops = XtdbDataframeOps(connection)

    result = ops.from_raw_sql("select * from test.records", {"id": pl.Int64})

    assert result is expected_df
    read_database.assert_called_once_with(
        "select * from test.records",
        connection,
        schema_overrides={"id": pl.Int64},
    )


def test_xtdb_dml_retries_with_append_time_when_system_time_is_too_old():
    class _DriverConnection:
        def __init__(self):
            self.executed = []
            self.rollback_count = 0
            self.commit_count = 0

        def execute(self, sql):
            self.executed.append(sql)
            if sql == "COMMIT":
                self.commit_count += 1
                if self.commit_count == 1:
                    raise RuntimeError(
                        "invalid-system-time: specified system-time older"
                    )

        def commit(self):
            self.commit_count += 1

        def rollback(self):
            self.rollback_count += 1

    class _Connection:
        def __init__(self):
            self.connection = SimpleNamespace(driver_connection=_DriverConnection())
            self.info = {}

        def in_transaction(self):
            return False

    connection = _Connection()

    row_count = _execute_xtdb_dml(
        connection,
        "INSERT INTO test.records (_id) VALUES (1)",
        system_time=datetime(2025, 1, 1, tzinfo=UTC),
    )

    assert row_count == 0
    assert connection.connection.driver_connection.rollback_count == 1
    assert connection.connection.driver_connection.executed == [
        (
            "BEGIN READ WRITE WITH (SYSTEM_TIME = TIMESTAMP WITH TIME ZONE "
            "'2025-01-01T00:00:00+00:00')"
        ),
        "INSERT INTO test.records (_id) VALUES (1)",
        "COMMIT",
        "ROLLBACK",
        "BEGIN READ WRITE",
        "INSERT INTO test.records (_id) VALUES (1)",
        "COMMIT",
    ]


def test_xtdb_dml_uses_append_time_without_submitting_known_old_system_time():
    driver_connection = Mock()
    connection = Mock()
    connection.info = {}
    connection.connection.driver_connection = driver_connection
    connection.in_transaction.return_value = False
    connection.execute.return_value.scalar_one_or_none.return_value = datetime(
        2026, 1, 1, tzinfo=UTC
    )

    _execute_xtdb_dml(
        connection,
        "INSERT INTO test.records (_id) VALUES (1)",
        system_time=datetime(2025, 1, 1, tzinfo=UTC),
    )

    assert "FROM xt.txs" in str(connection.execute.call_args.args[0])
    assert [call.args[0] for call in driver_connection.execute.call_args_list] == [
        "BEGIN READ WRITE",
        "INSERT INTO test.records (_id) VALUES (1)",
        "COMMIT",
    ]


def test_xtdb_dataframe_ops_reads_table_as_sql():
    connection = object()
    ops = XtdbDataframeOps(connection)
    ops.from_raw_sql = Mock(return_value=pl.DataFrame({"id": [1]}))

    result = ops.from_table("test", "records", {"id": pl.Int64})

    assert result.to_dict(as_series=False) == {"id": [1]}
    ops.from_raw_sql.assert_called_once_with(
        "SELECT * FROM test.records",
        {"id": pl.Int64},
    )


def test_xtdb_dataframe_ops_applies_table_time_hint():
    connection = object()
    ops = XtdbDataframeOps(connection)
    ops.from_raw_sql = Mock(return_value=pl.DataFrame({"id": [1]}))

    result = ops.from_table(
        "test",
        "records",
        {"id": pl.Int64},
        time_hint=TimeHint(
            mode="asof",
            asof_utc=datetime.fromisoformat("2026-01-02T03:04:05+00:00"),
        ),
    )

    assert result.to_dict(as_series=False) == {"id": [1]}
    ops.from_raw_sql.assert_called_once_with(
        "SELECT * FROM test.records FOR SYSTEM_TIME AS OF '2026-01-02T03:04:05+00:00'",
        {"id": pl.Int64},
    )


def test_xtdb_dataframe_ops_reads_table_with_configured_schema_overrides(monkeypatch):
    read_database = Mock(return_value=pl.DataFrame())
    monkeypatch.setattr(pl, "read_database", read_database)
    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination_date", "DATETIME"),
            TableColumnConfig("records", "amount_value", "DOUBLE"),
        ],
    )
    monkeypatch.setattr(
        XtdbTableConfigOps,
        "from_table",
        lambda self, table_schema, table_name: table_config,
    )
    connection = object()
    ops = XtdbDataframeOps(connection)

    ops.from_table("test", "records")

    read_database.assert_called_once_with(
        "SELECT * FROM test.records",
        connection,
        schema_overrides={
            "_id": pl.Int64,
            "id": pl.Int64,
            "destination_date": pl.Datetime("us", "UTC"),
            "amount_value": pl.Float64,
            "_valid_from": pl.Datetime("us", "UTC"),
            "_valid_to": pl.Datetime("us", "UTC"),
        },
    )


def test_xtdb_dataframe_ops_restores_logical_columns_with_slashes(monkeypatch):
    read_database = Mock(
        return_value=pl.DataFrame(
            {
                "_id": [1],
                "id": [1],
                "capacity_bcm": ["12.345"],
            }
        )
    )
    monkeypatch.setattr(pl, "read_database", read_database)
    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "capacity/bcm", "DECIMAL(15,3)"),
        ],
    )
    monkeypatch.setattr(
        XtdbTableConfigOps,
        "from_table",
        lambda self, table_schema, table_name: table_config,
    )
    connection = object()
    ops = XtdbDataframeOps(connection)

    result = ops.from_table("test", "records")

    assert result.columns == ["_id", "id", "capacity/bcm"]
    read_database.assert_called_once_with(
        "SELECT * FROM test.records",
        connection,
        schema_overrides={
            "_id": pl.Int64,
            "id": pl.Int64,
            "capacity_bcm": pl.Decimal(15, 3),
            "_valid_from": pl.Datetime("us", "UTC"),
            "_valid_to": pl.Datetime("us", "UTC"),
        },
    )


def test_xtdb_dataframe_ops_writes_dataframe_via_polars_write_database(monkeypatch):
    captured = {}

    def write_database(self, **kwargs):
        captured["df"] = self
        captured["kwargs"] = kwargs
        return 2

    monkeypatch.setattr(pl.DataFrame, "write_database", write_database)

    df = pl.DataFrame({"_id": [1, 2], "destination": ["Alpha", "Beta"]})
    connection = object()
    ops = XtdbDataframeOps(connection)

    result = ops.table_insert(df, "test", "records")

    assert result == 2
    assert captured["df"].to_dict(as_series=False) == {
        "_id": [1, 2],
        "destination": ["Alpha", "Beta"],
    }
    assert captured["kwargs"] == {
        "table_name": "test.records",
        "connection": connection,
        "if_table_exists": "append",
    }


def test_xtdb_dataframe_ops_maps_configured_primary_key_to_id(monkeypatch):
    captured = {}

    def write_database(self, **kwargs):
        captured["df"] = self
        captured["kwargs"] = kwargs
        return 2

    monkeypatch.setattr(pl.DataFrame, "write_database", write_database)

    df = pl.DataFrame({"id": [1, 2], "destination": ["A", "B"]})
    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
        ],
    )
    connection = object()
    ops = XtdbDataframeOps(connection)

    result = ops.table_insert(df, "test", "records", table_config=table_config)

    assert result == 2
    written_df = captured["df"]
    assert written_df.to_dict(as_series=False) == {
        "_id": [1, 2],
        "id": [1, 2],
        "destination": ["A", "B"],
    }
    assert captured["kwargs"] == {
        "table_name": "test.records",
        "connection": connection,
        "if_table_exists": "append",
    }


def test_xtdb_dataframe_ops_maps_composite_primary_key_to_synthetic_id(monkeypatch):
    captured = {}

    def write_database(self, **kwargs):
        captured["df"] = self
        captured["kwargs"] = kwargs
        return 2

    monkeypatch.setattr(pl.DataFrame, "write_database", write_database)

    df = pl.DataFrame(
        {
            "entity_id": [10, 10],
            "record_id": ["A", "B"],
            "destination": ["Alpha", "Beta"],
        }
    )
    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["entity_id", "record_id"],
        columns=[
            TableColumnConfig("records", "entity_id", "BIGINT", nullable=False),
            TableColumnConfig("records", "record_id", "VARCHAR(255)", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
        ],
    )
    connection = object()
    ops = XtdbDataframeOps(connection)

    result = ops.table_insert(df, "test", "records", table_config=table_config)

    assert result == 2
    written_df = captured["df"]
    assert written_df.to_dict(as_series=False) == {
        "_id": [
            'xtdb-pk-v1:[["entity_id",10],["record_id","A"]]',
            'xtdb-pk-v1:[["entity_id",10],["record_id","B"]]',
        ],
        "entity_id": [10, 10],
        "record_id": ["A", "B"],
        "destination": ["Alpha", "Beta"],
    }
    assert captured["kwargs"] == {
        "table_name": "test.records",
        "connection": connection,
        "if_table_exists": "append",
    }


def test_xtdb_dataframe_ops_uses_system_time_transaction_for_update_time():
    driver_connection = Mock()
    connection = Mock()
    connection.connection.driver_connection = driver_connection
    connection.in_transaction.return_value = False
    ops = XtdbDataframeOps(connection)

    result = ops.table_insert(
        pl.DataFrame({"_id": [1], "destination": ["Alpha"]}),
        "test",
        "records",
        update_time=datetime(2030, 1, 1, 12, 0, tzinfo=UTC),
    )

    assert result == 1
    assert driver_connection.execute.call_args_list[0].args == (
        (
            "BEGIN READ WRITE WITH (SYSTEM_TIME = TIMESTAMP WITH TIME ZONE "
            "'2030-01-01T12:00:00+00:00')"
        ),
    )


def test_xtdb_dataframe_ops_splits_pgwire_insert_by_max_rows():
    driver_connection = MagicMock()
    cursor = driver_connection.cursor.return_value
    connection = Mock()
    connection.connection.driver_connection = driver_connection
    connection.in_transaction.return_value = False
    ops = XtdbDataframeOps(connection, max_rows_per_insert=2)

    result = ops.table_insert(
        pl.DataFrame(
            {"_id": [1, 2, 3, 4, 5], "destination": ["A", "B", "C", "D", "E"]}
        ),
        "test",
        "records",
    )

    assert result == 5
    assert cursor.copy.call_count == 2
    assert cursor.executemany.call_args.args[1] == [(5, "E")]


def test_xtdb_dataframe_ops_binds_table_query_keys_once(monkeypatch):
    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["id"],
        columns=[TableColumnConfig("records", "id", "BIGINT", nullable=False)],
    )
    monkeypatch.setattr(
        XtdbTableConfigOps,
        "from_table",
        lambda self, table_schema, table_name: table_config,
    )
    ops = XtdbDataframeOps(object(), max_rows_per_insert=2)
    ops.from_raw_sql = Mock(return_value=pl.DataFrame({"id": [1, 2, 3, 4, 5]}))

    result = ops.table_query(
        "test",
        "records",
        pl.DataFrame({"id": [1, 2, 3, 4, 5]}),
        ["id"],
    )

    assert result.to_dict(as_series=False) == {"id": [1, 2, 3, 4, 5]}
    ops.from_raw_sql.assert_called_once()
    query, _, execute_options = ops.from_raw_sql.call_args.args
    assert "JOIN UNNEST(%s) AS q(key)" in query
    assert "t._id = CAST((q.key).id AS BIGINT)" in query
    assert execute_options["parameters"][0].obj == [
        {"id": 1},
        {"id": 2},
        {"id": 3},
        {"id": 4},
        {"id": 5},
    ]


def test_xtdb_dataframe_ops_preserves_bound_key_types():
    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["day", "seen_at", "amount", "raw"],
        columns=[
            TableColumnConfig("records", "day", "DATE", nullable=False),
            TableColumnConfig("records", "seen_at", "TIMESTAMPTZ", nullable=False),
            TableColumnConfig("records", "amount", "DECIMAL(10,2)", nullable=False),
            TableColumnConfig("records", "raw", "VARBINARY", nullable=False),
        ],
    )
    ops = XtdbDataframeOps(object())
    ops.from_raw_sql = Mock(return_value=pl.DataFrame(schema={"day": pl.Date}))

    ops.table_query(
        "test",
        "records",
        pl.DataFrame(
            {
                "day": [date(2026, 1, 2)],
                "seen_at": [datetime(2026, 1, 2, 3, 4, tzinfo=UTC)],
                "amount": [Decimal("1.20")],
                "raw": [b"\x00\xff"],
            },
            schema_overrides={"amount": pl.Decimal(10, 2)},
        ),
        ["day"],
        table_config=table_config,
    )

    query, _, execute_options = ops.from_raw_sql.call_args.args
    assert "CAST((q.key).day AS DATE)" in query
    assert "CAST((q.key).seen_at AS TIMESTAMP WITH TIME ZONE)" in query
    assert "CAST((q.key).amount AS DECIMAL(10,2))" in query
    assert "CAST((q.key).raw AS VARBINARY)" in query
    assert execute_options["parameters"][0].obj == [
        {
            "day": "2026-01-02",
            "seen_at": "2026-01-02T03:04:00+00:00",
            "amount": "1.20",
            "raw": "00ff",
        }
    ]


def test_xtdb_arrow_copy_writes_one_arrow_stream_transaction():
    driver_connection = MagicMock()
    connection = Mock()
    connection.connection.driver_connection = driver_connection
    connection.in_transaction.return_value = False

    _execute_xtdb_arrow_copy(
        connection,
        "test.records",
        pl.DataFrame({"_id": [1, 2, 3], "destination": ["A", "B", "C"]}),
    )

    assert [call.args[0] for call in driver_connection.execute.call_args_list] == [
        "BEGIN READ WRITE",
        "COMMIT",
    ]
    driver_connection.cursor.return_value.copy.assert_called_once_with(
        "COPY test.records FROM STDIN WITH (FORMAT 'arrow-stream')"
    )
    payload = driver_connection.cursor.return_value.copy.return_value.__enter__.return_value.write.call_args.args[
        0
    ]
    table = pa.ipc.open_stream(payload).read_all()
    assert table.to_pydict() == {
        "_id": [1, 2, 3],
        "destination": ["A", "B", "C"],
    }


def test_xtdb_dml_advances_reused_system_time_on_same_connection():
    from polars_hist_db.backends.xtdb import _execute_xtdb_dml

    class _DriverConnection:
        def __init__(self):
            self.statements = []

        def execute(self, sql):
            self.statements.append(sql)

        def commit(self):
            self.statements.append("COMMIT")

        def rollback(self):
            self.statements.append("ROLLBACK")

    class _Connection:
        def __init__(self):
            self.connection = SimpleNamespace(driver_connection=_DriverConnection())
            self.info = {}

        def in_transaction(self):
            return False

    connection = _Connection()
    system_time = datetime(2030, 1, 1, 12, 0, tzinfo=UTC)

    _execute_xtdb_dml(
        connection, "CREATE TABLE test.one (_id)", system_time=system_time
    )
    _execute_xtdb_dml(
        connection, "CREATE TABLE test.two (_id)", system_time=system_time
    )

    begin_statements = [
        statement
        for statement in connection.connection.driver_connection.statements
        if statement.startswith("BEGIN READ WRITE")
    ]
    assert begin_statements == [
        (
            "BEGIN READ WRITE WITH (SYSTEM_TIME = TIMESTAMP WITH TIME ZONE "
            "'2030-01-01T12:00:00+00:00')"
        ),
        (
            "BEGIN READ WRITE WITH "
            "(SYSTEM_TIME = TIMESTAMP WITH TIME ZONE "
            "'2030-01-01T12:00:00.000001+00:00')"
        ),
    ]


def test_xtdb_dml_registers_text_dumper_for_parameterized_rows():
    string_types = pytest.importorskip("psycopg.types.string")

    driver_connection = Mock()
    connection = Mock()
    connection.connection.driver_connection = driver_connection
    connection.in_transaction.return_value = False

    _execute_xtdb_dml(
        connection,
        "INSERT INTO test.records (_id, destination) VALUES (%s::BIGINT, %s::TEXT)",
        [(1, "Alpha")],
    )

    driver_connection.adapters.register_dumper.assert_any_call(
        str, string_types.StrDumper
    )


def test_xtdb_dml_skips_text_dumper_when_psycopg_is_unavailable(monkeypatch):
    original_import = builtins.__import__

    def import_without_psycopg(name, *args, **kwargs):
        if name.startswith("psycopg"):
            raise ModuleNotFoundError("No module named 'psycopg'")
        return original_import(name, *args, **kwargs)

    driver_connection = Mock()
    connection = Mock()
    connection.connection.driver_connection = driver_connection
    connection.in_transaction.return_value = False
    monkeypatch.setattr(builtins, "__import__", import_without_psycopg)

    _execute_xtdb_dml(
        connection,
        "INSERT INTO test.records (_id, destination) VALUES (%s::BIGINT, %s::TEXT)",
        [(1, "Alpha")],
    )

    driver_connection.adapters.register_dumper.assert_not_called()


def test_xtdb_dataframe_ops_preserves_bound_timestamp_parameters_as_utc_instants():
    driver_connection = Mock()
    connection = Mock()
    connection.connection.driver_connection = driver_connection
    connection.in_transaction.return_value = False
    ops = XtdbDataframeOps(connection)
    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "seen_at", "DATETIME"),
        ],
    )

    ops.table_insert(
        pl.DataFrame(
            {
                "id": [1],
                "seen_at": [datetime(2030, 1, 1, 12, 30, tzinfo=UTC)],
            }
        ),
        "test",
        "records",
        table_config=table_config,
    )

    insert_call = _single_executemany_call(driver_connection)
    assert "%s::TIMESTAMP WITH TIME ZONE" in insert_call.args[0]
    assert insert_call.args[1] == [(1, 1, datetime(2030, 1, 1, 12, 30, tzinfo=UTC))]


def test_xtdb_dataframe_ops_casts_datetime_precision_to_timestamp_with_timezone():
    driver_connection = Mock()
    connection = Mock()
    connection.connection.driver_connection = driver_connection
    connection.in_transaction.return_value = False
    ops = XtdbDataframeOps(connection)
    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "seen_at", "DATETIME(6)"),
        ],
    )

    ops.table_insert(
        pl.DataFrame(
            {
                "id": [1],
                "seen_at": [datetime(2030, 1, 1, 12, 30, tzinfo=UTC)],
            }
        ),
        "test",
        "records",
        table_config=table_config,
    )

    insert_call = _single_executemany_call(driver_connection)
    assert "%s::TIMESTAMP WITH TIME ZONE" in insert_call.args[0]
    assert insert_call.args[1] == [(1, 1, datetime(2030, 1, 1, 12, 30, tzinfo=UTC))]


def test_xtdb_dataframe_ops_uses_native_casts_for_mysql_compatibility_types():
    driver_connection = Mock()
    connection = Mock()
    connection.connection.driver_connection = driver_connection
    connection.in_transaction.return_value = False
    ops = XtdbDataframeOps(connection)
    table_config = TableConfig(
        schema="test",
        name="compat_types",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("compat_types", "id", "INT", nullable=False),
            TableColumnConfig("compat_types", "bool_col", "BOOL"),
            TableColumnConfig("compat_types", "bit_col", "BIT"),
            TableColumnConfig("compat_types", "tinyint_col", "TINYINT"),
            TableColumnConfig("compat_types", "smallint_col", "SMALLINT"),
            TableColumnConfig("compat_types", "mediumint_col", "MEDIUMINT"),
            TableColumnConfig("compat_types", "real_col", "REAL"),
            TableColumnConfig("compat_types", "datetime_col", "DATETIME"),
            TableColumnConfig("compat_types", "time_col", "TIME"),
        ],
    )

    ops.table_insert(
        pl.DataFrame(
            {
                "id": [1],
                "bool_col": [True],
                "bit_col": [1],
                "tinyint_col": [2],
                "smallint_col": [738221],
                "mediumint_col": [4],
                "real_col": [78.9],
                "datetime_col": [datetime.fromisoformat("2030-01-01T12:00:00")],
                "time_col": [time(12, 30)],
            }
        ),
        "test",
        "compat_types",
        table_config=table_config,
        force_type_coercion=True,
    )

    insert_call = _single_executemany_call(driver_connection)
    insert_sql = insert_call.args[0]
    assert "INSERT INTO test.compat_types" in insert_sql
    assert "%s::BOOLEAN" in insert_sql
    assert insert_sql.count("%s::INTEGER") == 6
    assert "%s::DOUBLE PRECISION" in insert_sql
    assert "%s::TIMESTAMP WITH TIME ZONE" in insert_sql
    assert "%s::TIME" in insert_sql
    assert insert_call.args[1] == [
        (
            1,
            1,
            True,
            1,
            2,
            738221,
            4,
            78.9,
            datetime(2030, 1, 1, 12, 0, tzinfo=UTC),
            time(12, 30),
        )
    ]


def test_xtdb_dataframe_ops_casts_null_values_to_configured_types():
    driver_connection = Mock()
    connection = Mock()
    connection.connection.driver_connection = driver_connection
    connection.in_transaction.return_value = False
    ops = XtdbDataframeOps(connection)
    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination_date", "DATETIME"),
            TableColumnConfig("records", "amount_value", "DOUBLE"),
        ],
    )

    ops.table_insert(
        pl.DataFrame(
            {
                "id": [1],
                "destination_date": [None],
                "amount_value": [None],
            }
        ),
        "test",
        "records",
        table_config=table_config,
    )

    insert_call = _single_executemany_call(driver_connection)
    insert_sql = insert_call.args[0]
    assert "%s::TIMESTAMP WITH TIME ZONE" in insert_sql
    assert "%s::DOUBLE PRECISION" in insert_sql
    assert insert_call.args[1] == [(1, 1, None, None)]


def test_xtdb_dataframe_ops_casts_categorical_values_to_configured_decimal():
    driver_connection = Mock()
    connection = Mock()
    connection.connection.driver_connection = driver_connection
    connection.in_transaction.return_value = False
    ops = XtdbDataframeOps(connection)
    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "capacity", "DECIMAL(15,3)"),
        ],
    )

    ops.table_insert(
        pl.DataFrame(
            {
                "id": [1],
                "capacity": ["12.345"],
            },
            schema_overrides={"capacity": pl.Categorical},
        ),
        "test",
        "records",
        table_config=table_config,
        force_type_coercion=True,
    )

    insert_call = _single_executemany_call(driver_connection)
    insert_sql = insert_call.args[0]
    assert "%s::DECIMAL(15,3)" in insert_sql
    assert insert_call.args[1] == [(1, 1, Decimal("12.345"))]


def test_xtdb_dataframe_ops_quotes_reserved_insert_columns():
    driver_connection = Mock()
    connection = Mock()
    connection.connection.driver_connection = driver_connection
    connection.in_transaction.return_value = False
    ops = XtdbDataframeOps(connection)
    table_config = TableConfig(
        schema="source_a",
        name="entity_info",
        primary_keys=["entity_id"],
        columns=[
            TableColumnConfig("entity_info", "entity_id", "INT", nullable=False),
            TableColumnConfig("entity_info", "name", "VARCHAR(64)"),
            TableColumnConfig("entity_info", "flag", "VARCHAR(64)"),
        ],
    )

    ops.table_insert(
        pl.DataFrame(
            {
                "entity_id": [311038700],
                "name": ["ALPHA"],
                "flag": ["BS"],
            }
        ),
        "source_a",
        "entity_info",
        table_config=table_config,
        force_type_coercion=True,
    )

    insert_sql = _single_executemany_call(driver_connection).args[0]
    assert (
        'INSERT INTO source_a.entity_info (_id, entity_id, name, "flag")' in insert_sql
    )


def test_xtdb_dataframe_ops_encodes_insert_columns_with_slashes():
    driver_connection = Mock()
    connection = Mock()
    connection.connection.driver_connection = driver_connection
    connection.in_transaction.return_value = False
    ops = XtdbDataframeOps(connection)
    table_config = TableConfig(
        schema="source_a",
        name="entity_info",
        primary_keys=["entity_id"],
        columns=[
            TableColumnConfig("entity_info", "entity_id", "INT", nullable=False),
            TableColumnConfig("entity_info", "capacity/bcm", "DECIMAL(15,3)"),
        ],
    )

    ops.table_insert(
        pl.DataFrame(
            {
                "entity_id": [1],
                "capacity/bcm": ["4.080"],
            }
        ),
        "source_a",
        "entity_info",
        table_config=table_config,
        force_type_coercion=True,
    )

    insert_call = _single_executemany_call(driver_connection)
    insert_sql = insert_call.args[0]
    assert '"capacity/bcm"' not in insert_sql
    assert "entity_id" in insert_sql
    assert "capacity_bcm" in insert_sql
    assert "%s::DECIMAL(15,3)" in insert_sql
    assert insert_call.args[1] == [(1, 1, Decimal("4.080"))]


def test_xtdb_dataframe_ops_uses_underscore_column_mapping():
    driver_connection = Mock()
    connection = Mock()
    connection.connection.driver_connection = driver_connection
    connection.in_transaction.return_value = False
    ops = XtdbDataframeOps(connection)
    table_config = TableConfig(
        schema="source_a",
        name="entity_info",
        primary_keys=["entity_id"],
        columns=[
            TableColumnConfig("entity_info", "entity_id", "INT", nullable=False),
            TableColumnConfig(
                "entity_info",
                "metrics.properties.Capacity/Value",
                "DECIMAL(15,3)",
            ),
        ],
    )

    ops.table_insert(
        pl.DataFrame(
            {
                "entity_id": [1],
                "metrics.properties.Capacity/Value": ["4.080"],
            }
        ),
        "source_a",
        "entity_info",
        table_config=table_config,
        force_type_coercion=True,
    )

    insert_sql = _single_executemany_call(driver_connection).args[0]
    assert '"metrics.properties.Capacity/Value"' not in insert_sql
    assert "metrics_properties_capacity_value" in insert_sql


def test_xtdb_dataframe_ops_rejects_physical_column_mapping_collisions():
    connection = Mock()
    connection.connection.driver_connection = Mock()
    connection.in_transaction.return_value = False
    ops = XtdbDataframeOps(connection)
    table_config = TableConfig(
        schema="source_a",
        name="entity_info",
        primary_keys=["entity_id"],
        columns=[
            TableColumnConfig("entity_info", "entity_id", "INT", nullable=False),
            TableColumnConfig("entity_info", "Metric.Value", "VARCHAR(64)"),
            TableColumnConfig("entity_info", "metric/value", "VARCHAR(64)"),
        ],
    )

    with pytest.raises(ValueError, match="physical column name collision"):
        ops.table_insert(
            pl.DataFrame(
                {
                    "entity_id": [1],
                    "Metric.Value": ["A"],
                    "metric/value": ["B"],
                }
            ),
            "source_a",
            "entity_info",
            table_config=table_config,
        )


def test_xtdb_dataframe_ops_rejects_implicit_type_coercion():
    connection = Mock()
    connection.connection.driver_connection = Mock()
    ops = XtdbDataframeOps(connection)
    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "seen_at", "DATETIME"),
        ],
    )

    with pytest.raises(TypeContractError, match="force_type_coercion=True"):
        ops.table_insert(
            pl.DataFrame({"id": [1], "seen_at": ["2030-01-01T00:00:00Z"]}),
            "test",
            "records",
            table_config=table_config,
        )


def test_xtdb_dataframe_ops_rejects_unknown_unconfigured_type():
    connection = Mock()
    connection.connection.driver_connection = Mock()
    ops = XtdbDataframeOps(connection)

    with pytest.raises(ValueError, match="Unsupported XTDB Polars type"):
        ops.table_insert(pl.DataFrame({"values": [[1, 2]]}), "test", "records")


def test_xtdb_dataframe_ops_forced_conversion_never_replaces_bad_values_with_null():
    connection = Mock()
    connection.connection.driver_connection = Mock()
    ops = XtdbDataframeOps(connection)
    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "amount", "DOUBLE"),
        ],
    )

    with pytest.raises(pl.exceptions.InvalidOperationError):
        ops.table_insert(
            pl.DataFrame({"id": [1], "amount": ["not-a-number"]}),
            "test",
            "records",
            table_config=table_config,
            force_type_coercion=True,
        )


def test_xtdb_backend_returns_xtdb_dataframe_ops():
    from polars_hist_db.backends import XtdbBackend

    connection = object()
    ops = XtdbBackend().dataframes(connection)

    assert isinstance(ops, XtdbDataframeOps)
