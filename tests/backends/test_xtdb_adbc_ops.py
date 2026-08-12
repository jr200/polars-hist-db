from unittest.mock import Mock

import polars as pl
import pyarrow as pa

from polars_hist_db.backends import DbEngineConfig, XtdbBackend
from polars_hist_db.backends.xtdb import XtdbAdbcDataframeOps
from polars_hist_db.config import TableColumnConfig, TableConfig


class _FakeCursor:
    def __init__(self):
        self.ingests = []
        self.executed = []
        self.arrow_result = pa.table({"_id": [1], "destination": ["Alpha"]})

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def adbc_ingest(self, table_name, arrow_table, mode, **kwargs):
        self.ingests.append((table_name, arrow_table, mode, kwargs))

    def execute(self, query, **options):
        self.executed.append((query, options))

    def fetch_arrow_table(self):
        return self.arrow_result


class _FakeAdbcConnection:
    def __init__(self):
        self.cursor_instance = _FakeCursor()

    def cursor(self):
        return self.cursor_instance


def _record_config(schema="public"):
    return TableConfig(
        schema=schema,
        name="records",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
        ],
    )


def test_xtdb_adbc_dataframe_ops_ingests_arrow_with_id_mapping():
    connection = _FakeAdbcConnection()
    ops = XtdbAdbcDataframeOps(connection)

    result = ops.table_insert(
        pl.DataFrame({"id": [1, 2], "destination": ["Alpha", "Beta"]}),
        "public",
        "records",
        table_config=_record_config(),
    )

    assert result == 2
    table_name, arrow_table, mode, kwargs = connection.cursor_instance.ingests[0]
    assert table_name == "records"
    assert mode == "create_append"
    assert kwargs == {"db_schema_name": "public"}
    assert arrow_table.schema.field("destination").type == pa.string()
    assert arrow_table.to_pydict() == {
        "_id": [1, 2],
        "id": [1, 2],
        "destination": ["Alpha", "Beta"],
    }


def test_xtdb_adbc_dataframe_ops_ingests_null_columns_with_configured_arrow_types():
    connection = _FakeAdbcConnection()
    ops = XtdbAdbcDataframeOps(connection)
    table_config = TableConfig(
        schema="public",
        name="records",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination_date", "DATETIME"),
            TableColumnConfig("records", "amount_value", "DOUBLE"),
        ],
    )

    result = ops.table_insert(
        pl.DataFrame(
            {
                "id": [1],
                "destination_date": [None],
                "amount_value": [None],
            }
        ),
        "public",
        "records",
        table_config=table_config,
    )

    assert result == 1
    _, arrow_table, _, _ = connection.cursor_instance.ingests[0]
    assert arrow_table.schema.field("destination_date").type == pa.timestamp(
        "us", tz="UTC"
    )
    assert arrow_table.schema.field("amount_value").type == pa.float64()
    assert arrow_table.to_pydict() == {
        "_id": [1],
        "id": [1],
        "destination_date": [None],
        "amount_value": [None],
    }


def test_xtdb_adbc_dataframe_ops_reads_arrow_into_polars():
    connection = _FakeAdbcConnection()
    ops = XtdbAdbcDataframeOps(connection)

    result = ops.from_table("public", "records")

    assert result.to_dict(as_series=False) == {"_id": [1], "destination": ["Alpha"]}
    assert connection.cursor_instance.executed == [("SELECT * FROM public.records", {})]


def test_xtdb_adbc_table_query_binds_arrow_key_relation_once():
    connection = _FakeAdbcConnection()
    connection.cursor_instance.arrow_result = pa.table({"id": [1]})
    ops = XtdbAdbcDataframeOps(connection)

    result = ops.table_query(
        "public",
        "records",
        pl.DataFrame({"id": [1, 2]}),
        ["id"],
        table_config=_record_config(),
    )

    assert result.to_dict(as_series=False) == {"id": [1]}
    query, options = connection.cursor_instance.executed[0]
    assert "JOIN UNNEST(?) AS q(key)" in query
    assert options["parameters"].to_pylist() == [{"v0": [{"id": 1}, {"id": 2}]}]


def test_xtdb_adbc_dataframe_ops_targets_configured_schema():
    connection = _FakeAdbcConnection()
    ops = XtdbAdbcDataframeOps(connection)

    ops.table_insert(
        pl.DataFrame({"id": [1]}),
        "analytics",
        "records",
        table_config=_record_config(schema="analytics"),
    )

    assert connection.cursor_instance.ingests[0][3] == {"db_schema_name": "analytics"}


def test_xtdb_adbc_dataframe_ops_splits_ingest_by_max_rows():
    connection = _FakeAdbcConnection()
    ops = XtdbAdbcDataframeOps(connection, max_rows_per_insert=2)

    result = ops.table_insert(
        pl.DataFrame({"id": [1, 2, 3, 4, 5], "destination": ["A", "B", "C", "D", "E"]}),
        "public",
        "records",
        table_config=_record_config(),
    )

    assert result == 5
    ingests = connection.cursor_instance.ingests
    assert len(ingests) == 3
    assert [ingest[1].to_pydict() for ingest in ingests] == [
        {"_id": [1, 2], "id": [1, 2], "destination": ["A", "B"]},
        {"_id": [3, 4], "id": [3, 4], "destination": ["C", "D"]},
        {"_id": [5], "id": [5], "destination": ["E"]},
    ]


def test_xtdb_backend_builds_adbc_uri():
    uri = XtdbBackend().adbc_uri(
        DbEngineConfig(backend="xtdb", hostname="127.0.0.1", adbc_port=19832)
    )

    assert uri == "grpc://127.0.0.1:19832"


def test_xtdb_backend_creates_adbc_connection(monkeypatch):
    fake_flight_sql = Mock()
    fake_flight_sql.connect.return_value = object()
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb._load_flight_sql",
        lambda: fake_flight_sql,
    )

    config = DbEngineConfig(backend="xtdb", hostname="127.0.0.1", adbc_port=19832)
    result = XtdbBackend().create_adbc_connection(config)

    assert result is fake_flight_sql.connect.return_value
    fake_flight_sql.connect.assert_called_once_with(
        "grpc://127.0.0.1:19832",
        autocommit=True,
    )
