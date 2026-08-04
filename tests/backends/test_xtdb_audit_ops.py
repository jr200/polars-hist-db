from datetime import UTC, datetime, timezone

import polars as pl

from polars_hist_db.core.audit import AuditOps
from polars_hist_db.loaders.input_source import InputSource


class _XtdbConnection:
    class _Dialect:
        name = "postgresql"

    dialect = _Dialect()


class _Context:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, exc_value, traceback):
        return False


def test_xtdb_audit_filter_items_creates_audit_table_without_sqlalchemy_inspector(
    monkeypatch,
):
    created_tables = []

    class _TableConfigOps:
        def __init__(self, connection):
            self.connection = connection

        def table_exists(self, table_schema, table_name):
            return False

        def create(self, table_config):
            created_tables.append(table_config)
            return table_config

    class _DataframeOps:
        def __init__(self, connection):
            self.connection = connection

        def from_raw_sql(self, query, schema_overrides=None):
            assert "FROM sample.__audit_log" in query
            return pl.DataFrame(
                {
                    "data_source": [],
                    "data_source_ts": [],
                },
                schema={
                    "data_source": pl.Utf8,
                    "data_source_ts": pl.Datetime("us", "UTC"),
                },
            )

    monkeypatch.setattr(
        "polars_hist_db.core.audit._xtdb_table_config_ops",
        _TableConfigOps,
    )
    monkeypatch.setattr("polars_hist_db.core.audit._xtdb_dataframe_ops", _DataframeOps)

    filtered_items = AuditOps("sample").filter_items(
        pl.DataFrame(
            {
                "__path": ["trades.csv"],
                "__created_at": [datetime(2026, 1, 1, tzinfo=UTC)],
            }
        ),
        "__path",
        "__created_at",
        "trades",
        _XtdbConnection(),
    )

    assert filtered_items.height == 1
    assert [table.name for table in created_tables] == ["__audit_log"]


def test_xtdb_audit_filter_items_returns_all_items_before_audit_data_table_exists(
    monkeypatch,
):
    class _TableConfigOps:
        def __init__(self, connection):
            self.connection = connection

        def table_exists(self, table_schema, table_name):
            return False

        def create(self, table_config):
            return table_config

    class _DataframeOps:
        def __init__(self, connection):
            self.connection = connection

        def from_raw_sql(self, query, schema_overrides=None):
            raise AssertionError("must not query a missing XTDB audit data table")

    monkeypatch.setattr(
        "polars_hist_db.core.audit._xtdb_table_config_ops",
        _TableConfigOps,
    )
    monkeypatch.setattr("polars_hist_db.core.audit._xtdb_dataframe_ops", _DataframeOps)

    source_items = pl.DataFrame(
        {
            "__path": ["trades.csv"],
            "__created_at": [datetime(2026, 1, 1, tzinfo=UTC)],
        }
    )
    filtered_items = AuditOps("sample").filter_items(
        source_items,
        "__path",
        "__created_at",
        "trades",
        _XtdbConnection(),
    )

    assert filtered_items.equals(source_items)


def test_xtdb_audit_filter_items_queries_only_candidate_sources(monkeypatch):
    queried_candidates = []

    class _TableConfigOps:
        def __init__(self, connection):
            self.connection = connection

        def table_exists(self, table_schema, table_name):
            return True

        def from_table(self, table_schema, table_name):
            return AuditOps(table_schema)._table_config(xtdb=True)

    class _DataframeOps:
        def __init__(self, connection):
            self.connection = connection

        def from_raw_sql(self, query, schema_overrides=None):
            assert "MAX(data_source_ts)" in query
            assert "GROUP BY data_source" not in query
            return pl.DataFrame({"data_source_ts": [datetime(2026, 1, 2, tzinfo=UTC)]})

        def table_query(
            self,
            table_schema,
            table_name,
            query_df,
            column_selection,
            table_config=None,
        ):
            queried_candidates.extend(query_df["data_source"].to_list())
            return pl.DataFrame({"data_source": ["processed.csv"]})

    monkeypatch.setattr(
        "polars_hist_db.core.audit._xtdb_table_config_ops",
        _TableConfigOps,
    )
    monkeypatch.setattr("polars_hist_db.core.audit._xtdb_dataframe_ops", _DataframeOps)

    filtered_items = AuditOps("sample").filter_items(
        pl.DataFrame(
            {
                "__path": ["processed.csv", "historic.csv", "new.csv"],
                "__created_at": [
                    datetime(2026, 1, 3, tzinfo=UTC),
                    datetime(2026, 1, 1, tzinfo=UTC),
                    datetime(2026, 1, 3, tzinfo=UTC),
                ],
            }
        ),
        "__path",
        "__created_at",
        "trades",
        _XtdbConnection(),
    )

    assert queried_candidates == ["processed.csv", "historic.csv", "new.csv"]
    assert filtered_items["__path"].to_list() == ["new.csv"]


def test_xtdb_audit_latest_entry_is_empty_before_audit_data_table_exists(
    monkeypatch,
):
    class _TableConfigOps:
        def __init__(self, connection):
            self.connection = connection

        def table_exists(self, table_schema, table_name):
            return False

        def create(self, table_config):
            return table_config

    class _DataframeOps:
        def __init__(self, connection):
            self.connection = connection

        def from_raw_sql(self, query, schema_overrides=None):
            raise AssertionError("must not query a missing XTDB audit data table")

    monkeypatch.setattr(
        "polars_hist_db.core.audit._xtdb_table_config_ops",
        _TableConfigOps,
    )
    monkeypatch.setattr("polars_hist_db.core.audit._xtdb_dataframe_ops", _DataframeOps)

    latest_entry = AuditOps("sample").get_latest_entry(_XtdbConnection())

    assert latest_entry.is_empty()
    assert latest_entry.schema["data_source"] == pl.Utf8
    assert latest_entry.schema["data_source_ts"] == pl.Datetime("us", "UTC")


def test_xtdb_audit_prevalidation_noops_before_audit_data_table_exists(monkeypatch):
    class _TableConfigOps:
        def __init__(self, connection):
            self.connection = connection

        def table_exists(self, table_schema, table_name):
            return False

        def create(self, table_config):
            return table_config

    class _DataframeOps:
        def __init__(self, connection):
            self.connection = connection

        def from_raw_sql(self, query, schema_overrides=None):
            raise AssertionError("must not query a missing XTDB audit data table")

    monkeypatch.setattr(
        "polars_hist_db.core.audit._xtdb_table_config_ops",
        _TableConfigOps,
    )
    monkeypatch.setattr("polars_hist_db.core.audit._xtdb_dataframe_ops", _DataframeOps)

    AuditOps("sample").prevalidate_new_items(
        "trades",
        pl.DataFrame(
            {
                "__path": ["trades.csv"],
                "__created_at": [datetime(2026, 1, 1, tzinfo=UTC)],
            }
        ),
        _XtdbConnection(),
    )


def test_xtdb_file_filter_uses_plain_connection_context(monkeypatch):
    class _InputSource(InputSource):
        async def next_df(self, engine):
            raise NotImplementedError

        async def cleanup(self):
            raise NotImplementedError

    class _AuditOps:
        def __init__(self, schema):
            self.schema = schema

        def filter_items(self, items, *args):
            return items

        def prevalidate_new_items(self, *args):
            return None

    class _Engine:
        class _Dialect:
            name = "postgresql"

        dialect = _Dialect()
        connection = _XtdbConnection()
        used_connect = False

        def connect(self):
            self.used_connect = True
            return _Context(self.connection)

        def begin(self):
            raise AssertionError("XTDB file filtering must not use engine.begin()")

    monkeypatch.setattr("polars_hist_db.loaders.input_source.AuditOps", _AuditOps)
    source = object.__new__(_InputSource)
    source.dataset = type("Dataset", (), {"scrape_limit": 0})()
    engine = _Engine()

    filtered = InputSource._search_and_filter_files(
        source,
        pl.DataFrame(
            {
                "__path": ["trades.csv"],
                "__created_at": [datetime(2026, 1, 1, tzinfo=UTC)],
            }
        ),
        "sample",
        "trades",
        engine,
    )

    assert engine.used_connect is True
    assert filtered.height == 1


def test_xtdb_audit_add_entry_writes_via_backend_dataframe_ops(monkeypatch):
    inserted = []

    class _TableConfigOps:
        def __init__(self, connection):
            self.connection = connection

        def table_exists(self, table_schema, table_name):
            return True

        def from_table(self, table_schema, table_name):
            return AuditOps(table_schema)._table_config(xtdb=True)

    class _DataframeOps:
        def __init__(self, connection):
            self.connection = connection

        def table_insert(self, df, table_schema, table_name, table_config=None):
            inserted.append((df, table_schema, table_name, table_config))
            return df.height

    monkeypatch.setattr(
        "polars_hist_db.core.audit._xtdb_table_config_ops",
        _TableConfigOps,
    )
    monkeypatch.setattr("polars_hist_db.core.audit._xtdb_dataframe_ops", _DataframeOps)

    did_insert = AuditOps("sample").add_entry(
        "dsv",
        "trades.csv",
        "trades",
        _XtdbConnection(),
        datetime(2026, 1, 1, tzinfo=UTC),
    )

    assert did_insert is True
    [(inserted_df, table_schema, table_name, table_config)] = inserted
    assert table_schema == "sample"
    assert table_name == "__audit_log"
    assert table_config.name == "__audit_log"
    assert "audit_id" in inserted_df.columns
    assert inserted_df.schema["audit_id"] == pl.String
    assert len(inserted_df["audit_id"].item()) == 64
    assert inserted_df.select("table_name", "data_source").rows() == [
        ("trades", "trades.csv")
    ]
