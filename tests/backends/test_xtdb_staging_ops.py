from datetime import UTC, date, datetime
from decimal import Decimal
from unittest.mock import Mock

import polars as pl
import pytest

from polars_hist_db.backends.xtdb import (
    XtdbBackend,
    XtdbStagingOps,
    _xtdb_deduced_foreign_key_payload,
    _xtdb_deduced_numeric_foreign_key_ids,
    _xtdb_integer_bits,
)
from polars_hist_db.config import (
    DatasetConfig,
    TableColumnConfig,
    TableConfig,
    ValidTimeConfig,
)


def _staging_with(stage_df, *, adbc_connection=None):
    staging = XtdbStagingOps(object(), adbc_connection=adbc_connection)
    staging._stage_run_cache["stage-1"] = stage_df
    return staging


def _empty_numeric_key_occupancy():
    return pl.DataFrame(
        {"id": [None], "__xtdb_minimum_id": [None]},
        schema={"id": pl.Int64, "__xtdb_minimum_id": pl.Int64},
    )


@pytest.mark.parametrize(
    ("sql_type", "expected_bits"),
    [("TINYINT", 8), ("SMALLINT", 16), ("INT", 32), ("BIGINT", 64)],
)
def test_xtdb_generated_numeric_keys_fit_configured_integer_width(
    sql_type, expected_bits
):
    table = TableConfig(
        schema="ref",
        name="parents",
        columns=[TableColumnConfig("parents", "id", sql_type)],
    )

    generated = _xtdb_deduced_numeric_foreign_key_ids(
        pl.Series(["xtdb-fk-v1:test"]), _xtdb_integer_bits(table, "id") - 1
    ).item()

    assert -(1 << (expected_bits - 1)) <= generated < 0


def test_xtdb_generated_foreign_key_payload_preserves_canonical_ids():
    table_config = TableConfig(schema="ref", name="events", columns=[])
    values = pl.DataFrame(
        {
            "name": ['a"b'],
            "day": [date(2020, 1, 2)],
            "at": [datetime(2020, 1, 2, 3, 4, 5, 123400, tzinfo=UTC)],
            "amount": [Decimal("2.10")],
        }
    )

    payload = values.select(
        _xtdb_deduced_foreign_key_payload(table_config, values.columns, values.schema)
    ).item()

    assert payload == (
        'xtdb-fk-v1:ref.events:[["name","a\\"b"],["day","2020-01-02"],'
        '["at","2020-01-02T03:04:05.123400+00:00"],["amount","2.10"]]'
    )


def test_xtdb_staging_partition_stays_in_memory_without_database_io():
    connection = Mock()
    delta_table_config = TableConfig(
        schema="fakedata",
        name="record_stream",
        columns=[
            TableColumnConfig("record_stream", "record_id", "INT"),
            TableColumnConfig("record_stream", "destination_name", "VARCHAR(64)"),
        ],
    )
    partition_time = datetime(2030, 1, 1, 12, 0, tzinfo=UTC)
    staging = XtdbStagingOps(connection)

    inserted_count = staging.insert_partition(
        pl.DataFrame(
            {
                "record_id": [1, 1],
                "destination_name": ["Beta", "Alpha"],
            }
        ),
        delta_table_config,
        "stage-1",
        partition_time,
        uniqueness_col_set=["record_id"],
        prefill_nulls_with_default=True,
    )

    assert inserted_count == 1
    assert staging._stage_run_cache["stage-1"].to_dict(as_series=False) == {
        "stage_row_index": [0],
        "record_id": [1],
        "destination_name": ["Alpha"],
        "stage_run_id": ["stage-1"],
        "stage_partition_time": [partition_time],
    }
    connection.assert_not_called()


def test_xtdb_staging_903_rows_avoids_database_io():
    config = TableConfig(
        schema="fakedata",
        name="record_stream",
        columns=[TableColumnConfig("record_stream", "record_id", "INT")],
    )
    staging = XtdbStagingOps(object())
    staging._bulk_table_insert = Mock(
        side_effect=AssertionError("staging must not write to XTDB")
    )

    inserted = staging.insert_partition(
        pl.DataFrame({"record_id": range(903)}),
        config,
        "stage-1",
        datetime(2030, 1, 1, tzinfo=UTC),
        uniqueness_col_set=["record_id"],
        prefill_nulls_with_default=True,
    )
    staging.cleanup_run("stage-1")

    assert inserted == 903
    assert not staging._stage_run_cache
    staging._bulk_table_insert.assert_not_called()


def test_xtdb_backend_staging_preserves_insert_row_limit():
    staging = XtdbBackend(max_rows_per_insert=2500).staging(object())

    assert staging.max_rows_per_insert == 2500


def test_xtdb_staging_projects_from_insert_cache_after_partition_insert(monkeypatch):
    table_insert = Mock(side_effect=AssertionError("staging must not write to XTDB"))
    from_raw_sql = Mock(
        return_value=pl.DataFrame(
            schema={
                "stage_run_id": pl.String,
                "stage_row_index": pl.Int64,
                "record_id": pl.Int64,
                "destination_name": pl.String,
            }
        )
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.table_insert",
        table_insert,
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.from_raw_sql",
        from_raw_sql,
    )

    delta_table_config = TableConfig(
        schema="fakedata",
        name="record_stream",
        columns=[
            TableColumnConfig("record_stream", "record_id", "INT"),
            TableColumnConfig("record_stream", "destination_name", "VARCHAR(64)"),
        ],
    )
    dataset = DatasetConfig(
        name="record_stream",
        delta_table_schema="fakedata",
        input_config={"type": "dsv", "search_paths": []},
        pipeline=[
            {
                "schema": "fakedata",
                "table": "records",
                "type": "primary",
                "columns": [
                    {"source": "record_id", "target": "record_id"},
                    {"source": "destination_name", "target": "destination"},
                ],
            }
        ],
    )
    table_config = TableConfig(
        schema="fakedata",
        name="records",
        primary_keys=["record_id"],
        columns=[
            TableColumnConfig("records", "record_id", "INT"),
            TableColumnConfig("records", "destination", "VARCHAR(64)"),
        ],
    )
    staging = XtdbStagingOps(object())

    staging.insert_partition(
        pl.DataFrame({"record_id": [1], "destination_name": ["Alpha"]}),
        delta_table_config,
        "stage-1",
        datetime(2030, 1, 1, tzinfo=UTC),
        uniqueness_col_set=["record_id"],
        prefill_nulls_with_default=True,
    )
    result = staging.prepare_pipeline_item_dataframe(
        "stage-1",
        dataset,
        0,
        table_config,
        valid_time=None,
    )

    assert result.to_dict(as_series=False) == {
        "record_id": [1],
        "destination": ["Alpha"],
    }
    table_insert.assert_not_called()
    from_raw_sql.assert_not_called()


def test_xtdb_staging_projects_empty_insert_from_cache(monkeypatch):
    table_insert = Mock(return_value=0)
    from_raw_sql = Mock(
        side_effect=AssertionError("empty staged partition should stay cached")
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.table_insert",
        table_insert,
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.from_raw_sql",
        from_raw_sql,
    )

    delta_table_config = TableConfig(
        schema="fakedata",
        name="record_stream",
        columns=[
            TableColumnConfig("record_stream", "record_id", "INT"),
            TableColumnConfig("record_stream", "destination_name", "VARCHAR(64)"),
        ],
    )
    dataset = DatasetConfig(
        name="record_stream",
        delta_table_schema="fakedata",
        input_config={"type": "dsv", "search_paths": []},
        pipeline=[
            {
                "schema": "fakedata",
                "table": "records",
                "type": "primary",
                "columns": [
                    {"source": "record_id", "target": "record_id"},
                    {"source": "destination_name", "target": "destination"},
                ],
            }
        ],
    )
    table_config = TableConfig(
        schema="fakedata",
        name="records",
        primary_keys=["record_id"],
        columns=[
            TableColumnConfig("records", "record_id", "INT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(64)"),
        ],
    )
    staging = XtdbStagingOps(object())

    inserted_count = staging.insert_partition(
        pl.DataFrame(schema={"record_id": pl.Int64, "destination_name": pl.String}),
        delta_table_config,
        "stage-1",
        datetime(2030, 1, 1, tzinfo=UTC),
        uniqueness_col_set=["record_id"],
        prefill_nulls_with_default=True,
    )
    result = staging.prepare_pipeline_item_dataframe(
        "stage-1",
        dataset,
        0,
        table_config,
        valid_time=None,
    )

    assert inserted_count == 0
    assert result.is_empty()
    assert result.schema == {"record_id": pl.Int64, "destination": pl.String}
    table_insert.assert_not_called()
    from_raw_sql.assert_not_called()


def test_xtdb_staging_rejects_unknown_run():
    with pytest.raises(ValueError, match="staged partition is unavailable"):
        XtdbStagingOps(object()).prepare_pipeline_item_dataframe(
            "missing", Mock(), 0, Mock(), valid_time=None
        )


def test_xtdb_staging_projects_pipeline_item_with_valid_time_mapping(monkeypatch):
    stage_df = pl.DataFrame(
        {
            "stage_run_id": ["stage-1"],
            "stage_row_index": [0],
            "record_id": [1],
            "destination_name": ["Alpha"],
            "msg_timestamp": [datetime(2030, 1, 1, tzinfo=UTC)],
            "ignored": ["not written"],
        }
    )
    from_raw_sql = Mock(return_value=stage_df)
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.from_raw_sql",
        from_raw_sql,
    )
    dataset = DatasetConfig(
        name="record_stream",
        delta_table_schema="fakedata",
        input_config={"type": "dsv", "search_paths": []},
        pipeline=[
            {
                "schema": "fakedata",
                "table": "records",
                "type": "primary",
                "columns": [
                    {"source": "record_id", "target": "record_id"},
                    {"source": "destination_name", "target": "destination"},
                ],
            }
        ],
    )
    table_config = TableConfig(
        schema="fakedata",
        name="records",
        primary_keys=["record_id"],
        columns=[
            TableColumnConfig("records", "record_id", "INT"),
            TableColumnConfig("records", "destination", "VARCHAR(64)"),
        ],
    )

    staging = XtdbStagingOps(object())
    staging._stage_run_cache["stage-1"] = stage_df
    result = staging.prepare_pipeline_item_dataframe(
        "stage-1",
        dataset,
        0,
        table_config,
        valid_time=ValidTimeConfig(table="records", from_column="msg_timestamp"),
    )

    assert result.to_dict(as_series=False) == {
        "record_id": [1],
        "destination": ["Alpha"],
        "msg_timestamp": [datetime(2030, 1, 1, tzinfo=UTC)],
    }
    from_raw_sql.assert_not_called()


def test_xtdb_staging_projects_missing_nullable_pipeline_columns(monkeypatch):
    stage_df = pl.DataFrame(
        {
            "stage_run_id": ["stage-1"],
            "stage_row_index": [0],
            "record_id": [1],
            "destination_name": ["Alpha"],
        }
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.from_raw_sql",
        Mock(return_value=stage_df),
    )
    dataset = DatasetConfig(
        name="record_stream",
        delta_table_schema="fakedata",
        input_config={"type": "dsv", "search_paths": []},
        pipeline=[
            {
                "schema": "fakedata",
                "table": "records",
                "type": "primary",
                "columns": [
                    {"source": "record_id", "target": "record_id"},
                    {"source": "destination_name", "target": "destination"},
                    {"source": "source_note", "target": "source_note"},
                ],
            }
        ],
    )
    table_config = TableConfig(
        schema="fakedata",
        name="records",
        primary_keys=["record_id"],
        columns=[
            TableColumnConfig("records", "record_id", "INT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(64)"),
            TableColumnConfig("records", "source_note", "VARCHAR(128)"),
        ],
    )

    staging = XtdbStagingOps(object())
    staging._stage_run_cache["stage-1"] = stage_df
    result = staging.prepare_pipeline_item_dataframe(
        "stage-1",
        dataset,
        0,
        table_config,
        valid_time=None,
    )

    assert result.to_dict(as_series=False) == {
        "record_id": [1],
        "destination": ["Alpha"],
        "source_note": [None],
    }
    assert result.schema["source_note"] == pl.String


def test_xtdb_staging_cache_materializes_missing_columns(monkeypatch):
    table_insert = Mock(side_effect=AssertionError("staging must not write to XTDB"))
    from_raw_sql = Mock(
        return_value=pl.DataFrame(
            schema={
                "stage_run_id": pl.String,
                "stage_row_index": pl.Int64,
                "record_id": pl.Int64,
                "destination_name": pl.String,
                "source_note": pl.String,
            }
        )
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.table_insert",
        table_insert,
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.from_raw_sql",
        from_raw_sql,
    )

    delta_table_config = TableConfig(
        schema="fakedata",
        name="record_stream",
        columns=[
            TableColumnConfig("record_stream", "record_id", "INT", nullable=False),
            TableColumnConfig("record_stream", "destination_name", "VARCHAR(64)"),
            TableColumnConfig("record_stream", "source_note", "VARCHAR(128)"),
        ],
    )
    dataset = DatasetConfig(
        name="record_stream",
        delta_table_schema="fakedata",
        input_config={"type": "dsv", "search_paths": []},
        pipeline=[
            {
                "schema": "fakedata",
                "table": "records",
                "type": "primary",
                "columns": [
                    {"source": "record_id", "target": "record_id"},
                    {"source": "destination_name", "target": "destination"},
                    {"source": "source_note", "target": "source_note"},
                ],
            }
        ],
    )
    table_config = TableConfig(
        schema="fakedata",
        name="records",
        primary_keys=["record_id"],
        columns=[
            TableColumnConfig("records", "record_id", "INT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(64)"),
            TableColumnConfig("records", "source_note", "VARCHAR(128)"),
        ],
    )
    staging = XtdbStagingOps(object())

    staging.insert_partition(
        pl.DataFrame({"record_id": [1], "destination_name": ["Alpha"]}),
        delta_table_config,
        "stage-1",
        datetime(2030, 1, 1, tzinfo=UTC),
        uniqueness_col_set=["record_id"],
        prefill_nulls_with_default=True,
    )
    result = staging.prepare_pipeline_item_dataframe(
        "stage-1",
        dataset,
        0,
        table_config,
        valid_time=None,
    )

    assert result.to_dict(as_series=False) == {
        "record_id": [1],
        "destination": ["Alpha"],
        "source_note": [None],
    }
    assert result.schema["source_note"] == pl.String
    assert staging._stage_run_cache["stage-1"].to_dict(as_series=False) == {
        "stage_row_index": [0],
        "record_id": [1],
        "destination_name": ["Alpha"],
        "source_note": [None],
        "stage_run_id": ["stage-1"],
        "stage_partition_time": [datetime(2030, 1, 1, tzinfo=UTC)],
    }
    assert staging._stage_run_cache["stage-1"].schema["source_note"] == pl.String
    table_insert.assert_not_called()
    from_raw_sql.assert_not_called()


def test_xtdb_staging_rejects_missing_required_pipeline_columns(monkeypatch):
    stage_df = pl.DataFrame(
        {
            "stage_run_id": ["stage-1"],
            "stage_row_index": [0],
            "record_id": [1],
        }
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.from_raw_sql",
        Mock(return_value=stage_df),
    )
    dataset = DatasetConfig(
        name="record_stream",
        delta_table_schema="fakedata",
        input_config={"type": "dsv", "search_paths": []},
        pipeline=[
            {
                "schema": "fakedata",
                "table": "records",
                "type": "primary",
                "columns": [
                    {"source": "record_id", "target": "record_id"},
                    {"source": "destination_name", "target": "destination"},
                ],
            }
        ],
    )
    table_config = TableConfig(
        schema="fakedata",
        name="records",
        primary_keys=["record_id"],
        columns=[
            TableColumnConfig("records", "record_id", "INT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(64)", nullable=False),
        ],
    )

    staging = XtdbStagingOps(object())
    staging._stage_run_cache["stage-1"] = stage_df
    with pytest.raises(ValueError, match="column mismatch"):
        staging.prepare_pipeline_item_dataframe(
            "stage-1",
            dataset,
            0,
            table_config,
            valid_time=None,
        )


def test_xtdb_staging_omits_explicitly_inapplicable_rows(caplog):
    dataset = DatasetConfig(
        name="record_stream",
        delta_table_schema="sample",
        input_config={"type": "dsv", "search_paths": []},
        pipeline=[
            {
                "schema": "sample",
                "table": "records",
                "type": "primary",
                "columns": [
                    {"source": "record_id", "target": "record_id"},
                    {
                        "source": "observed_at",
                        "target": "observed_at",
                        "nullable": True,
                        "omit_row_if_null": True,
                    },
                ],
            }
        ],
    )
    table_config = TableConfig(
        schema="sample",
        name="records",
        primary_keys=["record_id"],
        columns=[
            TableColumnConfig("records", "record_id", "INT", nullable=False),
            TableColumnConfig(
                "records", "observed_at", "TIMESTAMP WITH TIME ZONE", nullable=False
            ),
        ],
    )
    stage_df = pl.DataFrame(
        {
            "record_id": [1, 2],
            "observed_at": [
                datetime(2030, 1, 1, tzinfo=UTC),
                None,
            ],
        }
    )
    staging = XtdbStagingOps(object())
    staging._stage_run_cache["stage-1"] = stage_df

    result = staging.prepare_pipeline_item_dataframe(
        "stage-1",
        dataset,
        0,
        table_config,
        valid_time=ValidTimeConfig(
            schema="sample", table="records", from_column="observed_at"
        ),
    )

    assert result.to_dict(as_series=False) == {
        "record_id": [1],
        "observed_at": [datetime(2030, 1, 1, tzinfo=UTC)],
    }
    assert "omitted 1 pipeline row" in caplog.text


def test_xtdb_staging_cleanup_only_discards_memory_cache():
    connection = Mock()
    staging = XtdbStagingOps(connection)
    staging._stage_run_cache["stage-1"] = pl.DataFrame(
        {
            "stage_run_id": ["stage-1", "stage-1"],
            "stage_row_index": [0, 1],
            "record_id": [10, 20],
            "stage_partition_time": [
                datetime.fromisoformat("2030-01-01"),
                datetime.fromisoformat("2030-01-01"),
            ],
        }
    )

    staging.cleanup_run("stage-1")

    assert "stage-1" not in staging._stage_run_cache
    connection.assert_not_called()


def test_xtdb_staging_deduces_foreign_keys_and_inserts_missing_parent(
    monkeypatch,
):
    stage_df = pl.DataFrame(
        {
            "stage_run_id": ["stage-1"],
            "stage_row_index": [0],
            "country_id": [None],
            "country_name": ["Japan"],
        },
        schema={
            "stage_run_id": pl.String,
            "stage_row_index": pl.Int64,
            "country_id": pl.String,
            "country_name": pl.String,
        },
    )
    parent_df = pl.DataFrame(
        schema={
            "_id": pl.String,
            "country_id": pl.String,
            "name": pl.String,
        }
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.from_raw_sql",
        Mock(return_value=stage_df),
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.table_query",
        Mock(return_value=parent_df),
    )
    inserted = {}

    def table_insert(self, df, table_schema, table_name, table_config=None):
        inserted["df"] = df
        inserted["table_schema"] = table_schema
        inserted["table_name"] = table_name
        inserted["table_config"] = table_config
        return df.height

    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.table_insert",
        table_insert,
    )
    dataset = DatasetConfig(
        name="country_stream",
        delta_table_schema="ref",
        input_config={"type": "dsv", "search_paths": []},
        pipeline=[
            {
                "schema": "ref",
                "table": "countries",
                "type": "primary",
                "columns": [
                    {
                        "source": "country_id",
                        "target": "country_id",
                        "deduce_foreign_key": True,
                    },
                    {"source": "country_name", "target": "name"},
                ],
            }
        ],
    )
    table_config = TableConfig(
        schema="ref",
        name="countries",
        primary_keys=["country_id"],
        columns=[
            TableColumnConfig("countries", "country_id", "VARCHAR(128)"),
            TableColumnConfig("countries", "name", "VARCHAR(64)"),
        ],
    )

    result = _staging_with(stage_df).prepare_pipeline_item_dataframe(
        "stage-1",
        dataset,
        0,
        table_config,
        valid_time=None,
    )

    generated_id = 'xtdb-fk-v1:ref.countries:[["name","Japan"]]'
    assert inserted["table_schema"] == "ref"
    assert inserted["table_name"] == "countries"
    assert inserted["table_config"] == table_config
    assert inserted["df"].to_dict(as_series=False) == {
        "country_id": [generated_id],
        "name": ["Japan"],
    }
    assert result.to_dict(as_series=False) == {
        "country_id": [generated_id],
        "name": ["Japan"],
    }


def test_xtdb_staging_inserts_missing_parent_with_adbc_bulk_connection(monkeypatch):
    adbc_connection = object()
    stage_df = pl.DataFrame(
        {
            "stage_run_id": ["stage-1"],
            "stage_row_index": [0],
            "country_id": [None],
            "country_name": ["Japan"],
        },
        schema={
            "stage_run_id": pl.String,
            "stage_row_index": pl.Int64,
            "country_id": pl.String,
            "country_name": pl.String,
        },
    )
    parent_df = pl.DataFrame(
        schema={
            "_id": pl.String,
            "country_id": pl.String,
            "name": pl.String,
        }
    )
    pgwire_insert = Mock(side_effect=AssertionError("should use ADBC bulk ingest"))
    inserted = {}

    def adbc_table_insert(self, df, table_schema, table_name, table_config=None):
        inserted["connection"] = self.connection
        inserted["df"] = df
        inserted["table_schema"] = table_schema
        inserted["table_name"] = table_name
        inserted["table_config"] = table_config
        return df.height

    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.from_raw_sql",
        Mock(return_value=stage_df),
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbAdbcDataframeOps.table_query",
        Mock(return_value=parent_df),
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.table_insert",
        pgwire_insert,
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbAdbcDataframeOps.table_insert",
        adbc_table_insert,
    )
    dataset = DatasetConfig(
        name="country_stream",
        delta_table_schema="ref",
        input_config={"type": "dsv", "search_paths": []},
        pipeline=[
            {
                "schema": "ref",
                "table": "countries",
                "type": "primary",
                "columns": [
                    {
                        "source": "country_id",
                        "target": "country_id",
                        "deduce_foreign_key": True,
                    },
                    {"source": "country_name", "target": "name"},
                ],
            }
        ],
    )
    table_config = TableConfig(
        schema="ref",
        name="countries",
        primary_keys=["country_id"],
        columns=[
            TableColumnConfig("countries", "country_id", "VARCHAR(128)"),
            TableColumnConfig("countries", "name", "VARCHAR(64)"),
        ],
    )

    result = _staging_with(
        stage_df, adbc_connection=adbc_connection
    ).prepare_pipeline_item_dataframe(
        "stage-1",
        dataset,
        0,
        table_config,
        valid_time=None,
    )

    generated_id = 'xtdb-fk-v1:ref.countries:[["name","Japan"]]'
    assert inserted["connection"] is adbc_connection
    assert inserted["table_schema"] == "ref"
    assert inserted["table_name"] == "countries"
    assert inserted["table_config"] == table_config
    assert inserted["df"].to_dict(as_series=False) == {
        "country_id": [generated_id],
        "name": ["Japan"],
    }
    assert result.to_dict(as_series=False) == {
        "country_id": [generated_id],
        "name": ["Japan"],
    }
    pgwire_insert.assert_not_called()


def test_xtdb_staging_deduces_explicit_numeric_foreign_keys(monkeypatch):
    stage_df = pl.DataFrame(
        {
            "stage_run_id": ["stage-1"],
            "stage_row_index": [0],
            "origin_location_id": [1001],
            "origin_name": ["Alpha"],
            "origin_country": ["Nigeria"],
        }
    )
    parent_df = pl.DataFrame(
        schema={
            "id": pl.Int64,
            "name": pl.String,
            "country": pl.String,
        }
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.from_raw_sql",
        Mock(return_value=_empty_numeric_key_occupancy()),
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.table_query",
        Mock(return_value=parent_df),
    )
    inserted = {}

    def table_insert(self, df, table_schema, table_name, table_config=None):
        inserted["df"] = df
        inserted["table_schema"] = table_schema
        inserted["table_name"] = table_name
        inserted["table_config"] = table_config
        return df.height

    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.table_insert",
        table_insert,
    )
    dataset = DatasetConfig(
        name="record_stream",
        delta_table_schema="sample",
        input_config={"type": "dsv", "search_paths": []},
        pipeline=[
            {
                "schema": "sample",
                "table": "location_info",
                "type": "extract",
                "columns": [
                    {
                        "source": "origin_location_id",
                        "target": "id",
                        "deduce_foreign_key": True,
                    },
                    {"source": "origin_name", "target": "name"},
                    {"source": "origin_country", "target": "country"},
                ],
            },
            {
                "schema": "sample",
                "table": "trades",
                "type": "primary",
                "columns": [
                    {"source": "origin_location_id", "target": "origin_location_id"},
                ],
            },
        ],
    )
    table_config = TableConfig(
        schema="sample",
        name="location_info",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("location_info", "id", "INT", nullable=False),
            TableColumnConfig("location_info", "name", "VARCHAR(64)"),
            TableColumnConfig("location_info", "country", "VARCHAR(64)"),
        ],
    )

    result = _staging_with(stage_df).prepare_pipeline_item_dataframe(
        "stage-1",
        dataset,
        0,
        table_config,
        valid_time=None,
    )

    assert inserted["table_schema"] == "sample"
    assert inserted["table_name"] == "location_info"
    assert inserted["table_config"] == table_config
    assert inserted["df"].to_dict(as_series=False) == {
        "id": [1001],
        "name": ["Alpha"],
        "country": ["Nigeria"],
    }
    assert result.to_dict(as_series=False) == {
        "id": [1001],
        "name": ["Alpha"],
        "country": ["Nigeria"],
    }


def test_xtdb_staging_generates_numeric_foreign_key_when_source_key_is_missing(
    monkeypatch,
):
    stage_df = pl.DataFrame(
        {
            "stage_run_id": ["stage-1"],
            "stage_row_index": [0],
            "destination_location_id": [None],
            "destination_name": ["Southern Europe"],
            "destination_country": ["#Unspecified"],
        },
        schema={
            "stage_run_id": pl.String,
            "stage_row_index": pl.Int64,
            "destination_location_id": pl.Int64,
            "destination_name": pl.String,
            "destination_country": pl.String,
        },
    )
    parent_df = pl.DataFrame(
        schema={
            "id": pl.Int64,
            "name": pl.String,
            "country": pl.String,
        }
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.from_raw_sql",
        Mock(return_value=_empty_numeric_key_occupancy()),
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.table_query",
        Mock(return_value=parent_df),
    )
    inserted = {}

    def table_insert(self, df, table_schema, table_name, table_config=None):
        inserted["df"] = df
        return df.height

    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.table_insert",
        table_insert,
    )
    dataset = DatasetConfig(
        name="record_stream",
        delta_table_schema="sample",
        input_config={"type": "dsv", "search_paths": []},
        pipeline=[
            {
                "schema": "sample",
                "table": "location_info",
                "type": "extract",
                "columns": [
                    {
                        "source": "destination_location_id",
                        "target": "id",
                        "deduce_foreign_key": True,
                    },
                    {"source": "destination_name", "target": "name"},
                    {"source": "destination_country", "target": "country"},
                ],
            },
            {
                "schema": "sample",
                "table": "trades",
                "type": "primary",
                "columns": [
                    {
                        "source": "destination_location_id",
                        "target": "destination_location_id",
                    },
                ],
            },
        ],
    )
    table_config = TableConfig(
        schema="sample",
        name="location_info",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("location_info", "id", "INT", nullable=False),
            TableColumnConfig("location_info", "name", "VARCHAR(64)"),
            TableColumnConfig("location_info", "country", "VARCHAR(64)"),
        ],
    )

    result = _staging_with(stage_df).prepare_pipeline_item_dataframe(
        "stage-1",
        dataset,
        0,
        table_config,
        valid_time=None,
    )

    generated_id = result[0, "id"]
    assert generated_id == -970706434
    assert inserted["df"].to_dict(as_series=False) == {
        "id": [generated_id],
        "name": ["Southern Europe"],
        "country": ["#Unspecified"],
    }
    assert result.to_dict(as_series=False) == {
        "id": [generated_id],
        "name": ["Southern Europe"],
        "country": ["#Unspecified"],
    }


def test_xtdb_staging_reuses_deduced_foreign_key_for_later_primary_item(
    monkeypatch,
):
    stage_df = pl.DataFrame(
        {
            "stage_run_id": ["stage-1"],
            "stage_row_index": [0],
            "trade_id": [308327],
            "destination_location_id": [None],
            "destination_name": ["#Unspecified"],
            "destination_country": ["#Unspecified"],
        },
        schema={
            "stage_run_id": pl.String,
            "stage_row_index": pl.Int64,
            "trade_id": pl.Int64,
            "destination_location_id": pl.Int64,
            "destination_name": pl.String,
            "destination_country": pl.String,
        },
    )
    parent_df = pl.DataFrame(
        schema={
            "id": pl.Int64,
            "name": pl.String,
            "country": pl.String,
        }
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.from_raw_sql",
        Mock(return_value=_empty_numeric_key_occupancy()),
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.table_query",
        Mock(return_value=parent_df),
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.table_insert",
        lambda self, df, table_schema, table_name, table_config=None: df.height,
    )
    dataset = DatasetConfig(
        name="record_stream",
        delta_table_schema="sample",
        input_config={"type": "dsv", "search_paths": []},
        pipeline=[
            {
                "schema": "sample",
                "table": "location_info",
                "type": "extract",
                "columns": [
                    {
                        "source": "destination_location_id",
                        "target": "id",
                        "deduce_foreign_key": True,
                    },
                    {"source": "destination_name", "target": "name"},
                    {"source": "destination_country", "target": "country"},
                ],
            },
            {
                "schema": "sample",
                "table": "trades",
                "type": "primary",
                "columns": [
                    {"source": "trade_id", "target": "id"},
                    {
                        "source": "destination_location_id",
                        "target": "destination_location_id",
                    },
                ],
            },
        ],
    )
    location_config = TableConfig(
        schema="sample",
        name="location_info",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("location_info", "id", "INT", nullable=False),
            TableColumnConfig("location_info", "name", "VARCHAR(64)"),
            TableColumnConfig("location_info", "country", "VARCHAR(64)"),
        ],
    )
    trades_config = TableConfig(
        schema="sample",
        name="trades",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("trades", "id", "INT", nullable=False),
            TableColumnConfig("trades", "destination_location_id", "INT"),
        ],
    )
    staging = XtdbStagingOps(object())
    staging._stage_run_cache["stage-1"] = stage_df

    location_df = staging.prepare_pipeline_item_dataframe(
        "stage-1",
        dataset,
        0,
        location_config,
        valid_time=None,
    )
    trades_df = staging.prepare_pipeline_item_dataframe(
        "stage-1",
        dataset,
        1,
        trades_config,
        valid_time=None,
    )

    generated_id = location_df[0, "id"]
    assert location_df.schema["id"] == pl.Int32
    assert trades_df.schema["destination_location_id"] == pl.Int32
    assert trades_df.to_dict(as_series=False) == {
        "id": [308327],
        "destination_location_id": [generated_id],
    }


def test_xtdb_staging_does_not_insert_same_generated_parent_twice(
    monkeypatch,
):
    stage_df = pl.DataFrame(
        {
            "stage_run_id": ["stage-1", "stage-1"],
            "stage_row_index": [0, 1],
            "origin_location_id": [None, None],
            "origin_name": ["#Unspecified", "#Unspecified"],
            "origin_country": ["#Unspecified", "#Unspecified"],
            "destination_location_id": [None, None],
            "destination_name": ["#Unspecified", "#Unspecified"],
            "destination_country": ["#Unspecified", "#Unspecified"],
        },
        schema={
            "stage_run_id": pl.String,
            "stage_row_index": pl.Int64,
            "origin_location_id": pl.Int64,
            "origin_name": pl.String,
            "origin_country": pl.String,
            "destination_location_id": pl.Int64,
            "destination_name": pl.String,
            "destination_country": pl.String,
        },
    )
    parent_df = pl.DataFrame(
        schema={
            "id": pl.Int64,
            "name": pl.String,
            "country": pl.String,
        }
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.from_raw_sql",
        Mock(return_value=_empty_numeric_key_occupancy()),
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.table_query",
        Mock(return_value=parent_df),
    )
    inserted = []

    def table_insert(self, df, table_schema, table_name, table_config=None):
        inserted.append(df)
        return df.height

    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.table_insert",
        table_insert,
    )
    dataset = DatasetConfig(
        name="record_stream",
        delta_table_schema="sample",
        input_config={"type": "dsv", "search_paths": []},
        pipeline=[
            {
                "schema": "sample",
                "table": "location_info",
                "type": "extract",
                "columns": [
                    {
                        "source": "origin_location_id",
                        "target": "id",
                        "deduce_foreign_key": True,
                    },
                    {"source": "origin_name", "target": "name"},
                    {"source": "origin_country", "target": "country"},
                ],
            },
            {
                "schema": "sample",
                "table": "location_info",
                "columns": [
                    {
                        "source": "destination_location_id",
                        "target": "id",
                        "deduce_foreign_key": True,
                    },
                    {"source": "destination_name", "target": "name"},
                    {"source": "destination_country", "target": "country"},
                ],
            },
            {
                "schema": "sample",
                "table": "trades",
                "type": "primary",
                "columns": [
                    {"source": "origin_location_id", "target": "origin_location_id"},
                ],
            },
        ],
    )
    location_config = TableConfig(
        schema="sample",
        name="location_info",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("location_info", "id", "INT", nullable=False),
            TableColumnConfig("location_info", "name", "VARCHAR(64)"),
            TableColumnConfig("location_info", "country", "VARCHAR(64)"),
        ],
    )
    staging = XtdbStagingOps(object())
    staging._stage_run_cache["stage-1"] = stage_df

    origin_df = staging.prepare_pipeline_item_dataframe(
        "stage-1",
        dataset,
        0,
        location_config,
        valid_time=None,
    )
    destination_df = staging.prepare_pipeline_item_dataframe(
        "stage-1",
        dataset,
        1,
        location_config,
        valid_time=None,
    )

    assert len(inserted) == 1
    assert origin_df[0, "id"] == inserted[0][0, "id"]
    assert destination_df.is_empty()


def test_xtdb_staging_deduces_existing_and_missing_foreign_keys(monkeypatch):
    stage_df = pl.DataFrame(
        {
            "stage_run_id": ["stage-1", "stage-1"],
            "stage_row_index": [0, 1],
            "country_id": [None, None],
            "country_name": ["Japan", "Korea"],
        },
        schema={
            "stage_run_id": pl.String,
            "stage_row_index": pl.Int64,
            "country_id": pl.String,
            "country_name": pl.String,
        },
    )
    parent_df = pl.DataFrame(
        {
            "country_id": ["country:japan"],
            "name": ["Japan"],
        }
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.from_raw_sql",
        Mock(return_value=stage_df),
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.table_query",
        Mock(return_value=parent_df),
    )
    table_insert = Mock(return_value=1)
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.table_insert",
        table_insert,
    )
    dataset = DatasetConfig(
        name="country_stream",
        delta_table_schema="ref",
        input_config={"type": "dsv", "search_paths": []},
        pipeline=[
            {
                "schema": "ref",
                "table": "countries",
                "type": "primary",
                "columns": [
                    {
                        "source": "country_id",
                        "target": "country_id",
                        "deduce_foreign_key": True,
                    },
                    {"source": "country_name", "target": "name"},
                ],
            }
        ],
    )
    table_config = TableConfig(
        schema="ref",
        name="countries",
        primary_keys=["country_id"],
        columns=[
            TableColumnConfig("countries", "country_id", "VARCHAR(128)"),
            TableColumnConfig("countries", "name", "VARCHAR(64)"),
        ],
    )

    result = _staging_with(stage_df).prepare_pipeline_item_dataframe(
        "stage-1",
        dataset,
        0,
        table_config,
        valid_time=None,
    )

    inserted = table_insert.call_args.args[0]
    generated_id = 'xtdb-fk-v1:ref.countries:[["name","Korea"]]'
    assert inserted.to_dict(as_series=False) == {
        "country_id": [generated_id],
        "name": ["Korea"],
    }
    assert result.to_dict(as_series=False) == {
        "country_id": ["country:japan", generated_id],
        "name": ["Japan", "Korea"],
    }


def test_xtdb_staging_deduces_foreign_keys_with_null_typed_empty_parent(
    monkeypatch,
):
    stage_df = pl.DataFrame(
        {
            "stage_run_id": ["stage-1"],
            "stage_row_index": [0],
            "country_id": ["country:japan"],
            "country_name": ["Japan"],
        }
    )
    parent_df = pl.DataFrame(
        {
            "country_id": pl.Series("country_id", [], dtype=pl.Null),
            "name": pl.Series("name", [], dtype=pl.Null),
        }
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.from_raw_sql",
        Mock(return_value=stage_df),
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.table_query",
        Mock(return_value=parent_df),
    )
    table_insert = Mock(return_value=1)
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.table_insert",
        table_insert,
    )
    dataset = DatasetConfig(
        name="country_stream",
        delta_table_schema="ref",
        input_config={"type": "dsv", "search_paths": []},
        pipeline=[
            {
                "schema": "ref",
                "table": "countries",
                "type": "primary",
                "columns": [
                    {
                        "source": "country_id",
                        "target": "country_id",
                        "deduce_foreign_key": True,
                    },
                    {"source": "country_name", "target": "name"},
                ],
            }
        ],
    )
    table_config = TableConfig(
        schema="ref",
        name="countries",
        primary_keys=["country_id"],
        columns=[
            TableColumnConfig("countries", "country_id", "VARCHAR(128)"),
            TableColumnConfig("countries", "name", "VARCHAR(64)"),
        ],
    )

    result = _staging_with(stage_df).prepare_pipeline_item_dataframe(
        "stage-1",
        dataset,
        0,
        table_config,
        valid_time=None,
    )

    assert result.to_dict(as_series=False) == {
        "country_id": ["country:japan"],
        "name": ["Japan"],
    }


@pytest.mark.parametrize("use_adbc", [False, True])
def test_xtdb_staging_resolves_generated_numeric_key_collisions(monkeypatch, use_adbc):
    table_config = TableConfig(
        schema="ref",
        name="parents",
        primary_keys=["id"],
        columns=[TableColumnConfig("parents", "id", "INT", nullable=False)],
    )
    from_raw_sql = Mock(
        return_value=pl.DataFrame(
            {"id": [None], "__xtdb_minimum_id": [None]},
            schema={"id": pl.Int64, "__xtdb_minimum_id": pl.Int64},
        )
    )
    ops_class = "XtdbAdbcDataframeOps" if use_adbc else "XtdbDataframeOps"
    monkeypatch.setattr(
        f"polars_hist_db.backends.xtdb.{ops_class}.from_raw_sql", from_raw_sql
    )
    rows = pl.DataFrame(
        {
            "id": [-42, -42, -43],
            "__xtdb_generated_id": [True, True, True],
        }
    )

    result = XtdbStagingOps(
        object(), adbc_connection=object() if use_adbc else None
    )._resolve_numeric_foreign_key_collisions(
        rows,
        table_config,
        ["id"],
    )

    assert result.get_column("id").to_list() == [-42, -44, -43]
    assert from_raw_sql.call_count == 1
    query, _, execute_options = from_raw_sql.call_args.args
    if use_adbc:
        assert "JOIN UNNEST(?) AS q(key)" in query
        assert "t._id = (q.key).id" in query
        assert execute_options["parameters"].to_pylist() == [
            {"v0": [{"id": -42}, {"id": -43}]}
        ]
    else:
        assert "JOIN UNNEST(%s) AS q(key)" in query
        assert "t._id = CAST((q.key).id AS INTEGER)" in query
        parameter = execute_options["parameters"][0]
        assert parameter.obj == [{"id": -42}, {"id": -43}]


def test_xtdb_staging_rejects_explicit_numeric_key_collisions(monkeypatch):
    table_config = TableConfig(
        schema="ref",
        name="parents",
        primary_keys=["id"],
        columns=[TableColumnConfig("parents", "id", "INT", nullable=False)],
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.from_raw_sql",
        Mock(return_value=pl.DataFrame({"id": [-42], "__xtdb_minimum_id": [-42]})),
    )
    rows = pl.DataFrame({"id": [-42], "__xtdb_generated_id": [False]})

    with pytest.raises(ValueError, match="parents.id is already in use"):
        XtdbStagingOps(object())._resolve_numeric_foreign_key_collisions(
            rows,
            table_config,
            ["id"],
        )


def test_xtdb_staging_rejects_exhausted_numeric_keys(monkeypatch):
    table_config = TableConfig(
        schema="ref",
        name="parents",
        primary_keys=["id"],
        columns=[TableColumnConfig("parents", "id", "TINYINT", nullable=False)],
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.from_raw_sql",
        Mock(
            return_value=pl.DataFrame(
                {"id": [None], "__xtdb_minimum_id": [None]},
                schema={"id": pl.Int8, "__xtdb_minimum_id": pl.Int8},
            )
        ),
    )
    rows = pl.DataFrame(
        {"id": [-128, -128], "__xtdb_generated_id": [True, True]},
        schema={"id": pl.Int8, "__xtdb_generated_id": pl.Boolean},
    )

    with pytest.raises(ValueError, match="No generated foreign keys remain"):
        XtdbStagingOps(object())._resolve_numeric_foreign_key_collisions(
            rows,
            table_config,
            ["id"],
        )


def test_xtdb_staging_avoids_existing_generated_numeric_key(monkeypatch):
    table_config = TableConfig(
        schema="ref",
        name="parents",
        primary_keys=["id"],
        columns=[TableColumnConfig("parents", "id", "INT", nullable=False)],
    )
    from_raw_sql = Mock(
        return_value=pl.DataFrame({"id": [-42], "__xtdb_minimum_id": [-42]})
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.from_raw_sql",
        from_raw_sql,
    )
    rows = pl.DataFrame({"id": [-42], "__xtdb_generated_id": [True]})

    result = XtdbStagingOps(object())._resolve_numeric_foreign_key_collisions(
        rows,
        table_config,
        ["id"],
    )

    assert result.get_column("id").to_list() == [-43]
    assert from_raw_sql.call_count == 1


def test_xtdb_staging_avoids_an_existing_numeric_key_chain(monkeypatch):
    table_config = TableConfig(
        schema="ref",
        name="parents",
        primary_keys=["id"],
        columns=[TableColumnConfig("parents", "id", "INT", nullable=False)],
    )
    from_raw_sql = Mock(
        return_value=pl.DataFrame({"id": [-42], "__xtdb_minimum_id": [-43]})
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.from_raw_sql",
        from_raw_sql,
    )
    rows = pl.DataFrame({"id": [-42], "__xtdb_generated_id": [True]})

    result = XtdbStagingOps(object())._resolve_numeric_foreign_key_collisions(
        rows,
        table_config,
        ["id"],
    )

    assert result.get_column("id").to_list() == [-44]
    assert from_raw_sql.call_count == 1
