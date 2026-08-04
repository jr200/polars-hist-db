from datetime import UTC, date, datetime, timezone

import polars as pl
import pytest
from sqlalchemy import Column, DateTime, Integer, MetaData, String, Table

from polars_hist_db.config import DeltaConfig
from polars_hist_db.config.parser_config import IngestionColumnConfig
from polars_hist_db.core.audit import AuditOps
from polars_hist_db.core.dataframe import DataframeOps
from polars_hist_db.core.delta_table import (
    DeltaTableOps,
    _prevalidate_upsert_from_table,
)
from polars_hist_db.loaders.dsv.dsv_loader import _validate_expected_columns
from polars_hist_db.types import PolarsType


def test_boolean_string_defaults_are_parsed():
    df = pl.DataFrame({"enabled": [None]}, schema={"enabled": pl.Boolean})

    result = DataframeOps.fill_nulls_with_defaults(df, {"enabled": "false"})

    assert result["enabled"].to_list() == [False]


def test_date_string_defaults_are_parsed_without_deprecation_warning(recwarn):
    df = pl.DataFrame({"as_of": [None]}, schema={"as_of": pl.Date})

    result = DataframeOps.fill_nulls_with_defaults(df, {"as_of": "1985-10-26"})

    assert not [
        warning for warning in recwarn if warning.category is DeprecationWarning
    ]
    assert result["as_of"].to_list() == [date(1985, 10, 26)]


def test_missing_dsv_columns_are_added_to_rows():
    config = IngestionColumnConfig(
        column_type="data",
        schema="sample",
        table="items",
        ingestion_data_type="VARCHAR(10)",
        target_data_type="VARCHAR(10)",
        source="missing",
        target="missing",
    )

    result = _validate_expected_columns(pl.DataFrame({"present": [1, 2]}), [config])

    assert result.schema == {"missing": pl.String}
    assert result["missing"].to_list() == [None, None]


@pytest.mark.parametrize("expected", [pl.String, pl.Utf8, pl.Categorical])
def test_database_contract_accepts_all_string_encodings(expected):
    categorical = pl.DataFrame({"value": ["one", "two"]}).with_columns(
        pl.col("value").cast(pl.Categorical)
    )

    result = PolarsType.enforce_database_schema(
        categorical,
        {"value": expected},
        backend="test",
        operation="table_insert",
    )

    assert result.schema["value"] == pl.Categorical


def test_last_delta_column_type_mismatch_raises():
    metadata = MetaData()
    source = Table("source", metadata, Column("id", String(10)))
    target = Table("target", metadata, Column("id", Integer))

    with pytest.raises(ValueError, match="column type mismatches"):
        _prevalidate_upsert_from_table(source, target, ["id"], {}, False)


def test_temporal_delete_resets_session_timestamp_after_error(monkeypatch):
    timestamps = []
    ops = DataframeOps(object())
    monkeypatch.setattr(
        "polars_hist_db.core.dataframe.DbOps.set_system_versioning_time",
        lambda self, value: timestamps.append(value),
    )
    monkeypatch.setattr(
        ops,
        "table_delete_rows",
        lambda *args: (_ for _ in ()).throw(RuntimeError("delete failed")),
    )
    update_time = datetime(2026, 1, 1, tzinfo=UTC)

    with pytest.raises(RuntimeError, match="delete failed"):
        ops.table_delete_rows_temporal(
            pl.DataFrame({"id": [1]}), "sample", "items", update_time
        )

    assert timestamps == [update_time, None]


def test_temporal_upsert_resets_session_timestamp_after_error(monkeypatch):
    timestamps = []
    ops = DeltaTableOps("sample", "delta", DeltaConfig(), object())
    monkeypatch.setattr(
        "polars_hist_db.core.delta_table.DbOps.set_system_versioning_time",
        lambda self, value: timestamps.append(value),
    )
    monkeypatch.setattr(
        "polars_hist_db.core.delta_table.TableOps.get_table_metadata",
        lambda self: (_ for _ in ()).throw(RuntimeError("upsert failed")),
    )
    update_time = datetime(2026, 1, 1, tzinfo=UTC)

    with pytest.raises(RuntimeError, match="upsert failed"):
        ops.upsert("items", update_time)

    assert timestamps == [update_time, None]


def test_latest_audit_entry_keeps_utc_schema_when_empty(monkeypatch):
    metadata = MetaData()
    audit_table = Table(
        "__audit_log",
        metadata,
        Column("audit_id", Integer),
        Column("table_name", String),
        Column("data_source_type", String),
        Column("data_source", String),
        Column("data_source_ts", DateTime),
        Column("upload_ts", DateTime),
    )
    empty = pl.DataFrame(
        schema={
            "audit_id": pl.String,
            "table_name": pl.String,
            "data_source_type": pl.String,
            "data_source": pl.String,
            "data_source_ts": pl.Datetime("us"),
            "upload_ts": pl.Datetime("us"),
        }
    )
    monkeypatch.setattr(AuditOps, "create", lambda self, connection: audit_table)
    monkeypatch.setattr(
        DataframeOps, "from_selectable", lambda self, query: empty.clone()
    )

    result = AuditOps("sample").get_latest_entry(object())

    assert result.schema["data_source_ts"] == pl.Datetime("us", "UTC")
    assert result.schema["upload_ts"] == pl.Datetime("us", "UTC")
