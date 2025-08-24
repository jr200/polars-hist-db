# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
import pytest
import polars as pl
import pytz

from polars_hist_db.core import AuditOps
from polars_hist_db.loaders import find_files
from ..utils.dsv_helper import create_temp_file_tree, setup_fixture_dataset


@pytest.fixture(scope="session", autouse=True)
def fixture_with_tmpdir():
    tmpDir = create_temp_file_tree(10, 3, 10)
    yield tmpDir
    tmpDir.cleanup()


@pytest.fixture(scope="session", autouse=True)
def fixture_with_table():
    yield from setup_fixture_dataset("simple_nontemporal.yaml")


def test_find_files(fixture_with_tmpdir):
    source_tz = pytz.timezone("US/Pacific")

    root_path = fixture_with_tmpdir.name
    t = datetime.fromisoformat("2023-01-01T12:00:01")

    search_spec_df = pl.from_records(
        [
            {
                "root_path": root_path,
                "file_include": ["*.log", "*.bin"],
                "timestamp": {
                    "method": "manual",
                    "source_tz": str(source_tz),
                    "datetime": t,
                },
                "is_enabled": True,
                "max_depth": 4,
                "dir_include": [],
                "dir_exclude": [],
                "file_exclude": [],
            },
        ]
    )

    df = find_files(search_spec_df)

    assert not df.is_empty()
    assert ["__path", "__created_at"] == df.columns
    assert df["__created_at"].first() == source_tz.localize(t).astimezone(pytz.utc)
    assert df.filter(pl.col("__path").str.ends_with("bin")).shape[0] > 0
    assert df.filter(pl.col("__path").str.ends_with("log")).shape[0] > 0


def test_loader(fixture_with_table):
    engine, config = fixture_with_table
    app_name = "myapp"
    table_schema = config.tables.schemas()[0]

    aops = AuditOps(table_schema)
    with engine.connect() as connection:
        audit_tbl = aops.create(connection)

    aops_name = audit_tbl.name
    assert f"{table_schema}.{aops_name}" == aops.fqtn()

    with engine.connect() as connection:
        num_deleted = aops.purge(app_name, connection)
        assert num_deleted == 0

    with engine.connect() as connection:
        aops.drop(connection)


def test_latest_entry(fixture_with_table):
    engine, config = fixture_with_table
    table_name = "myapp"
    table_schema = config.tables.schemas()[0]

    aops = AuditOps(table_schema)
    with engine.connect() as connection:
        empty_audit_df = aops.get_latest_entry(connection)
        assert empty_audit_df.is_empty()

        audit_df_schema = empty_audit_df.schema
        assert len(audit_df_schema) == 6
        assert audit_df_schema["audit_id"] == pl.Int32
        assert audit_df_schema["table_name"] == pl.Categorical
        assert audit_df_schema["data_source_type"] == pl.Categorical
        assert audit_df_schema["data_source"] == pl.Categorical
        assert audit_df_schema["data_source_ts"] == pl.Datetime("us", "UTC")
        assert audit_df_schema["upload_ts"] == pl.Datetime("us", "UTC")

        ts = [
            datetime(2022, 1, 1, 3, 4, 5, tzinfo=pytz.utc) + timedelta(days=365 * i)
            for i in range(5)
        ]
        aops.add_entry("dsv", "file_1.csv", table_name, connection, ts[0])
        aops.add_entry("dsv", "file_2.csv", table_name, connection, ts[1])
        aops.add_entry("dsv", "file_3.csv", table_name, connection, ts[2])
        aops.add_entry("dsv", "file_4.csv", table_name, connection, ts[3])
        aops.add_entry("dsv", "file_5.csv", table_name, connection, ts[4])

        # audit_tbl = aops.create(connection)
        # cursor = connection.execute(audit_tbl.select())
        # for row in cursor:
        #     print(row)

        audit_df = aops.get_latest_entry(
            connection,
            target_table_name=table_name,
            data_source_type="dsv",
            asof_timestamp=ts[2],
        )
        assert len(audit_df) == 1
        assert audit_df["data_source_ts"].first().astimezone(pytz.utc) == ts[2]

        audit_df = aops.get_latest_entry(
            connection,
            target_table_name=table_name,
            data_source_type="dsv",
            asof_timestamp=ts[3],
        )
        assert len(audit_df) == 1
        assert audit_df["data_source_ts"].first().astimezone(pytz.utc) == ts[3]

        audit_df = aops.get_latest_entry(
            connection,
            target_table_name=table_name,
            data_source_type="dsv",
            asof_timestamp=ts[4],
        )
        assert len(audit_df) == 1
        assert audit_df["data_source_ts"].first().astimezone(pytz.utc) == ts[4]
