# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
import pytest
import polars as pl

from polars_db_engine.core import AuditOps
from polars_db_engine.loaders import find_files
from tests.utils import create_temp_file_tree, setup_fixture_tableconfigs


@pytest.fixture(scope="session", autouse=True)
def fixture_with_tmpdir():
    tmpDir = create_temp_file_tree(10, 3, 10)
    yield tmpDir
    tmpDir.cleanup()


@pytest.fixture(scope="session", autouse=True)
def fixture_with_table():
    yield from setup_fixture_tableconfigs("simple_nontemporal_table.yaml")


def test_find_files(fixture_with_tmpdir):
    source_tz_name = "US/Pacific"
    target_tz_name = "UTC"

    root_path = fixture_with_tmpdir.name
    t = datetime.fromisoformat("2023-01-01T12:00:01")

    search_spec_df = pl.from_records(
        [
            {
                "root_path": root_path,
                "file_include": ["*.log", "*.bin"],
                "timestamp": {
                    "method": "manual",
                    "source_tz": source_tz_name,
                    "target_tz": target_tz_name,
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
    assert ["path", "created_at"] == df.columns
    assert df["created_at"].unique()[0] == t + timedelta(hours=8)
    assert df.filter(pl.col("path").str.ends_with("bin")).shape[0] > 0
    assert df.filter(pl.col("path").str.ends_with("log")).shape[0] > 0


def test_loader(fixture_with_table):
    engine, _table_configs, table_schema = fixture_with_table
    app_name = "myapp"

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
