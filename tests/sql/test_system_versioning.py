from datetime import datetime, timezone

import pytest

from polars_hist_db.core import DbOps
from tests.utils import setup_fixture_tableconfigs


@pytest.fixture
def fixture_with_simple_table_1():
    yield from setup_fixture_tableconfigs("simple_table.yaml")


@pytest.fixture
def fixture_with_simple_table_2():
    yield from setup_fixture_tableconfigs("simple_table.yaml")


def test_long_write(fixture_with_simple_table_1, fixture_with_simple_table_2):
    engine_1, _table_configs_1, _table_schema_1 = fixture_with_simple_table_1
    engine_2, _table_configs_2, _table_schema_2 = fixture_with_simple_table_2

    # upload then test initial df
    ts_1 = datetime.fromisoformat("1985-01-01T00:00:01Z")

    # set the system_versioning time to ts_1 on engine_1
    with engine_1.begin() as connection:
        DbOps(connection).set_system_versioning_time(ts_1)
        t = DbOps(connection).get_system_versioning_time()
        assert t == ts_1

    # verify the system_versioning time persists between connections on engine_1
    with engine_1.begin() as connection:
        t = DbOps(connection).get_system_versioning_time()
        assert t == ts_1

    # verify the system_versioning time is not affected on engine_2
    with engine_2.begin() as connection:
        t = DbOps(connection).get_system_versioning_time()
        assert t.date() == datetime.now(tz=timezone.utc).date()

    # set the engine_1 time back to 'now'
    with engine_1.begin() as connection:
        DbOps(connection).set_system_versioning_time(None)
        t = DbOps(connection).get_system_versioning_time()
        assert t.date() == datetime.now(tz=timezone.utc).date()

    # verify the system_versioning time for engine_2 is still unaffected
    with engine_2.begin() as connection:
        t = DbOps(connection).get_system_versioning_time()
        assert t.date() == datetime.now(tz=timezone.utc).date()
