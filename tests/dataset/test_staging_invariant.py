"""Stage/delta table must carry no rows once run_datasets returns."""

from datetime import datetime

import pytest
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from polars_hist_db.dataset import run_datasets

from ..utils.dsv_helper import backend_params, setup_fixture_dataset

pytestmark = [
    pytest.mark.integration,
    pytest.mark.parametrize(
        "fixture_simple",
        backend_params(),
        indirect=True,
    ),
]


@pytest.fixture
def fixture_simple(request):
    yield from setup_fixture_dataset("simple_nontemporal.yaml", request.param)


def _stage_row_count(engine, dataset, backend_name: str) -> int:
    schema = dataset.delta_table_schema
    if backend_name == "xtdb":
        sql = (
            f"SELECT COUNT(*) FROM {schema}.__{dataset.name}_stream_stage "
            "FOR ALL SYSTEM_TIME"
        )
    else:
        sql = f"SELECT COUNT(*) FROM {schema}.{dataset.name}"

    with engine.connect() as connection:
        try:
            result = connection.execute(text(sql)).scalar()
        except SQLAlchemyError:
            return 0
    return int(result or 0)


@pytest.mark.asyncio
async def test_run_datasets_leaves_stage_table_empty(fixture_simple, request):
    engine, base_config = fixture_simple
    dataset = base_config.datasets.datasets[0]
    backend_name = request.node.callspec.params["fixture_simple"]

    ts = datetime.fromisoformat("2025-01-01T00:00:00Z")
    dsv = "id,double_col,varchar_col\n1,1.5,alpha\n2,2.5,beta\n"

    dataset.input_config.set_payload(dsv, ts)
    await run_datasets(base_config, engine, raise_on_error=True)

    assert _stage_row_count(engine, dataset, backend_name) == 0
