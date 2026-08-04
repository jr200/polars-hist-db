from datetime import UTC, datetime, timezone
from types import SimpleNamespace

import polars as pl
import pytest

from polars_hist_db.config import (
    TableColumnConfig,
    TableConfig,
    ValidTimeConfig,
)
from polars_hist_db.config.dataset import Pipeline
from polars_hist_db.pipeline_projection import project_staged_pipeline_item_dataframe


def _table_config() -> TableConfig:
    return TableConfig(
        schema="source",
        name="events",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("events", "id", "INT", nullable=False),
            TableColumnConfig("events", "event_value", "VARCHAR(64)"),
        ],
    )


def _dataset() -> SimpleNamespace:
    return SimpleNamespace(
        pipeline=Pipeline(
            [
                {
                    "schema": "source",
                    "table": "events",
                    "type": "primary",
                    "columns": [
                        {"source": "id", "target": "id"},
                        {"source": "event_value", "target": "event_value"},
                    ],
                }
            ]
        )
    )


def test_valid_time_source_columns_are_projected_from_staging_data():
    event_time = datetime(2030, 1, 1, tzinfo=UTC)

    result = project_staged_pipeline_item_dataframe(
        pl.DataFrame(
            {
                "id": [1],
                "event_value": ["Alpha"],
                "event_timestamp": [event_time],
            }
        ),
        _dataset(),
        0,
        _table_config(),
        ValidTimeConfig(table="events", from_column="event_timestamp"),
    )

    assert result.to_dict(as_series=False) == {
        "id": [1],
        "event_value": ["Alpha"],
        "event_timestamp": [event_time],
    }


@pytest.mark.parametrize("event_timestamp", [None])
def test_valid_time_source_columns_are_required_and_non_null(event_timestamp):
    with pytest.raises(ValueError, match="null source value.*event_timestamp"):
        project_staged_pipeline_item_dataframe(
            pl.DataFrame(
                {
                    "id": [1],
                    "event_value": ["Alpha"],
                    "event_timestamp": [event_timestamp],
                }
            ),
            _dataset(),
            0,
            _table_config(),
            ValidTimeConfig(table="events", from_column="event_timestamp"),
        )


def test_missing_valid_time_source_column_is_rejected_before_backend_write():
    with pytest.raises(ValueError, match="missing source column.*event_timestamp"):
        project_staged_pipeline_item_dataframe(
            pl.DataFrame(
                {
                    "id": [1],
                    "event_value": ["Alpha"],
                }
            ),
            _dataset(),
            0,
            _table_config(),
            ValidTimeConfig(table="events", from_column="event_timestamp"),
        )
