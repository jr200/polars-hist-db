from datetime import UTC, datetime

import polars as pl
import pytest

from polars_hist_db.overrides import ColumnOverride, apply_columnar_overrides


def test_applies_overrides_columnarly_and_preserves_schema() -> None:
    frame = pl.DataFrame(
        {
            "trade_id": range(150),
            "status": ["Expected"] * 150,
            "amount": [1.0] * 150,
            "aware_at": [datetime(2026, 7, 1, tzinfo=UTC)] * 150,
            "naive_at": [datetime.fromisoformat("2026-07-01")] * 150,
        }
    )
    original_schema = frame.schema

    result = apply_columnar_overrides(
        frame,
        entity_column="trade_id",
        overrides=[
            ColumnOverride("149", "status", "Possible"),
            ColumnOverride("149", "status", "Actual"),
            ColumnOverride("149", "amount", 3.5),
            ColumnOverride("149", "aware_at", datetime(2026, 7, 2, tzinfo=UTC)),
            ColumnOverride("149", "naive_at", datetime.fromisoformat("2026-07-02")),
            ColumnOverride("149", "classification", "Actual", pl.String()),
            ColumnOverride("0", "amount", None),
        ],
    )

    assert result.select(original_schema.names()).schema == original_schema
    assert result["status"][149] == "Actual"
    assert result["amount"][0] is None
    assert result["amount"][149] == 3.5
    assert result["aware_at"][149] == datetime(2026, 7, 2, tzinfo=UTC)
    assert result["naive_at"][149] == datetime.fromisoformat("2026-07-02")
    assert result["classification"][0] is None
    assert result["classification"][149] == "Actual"


def test_requires_type_for_all_null_override_only_column() -> None:
    with pytest.raises(ValueError, match="dtype is required"):
        apply_columnar_overrides(
            pl.DataFrame({"trade_id": [1]}),
            entity_column="trade_id",
            overrides=[ColumnOverride("1", "comment", None)],
        )
