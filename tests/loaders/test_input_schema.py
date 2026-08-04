from typing import Literal

import polars as pl
import pytest

from polars_hist_db.config.parser_config import IngestionColumnConfig
from polars_hist_db.loaders.transform import (
    InputSchemaError,
    apply_transformations,
    enforce_input_schema,
)


def _column(
    source: str,
    data_type: str,
    *,
    column_type: Literal["data", "input_only"] = "data",
    nullable: bool = True,
) -> IngestionColumnConfig:
    return IngestionColumnConfig(
        column_type=column_type,
        schema="sample",
        table="items" if column_type == "data" else None,
        ingestion_data_type=data_type,
        target_data_type=data_type,
        source=source,
        target=source if column_type == "data" else None,
        nullable=nullable,
    )


def test_input_schema_types_nulls_and_drops_declared_input_only_columns() -> None:
    columns = [
        _column("id", "INT", nullable=False),
        _column("note", "VARCHAR(32)"),
        _column("unused", "VARCHAR(32)", column_type="input_only"),
    ]
    first_raw = pl.DataFrame(
        {"id": [1], "unused": [None]},
        schema={"id": pl.Int64, "unused": pl.Null},
    )
    second_raw = pl.DataFrame({"id": [2], "note": ["ready"], "unused": ["x"]})

    typed = pl.concat(
        [
            enforce_input_schema(first_raw, columns),
            enforce_input_schema(second_raw, columns),
        ]
    )
    transformed = apply_transformations(typed, columns)

    assert typed.schema == {
        "id": pl.Int32,
        "note": pl.String,
        "unused": pl.String,
    }
    assert typed.get_column("note").to_list() == [None, "ready"]
    assert transformed.columns == ["id", "note"]


def test_input_schema_preserves_declared_array_input() -> None:
    columns = [_column("coordinates", "ARRAY(DOUBLE)", column_type="input_only")]

    typed = enforce_input_schema(pl.DataFrame({"coordinates": [[1.5, 2.5]]}), columns)

    assert typed.schema == {"coordinates": pl.List(pl.Float64)}
    assert apply_transformations(typed, columns).is_empty()


def test_input_schema_accepts_only_null_structural_ancestors() -> None:
    columns = [_column("payload.child", "VARCHAR(32)")]

    typed = enforce_input_schema(
        pl.DataFrame({"payload": [None]}, schema={"payload": pl.Null}), columns
    )

    assert typed.schema == {"payload.child": pl.String}
    with pytest.raises(InputSchemaError, match="undeclared input columns: payload"):
        enforce_input_schema(pl.DataFrame({"payload": ["bad"]}), columns)
    with pytest.raises(InputSchemaError, match="undeclared input columns: other"):
        enforce_input_schema(
            pl.DataFrame({"other": [None]}, schema={"other": pl.Null}), columns
        )


def test_input_schema_rejects_unknown_missing_null_and_conflicting_columns() -> None:
    required = _column("id", "INT", nullable=False)

    with pytest.raises(InputSchemaError, match="undeclared input columns: surprise"):
        enforce_input_schema(pl.DataFrame({"id": [1], "surprise": [2]}), [required])

    with pytest.raises(InputSchemaError, match="missing required input column: id"):
        enforce_input_schema(pl.DataFrame(), [required])

    with pytest.raises(InputSchemaError, match="nulls in required input column: id"):
        enforce_input_schema(
            pl.DataFrame({"id": [None]}, schema={"id": pl.Null}), [required]
        )

    with pytest.raises(InputSchemaError, match="conflicting ingestion types"):
        enforce_input_schema(
            pl.DataFrame({"id": [1]}),
            [required, _column("id", "VARCHAR(8)")],
        )
