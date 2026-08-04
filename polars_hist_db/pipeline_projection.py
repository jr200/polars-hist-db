import logging
from collections.abc import Sequence
from typing import Any

import polars as pl

from .config import PipelineExtractColumn, TableConfig, ValidTimeConfig
from .types import PolarsType

LOGGER = logging.getLogger(__name__)


def _omit_inapplicable_rows(
    stage_df: pl.DataFrame,
    upload_items: Sequence[PipelineExtractColumn],
) -> pl.DataFrame:
    sources = [item.source for item in upload_items if item.omit_row_if_null]
    if not sources:
        return stage_df

    missing = sorted(set(sources).difference(stage_df.columns))
    if missing:
        raise ValueError(
            "omit-row mapping references missing source column(s): "
            + ", ".join(missing)
        )

    result = stage_df.filter(
        pl.all_horizontal(pl.col(source).is_not_null() for source in sources)
    )
    omitted = stage_df.height - result.height
    if omitted:
        LOGGER.warning(
            "omitted %d pipeline row(s) because %s contained null",
            omitted,
            ", ".join(sources),
        )
    return result


def _include_valid_time_source_columns(
    selected_columns: list[str],
    src_tgt_colname_map: dict[str, str],
    valid_time: ValidTimeConfig | None,
    stage_df: pl.DataFrame,
) -> list[str]:
    result = list(selected_columns)
    if valid_time is None:
        return result

    required_source_columns = valid_time_source_columns(src_tgt_colname_map, valid_time)
    for source_column in required_source_columns:
        if source_column not in result:
            result.append(source_column)

    missing_columns = [
        source for source in required_source_columns if source not in stage_df.columns
    ]
    if missing_columns:
        raise ValueError(
            "valid-time mapping references missing source column(s): "
            + ", ".join(missing_columns)
        )

    null_columns = [
        source
        for source in required_source_columns
        if stage_df[source].null_count() > 0
    ]
    if null_columns:
        raise ValueError(
            "valid-time mapping references null source value(s): "
            + ", ".join(null_columns)
        )

    return result


def valid_time_source_columns(
    src_tgt_colname_map: dict[str, str],
    valid_time: ValidTimeConfig | None,
) -> list[str]:
    if valid_time is None:
        return []

    target_to_source = {
        target: source for source, target in src_tgt_colname_map.items()
    }
    result = []
    for valid_time_column in [valid_time.from_column, valid_time.to_column]:
        if valid_time_column is None:
            continue
        result.append(target_to_source.get(valid_time_column, valid_time_column))
    return result


def project_staged_pipeline_item_dataframe(
    stage_df: pl.DataFrame,
    dataset: Any,
    pipeline_id: int,
    table_config: TableConfig,
    valid_time: ValidTimeConfig | None,
) -> pl.DataFrame:
    upload_items = dataset.pipeline.extract_items(pipeline_id)
    stage_df = _omit_inapplicable_rows(stage_df, upload_items)
    src_tgt_colname_map = {item.source: item.target for item in upload_items}
    selected_columns = [item.source for item in upload_items]
    selected_columns = _include_valid_time_source_columns(
        selected_columns,
        src_tgt_colname_map,
        valid_time,
        stage_df,
    )

    missing_columns = sorted(set(selected_columns).difference(stage_df.columns))
    if missing_columns:
        nullable_target_columns = {
            column.name: column
            for column in table_config.columns
            if column.nullable and not column.autoincrement
        }
        fill_columns = []
        rejected_columns = []
        for source_column in missing_columns:
            target_column_name = src_tgt_colname_map[source_column]
            target_column = nullable_target_columns.get(target_column_name)
            if target_column is None:
                rejected_columns.append(source_column)
                continue

            fill_columns.append(
                pl.lit(None)
                .cast(PolarsType.from_sql(target_column.data_type))
                .alias(source_column)
            )

        if rejected_columns:
            raise ValueError(
                f"column mismatch on {set(rejected_columns)} in {selected_columns}"
            )

        stage_df = stage_df.with_columns(fill_columns)

    return stage_df.select(selected_columns).rename(
        {
            source: target
            for source, target in src_tgt_colname_map.items()
            if source in selected_columns and source != target
        }
    )
