import logging
from collections.abc import Mapping
from datetime import datetime
from typing import Any

import polars as pl
from sqlalchemy import Connection

from ..config import DatasetConfig, TableConfig, TableConfigs
from ..core import DeltaTableOps, TableConfigOps, TableOps
from ..utils import NonRetryableException
from .foreign_key_helper import deduce_foreign_keys
from .primary_item import scrape_xtdb_pipeline_item

LOGGER = logging.getLogger(__name__)


def scrape_extract_item(
    pipeline_id: int,
    dataset: DatasetConfig,
    target_table: str,
    tables: TableConfigs,
    upload_time: datetime,
    connection: Connection,
    partition_df: pl.DataFrame | None = None,
    stage_run_id: str | None = None,
    staging: Any = None,
    backend: Any = None,
) -> bool:
    pipeline = dataset.pipeline
    delta_table_name = dataset.name

    target_table_config: TableConfig = tables[target_table]
    if staging is not None:
        return scrape_xtdb_pipeline_item(
            pipeline_id,
            dataset,
            target_table_config,
            upload_time,
            connection,
            stage_run_id,
            staging,
            backend,
        )

    if target_table_config.is_temporal:
        raise NotImplementedError("temporal tables are not supported yet")

    LOGGER.debug("(item %d) extracting item %s", pipeline_id, target_table_config.name)

    TableConfigOps(connection).create(target_table_config)

    col_info = pipeline.extract_items(pipeline_id)
    required_cols = [column.source for column in col_info if column.required]

    # these case can pass through
    # require columns are all present in source data
    # required cols not in source data and either:
    # - have default value
    # - have values implied from other columns

    tbo = TableOps(target_table_config.schema, delta_table_name, connection)
    found_required_cols = tbo.get_column_intersection(required_cols)

    if len(required_cols) != len(found_required_cols):
        err = f"skipping extract. required columns {required_cols} not found in table {delta_table_name}."
        raise NonRetryableException(err)

    deduce_foreign_keys(
        dataset.delta_table_schema,
        delta_table_name,
        target_table_config,
        col_info,
        connection,
    )

    found_source_cols = [
        str(c.name)
        for c in tbo.get_column_intersection([column.source for column in col_info])
    ]

    col_map_dict: Mapping[str, str] = {
        column.source: column.target
        for column in col_info
        if column.source in found_source_cols
    }

    ni, nu, nd = DeltaTableOps(
        dataset.delta_table_schema,
        delta_table_name,
        dataset.delta_config,
        connection,
    ).upsert(
        target_table_config.name,
        upload_time,
        is_main_table=False,
        source_columns=found_source_cols,
        src_tgt_colname_map=col_map_dict,
    )

    LOGGER.debug("(item %d) upserted %d rows", pipeline_id, ni + nu + nd)

    return (ni + nu + nd) > 0
