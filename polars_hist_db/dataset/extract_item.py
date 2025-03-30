from datetime import datetime
import logging
from typing import Mapping

import polars as pl
from sqlalchemy import Connection

from .foreign_key_helper import deduce_foreign_keys

from ..config import TableConfig, TableConfigs, DatasetConfig
from ..core import TableConfigOps, DeltaTableOps, TableOps
from ..utils import NonRetryableException


LOGGER = logging.getLogger(__name__)


def scrape_extract_item(
    dataset: DatasetConfig,
    target_table: str,
    tables: TableConfigs,
    upload_time: datetime,
    connection: Connection,
):
    pipeline = dataset.pipeline
    delta_table_name = dataset.name
    main_table_config = tables[pipeline.get_main_table_name()]
    assert main_table_config.delta_config is not None

    target_table_config: TableConfig = tables[target_table]
    if target_table_config.is_temporal:
        raise NotImplementedError("temporal tables are not supported yet")

    LOGGER.debug("(item %d) extracting item %s", -1, target_table_config.name)

    TableConfigOps(connection).create(target_table_config)

    extract_spec = pipeline.extract_items(target_table)
    main_table_cols_df = main_table_config.columns_df()
    col_info = extract_spec.join(
        main_table_cols_df, how="left", left_on="source", right_on="name"
    ).select("source", "target", "deduce_foreign_key", "required")

    required_cols = col_info.filter("required")["source"].to_list()

    # these case can pass through
    # require columns are all present in source data
    # required cols not in source data and either:
    # - have default value
    # - have values implied from other columns

    tbo = TableOps(main_table_config.schema, delta_table_name, connection)
    found_required_cols = tbo.get_column_intersection(required_cols)

    if len(required_cols) != len(found_required_cols):
        err = f"skipping extract. required columns {required_cols} not found in table {delta_table_name}."
        raise NonRetryableException(err)

    found_source_cols = [
        str(c.name) for c in tbo.get_column_intersection(col_info["source"].to_list())
    ]

    deduce_foreign_keys(
        main_table_config.schema,
        delta_table_name,
        target_table_config,
        col_info,
        connection,
    )

    col_map_dict: Mapping[str, str] = {
        src: tgt
        for src, tgt in col_info.filter(pl.col("source").is_in(found_source_cols))
        .select("source", "target")
        .iter_rows()
    }

    ni, nu, nd = DeltaTableOps(
        target_table_config.schema,
        delta_table_name,
        target_table_config.delta_config,
        connection,
    ).upsert(
        target_table_config.name,
        upload_time,
        source_columns=found_source_cols,
        src_tgt_colname_map=col_map_dict,
    )

    LOGGER.debug("(item %d) upserted %d rows", -1, ni + nu + nd)

    # TODO: trigger table mod notification
