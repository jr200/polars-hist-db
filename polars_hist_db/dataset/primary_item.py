import logging
from datetime import datetime
from typing import Any

import polars as pl
from sqlalchemy import Connection, Table, func, or_, select

from ..config import DatasetConfig, TableConfig, TableConfigs
from ..core import DeltaTableOps, TableConfigOps, TableOps
from ..pipeline_projection import valid_time_source_columns

LOGGER = logging.getLogger(__name__)


def _xtdb_temporal_write_kwargs(
    table_config: TableConfig,
    valid_time: Any,
    upload_time: datetime,
    staging: Any,
) -> dict[str, Any]:
    if table_config.is_temporal or valid_time is not None:
        return {"update_time": upload_time}
    if hasattr(staging, "bulk_dataframes"):
        return {"dataframe_ops": staging.bulk_dataframes()}
    return {}


def _validate_sql_valid_time_source_table(
    connection: Connection,
    source_table: Table,
    source_columns: list[str],
) -> None:
    missing_columns = [
        column for column in source_columns if column not in source_table.c
    ]
    if missing_columns:
        raise ValueError(
            "valid-time mapping references missing source column(s): "
            + ", ".join(missing_columns)
        )

    if not source_columns:
        return

    null_predicates = [source_table.c[column].is_(None) for column in source_columns]
    null_count = connection.execute(
        select(func.count()).select_from(source_table).where(or_(*null_predicates))
    ).scalar_one()
    if null_count:
        raise ValueError(
            "valid-time mapping references null source value(s): "
            + ", ".join(source_columns)
        )


def scrape_primary_item(
    pipeline_id: int,
    dataset: DatasetConfig,
    tables: TableConfigs,
    upload_time: datetime,
    connection: Connection,
    partition_df: pl.DataFrame | None = None,
    stage_run_id: str | None = None,
    staging: Any = None,
    backend: Any = None,
) -> bool:
    pipeline = dataset.pipeline
    delta_table_schema = dataset.delta_table_schema
    delta_table_name = dataset.name
    main_table_config = tables[pipeline.get_main_table_name()[1]]
    LOGGER.debug("(item %d) scraping item %s", pipeline_id, main_table_config.name)

    if staging is not None:
        return scrape_xtdb_pipeline_item(
            pipeline_id,
            dataset,
            main_table_config,
            upload_time,
            connection,
            stage_run_id,
            staging,
            backend,
        )

    TableConfigOps(connection).create(main_table_config)
    tbo = TableOps(delta_table_schema, delta_table_name, connection)
    source_tbl = tbo.get_table_metadata()

    upload_items = pipeline.extract_items(pipeline_id)
    selected_columns = [item.source for item in upload_items]
    src_tgt_colname_map = {item.source: item.target for item in upload_items}
    valid_time = dataset.valid_time_for_table(
        main_table_config.schema, main_table_config.name
    )
    _validate_sql_valid_time_source_table(
        connection,
        source_tbl,
        valid_time_source_columns(src_tgt_colname_map, valid_time),
    )

    common_columns = [c.name for c in tbo.get_column_intersection(selected_columns)]

    if selected_columns is not None and len(common_columns) != len(selected_columns):
        cols_not_configured = set(common_columns).symmetric_difference(selected_columns)
        raise ValueError(
            f"column mismatch on {cols_not_configured} in {selected_columns}"
        )

    ni, nu, nd = DeltaTableOps(
        delta_table_schema, delta_table_name, dataset.delta_config, connection
    ).upsert(
        main_table_config.name,
        upload_time,
        is_main_table=True,
        source_columns=common_columns,
        src_tgt_colname_map=src_tgt_colname_map,
    )

    LOGGER.debug("(item %d) upserted %d rows", -1, ni + nu)

    # TODO: trigger table mod notification
    return (ni + nu + nd) > 0


def scrape_xtdb_pipeline_item(
    pipeline_id: int,
    dataset: DatasetConfig,
    table_config: TableConfig,
    upload_time: datetime,
    connection: Connection,
    stage_run_id: str | None,
    staging: Any,
    backend: Any,
) -> bool:
    if stage_run_id is None or staging is None:
        raise ValueError("XTDB ingest requires a staged partition")

    valid_time = dataset.valid_time_for_table(table_config.schema, table_config.name)
    target_df = staging.prepare_pipeline_item_dataframe(
        stage_run_id,
        dataset,
        pipeline_id,
        table_config,
        valid_time=valid_time,
    )

    temporal_write_kwargs = _xtdb_temporal_write_kwargs(
        table_config,
        valid_time,
        upload_time,
        staging,
    )
    modified_count = backend.temporal_upsert(
        target_df,
        table_config.schema,
        table_config.name,
        connection=connection,
        table_config=table_config,
        delta_config=dataset.delta_config,
        valid_time=valid_time,
        **temporal_write_kwargs,
    )

    LOGGER.debug("(item %d) XTDB upserted %d rows", pipeline_id, modified_count)
    return modified_count > 0
