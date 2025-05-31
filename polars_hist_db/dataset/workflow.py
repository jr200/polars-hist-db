from datetime import datetime
import logging
import time
from typing import Dict, Optional, Tuple

import polars as pl
from sqlalchemy import Engine

from ..loaders.input_source_factory import InputSourceFactory
from ..utils.clock import Clock

from ..config import Config, DatasetConfig, TableConfigs, TableConfig
from ..config.input_source import InputConfig
from ..core import DeltaTableOps, TableConfigOps, TableOps
from .scrape import try_upload

LOGGER = logging.getLogger(__name__)


async def run_workflows(
    config: Config,
    engine: Engine,
    dataset_name: Optional[str] = None,
    debug_capture_output: Optional[Dict[Tuple[datetime,], pl.DataFrame]] = None,
):
    for dataset in config.datasets.datasets:
        if dataset_name is None or dataset.name == dataset_name:
            LOGGER.info("scraping dataset %s", dataset.name)
            await _run_workflow(
                dataset.input_config,
                dataset,
                config.tables,
                engine,
                debug_capture_output,
            )


def _create_delta_table(
    engine: Engine,
    tables: TableConfigs,
    dataset: DatasetConfig,
    table_config: TableConfig,
):
    with engine.begin() as connection:
        TableConfigOps(connection).create_all(tables)

    if table_config.delta_config is not None:
        col_defs = dataset.pipeline.build_delta_table_column_configs(
            tables, dataset.name
        )
        with engine.begin() as connection:
            delta_table_config = DeltaTableOps(
                dataset.delta_table_schema,
                dataset.name,
                table_config.delta_config,
                connection,
            ).table_config(col_defs)

            if not TableOps(
                delta_table_config.schema, delta_table_config.name, connection
            ).table_exists():
                TableConfigOps(connection).create(
                    delta_table_config,
                    is_delta_table=True,
                    is_temporary_table=False,
                )


async def _run_workflow(
    input_config: InputConfig,
    dataset: DatasetConfig,
    tables: TableConfigs,
    engine: Engine,
    debug_capture_output: Optional[Dict[Tuple[datetime,], pl.DataFrame]],
):
    table_name = dataset.pipeline.get_main_table_name()
    table_config = tables[table_name]

    LOGGER.info(f"starting ingest for {table_name}")

    _create_delta_table(engine, tables, dataset, table_config)

    start_time = time.perf_counter()

    input_source = InputSourceFactory.create_input_source(tables, dataset, input_config)
    try:
        async for partitions, commit_fn in await input_source.next_df(engine):
            if debug_capture_output is not None:
                debug_capture_output.update(partitions)

            await try_upload(partitions, dataset, tables, engine, commit_fn)

    except Exception as e:
        LOGGER.error("error while processing InputSource: %s", e, exc_info=e)
    finally:
        await input_source.cleanup()

    Clock().add_timing("workflow", time.perf_counter() - start_time)

    LOGGER.info("stopped scrape - %s", table_name)
