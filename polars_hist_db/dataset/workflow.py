import logging
from pathlib import Path
import time

from sqlalchemy import Engine

from ..config import (
    Config,
    DatasetConfig,
    ColumnDefinitions,
    TableConfigs,
)
from ..core import AuditOps, DeltaTableOps, TableConfigOps, TableOps
from ..loaders import find_files
from ..utils import Clock
from .scrape import scrape_pipeline_as_transaction

LOGGER = logging.getLogger(__name__)


def run_workflows(config: Config, engine: Engine):
    for dataset in config.datasets.datasets:
        LOGGER.info("scraping dataset %s", dataset.name)

        _run_workflow(dataset, config.tables.column_definitions, config.tables, engine)


def _run_workflow(
    dataset: DatasetConfig,
    column_definitions: ColumnDefinitions,
    tables: TableConfigs,
    engine: Engine,
):
    table_name = dataset.pipeline.get_main_table_name()
    table_config = tables[table_name]
    table_schema = table_config.schema
    aops = AuditOps(table_schema)

    LOGGER.info(f"starting ingest for {table_name}")

    scrape_limit = dataset.scrape_limit
    csv_files_df = find_files(dataset.search_paths)

    with engine.begin() as connection:
        csv_files_df = aops.filter_unprocessed_items(
            csv_files_df, "path", table_name, connection
        ).sort("created_at")

        if scrape_limit is not None:
            csv_files_df = csv_files_df.head(scrape_limit)

        aops.prevalidate_new_items(table_name, csv_files_df, connection)

    with engine.begin() as connection:
        TableConfigOps(connection).create_all(tables)

    if table_config.delta_config is not None:
        with engine.begin() as connection:
            delta_table_config = DeltaTableOps(
                dataset.delta_table_schema,
                dataset.name,
                table_config.delta_config,
                connection,
            ).table_config(column_definitions)

            if not TableOps(
                delta_table_config.schema, delta_table_config.name, connection
            ).table_exists():
                TableConfigOps(connection).create(
                    delta_table_config,
                    is_delta_table=True,
                    is_temporary_table=True,
                )

    timings = Clock()

    for i, (csv_file, file_time) in enumerate(csv_files_df.rows()):
        LOGGER.info(
            "[%d/%d] processing file mtime=%s", i + 1, len(csv_files_df), file_time
        )

        start_time = time.perf_counter()

        scrape_pipeline_as_transaction(
            Path(csv_file), file_time, dataset, tables, engine
        )

        pipeline_time = time.perf_counter() - start_time
        timings.add_timing("pipeline", pipeline_time)
        LOGGER.debug("avg pipeline time %f seconds", timings.get_avg("pipeline"))
        LOGGER.debug("eta: %s", str(timings.eta("pipeline", len(csv_files_df) - i - 1)))

    LOGGER.info("stopped scrape - %s", table_name)
