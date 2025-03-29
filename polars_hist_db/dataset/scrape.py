from datetime import datetime
import logging
from pathlib import Path
from time import sleep

import polars as pl
from sqlalchemy import Connection, Engine

from ..config import TableConfig, TableConfigs, DatasetConfig
from ..core import AuditOps, DataframeOps
from ..loaders import load_typed_dsv
from ..utils import NonRetryableException

from .extract_item import scrape_extract_item
from .primary_item import scrape_primary_item

LOGGER = logging.getLogger(__name__)


def _scrape_pipeline_item(
    dataset: DatasetConfig,
    target_table: str,
    tables: TableConfigs,
    upload_time: datetime,
    connection: Connection,
) -> None:
    item_type = dataset.pipeline.item_type(target_table)
    if item_type == "primary":
        scrape_primary_item(dataset, tables, upload_time, connection)
    elif item_type == "extract":
        scrape_extract_item(dataset, target_table, tables, upload_time, connection)
    else:
        raise ValueError(f"unknown item type: {item_type}")


def scrape_pipeline_as_transaction(
    csv_file: Path,
    csv_file_time: datetime,
    dataset: DatasetConfig,
    tables: TableConfigs,
    engine: Engine,
    num_retries: int = 3,
    seconds_between_retries: float = 60,
):
    pipeline = dataset.pipeline
    main_table_config: TableConfig = tables[pipeline.get_main_table_name()]
    table_schema = main_table_config.schema
    aops = AuditOps(table_schema)
    assert main_table_config.delta_config is not None

    df = load_typed_dsv(csv_file, tables.column_definitions.column_definitions)
    df = DataframeOps.populate_nulls(df, main_table_config)

    LOGGER.debug("loaded %d rows", len(df))

    if main_table_config.delta_config.time_partition:
        tp = main_table_config.delta_config.time_partition
        time_col = tp.column
        interval = tp.truncate
        unique_strategy = tp.unique_strategy

        partitions = (
            df.with_columns(__interval=pl.col(time_col).dt.truncate(interval))
            .sort(time_col)
            .unique(
                [*main_table_config.primary_keys, "__interval"],
                keep=unique_strategy,
                maintain_order=True,
            )
            .partition_by(
                "__interval", include_key=False, as_dict=True, maintain_order=True
            )
        )
    else:
        partitions = {(csv_file_time,): df}

    while num_retries > 0:
        with engine.connect() as connection:
            try:
                with connection.begin():
                    for i, ((ts,), partition_df) in enumerate(partitions.items()):
                        assert isinstance(ts, datetime)
                        LOGGER.info(
                            "-- processing time_partition (%d/%d) %s",
                            i + 1,
                            len(partitions),
                            ts.isoformat(),
                        )

                        DataframeOps(connection).table_insert(
                            partition_df,
                            dataset.delta_table_schema,
                            dataset.name,
                            uniqueness_col_set=main_table_config.primary_keys,
                            prefill_nulls_with_default=True,
                            clear_table_first=True,
                        )

                        for target_table in pipeline.items["table"]:
                            _scrape_pipeline_item(
                                dataset,
                                target_table,
                                tables,
                                ts,
                                connection,
                            )

                    aops.add_entry(
                        "file",
                        csv_file,
                        main_table_config.name,
                        connection,
                        csv_file_time,
                    )

                    return

            except NonRetryableException as e:
                LOGGER.error("non-retryable exception %s", e)
                connection.rollback()
                raise

            except Exception as e:
                LOGGER.error("error in scrape_pipeline_as_transaction: %s", e)

                connection.rollback()
                if num_retries == 0:
                    raise

                sleep(seconds_between_retries)
                LOGGER.info("retries remaining: %d", num_retries)
                num_retries -= 1
