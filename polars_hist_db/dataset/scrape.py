from datetime import datetime
import logging
from time import sleep
from typing import Awaitable, Callable, List, Tuple

import polars as pl
from sqlalchemy import Connection, Engine

from ..config import TableConfig, TableConfigs, DatasetConfig
from ..core import DataframeOps
from ..utils import NonRetryableException

from .extract_item import scrape_extract_item
from .primary_item import scrape_primary_item

LOGGER = logging.getLogger(__name__)


def _scrape_pipeline_item(
    pipeline_id: int,
    dataset: DatasetConfig,
    target_table: str,
    tables: TableConfigs,
    upload_time: datetime,
    connection: Connection,
) -> None:
    item_type = dataset.pipeline.item_type(target_table)
    if item_type == "primary":
        scrape_primary_item(pipeline_id, dataset, tables, upload_time, connection)
    elif item_type == "extract":
        scrape_extract_item(
            pipeline_id, dataset, target_table, tables, upload_time, connection
        )
    else:
        raise ValueError(f"unknown item type: {item_type}")


async def try_run_pipeline_as_transaction(
    partitions: List[Tuple[datetime, pl.DataFrame]],
    dataset: DatasetConfig,
    tables: TableConfigs,
    engine: Engine,
    commit_fn: Callable[[Connection], Awaitable[bool]],
    num_retries: int = 3,
    seconds_between_retries: float = 60,
):
    main_table_config: TableConfig = tables[dataset.pipeline.get_main_table_name()[1]]
    tbl_to_header_map = dataset.pipeline.get_header_map(main_table_config.name)
    header_keys = [tbl_to_header_map.get(k, k) for k in main_table_config.primary_keys]

    while num_retries > 0:
        with engine.connect() as connection:
            try:
                with connection.begin():
                    for i, (ts, partition_df) in enumerate(partitions):
                        assert isinstance(ts, datetime), (
                            f"timestamp is not a datetime [{type(ts)}]"
                        )
                        LOGGER.info(
                            "-- (%d/%d) time_partition[%s] %d rows",
                            i + 1,
                            len(partitions),
                            ts.isoformat(),
                            len(partition_df),
                        )

                        DataframeOps(connection).table_insert(
                            partition_df,
                            dataset.delta_table_schema,
                            dataset.name,
                            uniqueness_col_set=header_keys,
                            prefill_nulls_with_default=True,
                            clear_table_first=True,
                        )

                        for (
                            pipeline_id,
                            target_table,
                        ) in dataset.pipeline.get_pipeline_items().items():
                            _scrape_pipeline_item(
                                pipeline_id,
                                dataset,
                                target_table,
                                tables,
                                ts,
                                connection,
                            )

                    success = await commit_fn(connection)
                    if success:
                        return

            except NonRetryableException as e:
                LOGGER.error("non-retryable exception %s", e)
                connection.rollback()
                raise

            except Exception as e:
                LOGGER.error("error in scrape_pipeline_as_transaction", exc_info=e)

                connection.rollback()
                if num_retries == 0:
                    raise

                sleep(seconds_between_retries)
                LOGGER.info("retries remaining: %d", num_retries)
                num_retries -= 1
