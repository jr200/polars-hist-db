from datetime import datetime
import logging
from pathlib import Path
import time
from typing import AsyncGenerator, Awaitable, Callable, Dict, Tuple

import polars as pl
from sqlalchemy import Connection, Engine

from ..config.dataset import DatasetConfig
from ..config.input_source import DSVInputConfig
from ..config.table import TableConfig, TableConfigs
from ..core.audit import AuditOps
from ..core.dataframe import DataframeOps
from .dsv.dsv_loader import load_typed_dsv
from .dsv.file_search import find_files
from .input_source import InputSource
from ..utils.clock import Clock

LOGGER = logging.getLogger(__name__)


class DsvInputSource(InputSource):
    def __init__(self, config: DSVInputConfig):
        self.config = config

    def _scrape_file(
        self,
        csv_file: Path,
        csv_file_time: datetime,
        dataset: DatasetConfig,
        tables: TableConfigs,
    ):
        pipeline = dataset.pipeline
        main_table_config: TableConfig = tables[pipeline.get_main_table_name()]
        tbl_to_header_map = pipeline.get_header_map(main_table_config.name)
        header_keys = [tbl_to_header_map[k] for k in main_table_config.primary_keys]

        assert main_table_config.delta_config is not None

        column_definitions = dataset.pipeline.build_ingestion_column_definitions(tables)

        df = load_typed_dsv(
            csv_file, column_definitions, null_values=dataset.null_values
        )

        missing_values_map = {
            c.source: c.value_if_missing
            for c in column_definitions
            if c.value_if_missing and c.source
        }
        df = DataframeOps.populate_nulls(df, missing_values_map)

        LOGGER.debug("loaded %d rows", len(df))

        if dataset.time_partition:
            tp = dataset.time_partition
            time_col = tp.column
            interval = tp.truncate
            unique_strategy = tp.unique_strategy

            partitions = (
                df.with_columns(
                    __interval=pl.col(time_col).dt.truncate(interval).cast(pl.Datetime)
                )
                .sort(time_col)
                .unique(
                    [*header_keys, "__interval"],
                    keep=unique_strategy,
                    maintain_order=True,
                )
                .partition_by(
                    "__interval", include_key=False, as_dict=True, maintain_order=True
                )
            )
        else:
            partitions = {(csv_file_time,): df}

        return partitions

    async def next_df(
        self,
        dataset: DatasetConfig,
        tables: TableConfigs,
        engine: Engine,
        scrape_limit: int = -1,
    ) -> AsyncGenerator[
        Tuple[
            Dict[Tuple[datetime], pl.DataFrame], Callable[[Connection], Awaitable[bool]]
        ],
        None,
    ]:
        async def _generator() -> AsyncGenerator[
            Tuple[
                Dict[Tuple[datetime], pl.DataFrame],
                Callable[[Connection], Awaitable[bool]],
            ],
            None,
        ]:
            table_name = dataset.pipeline.get_main_table_name()
            table_config = tables[table_name]
            table_schema = table_config.schema

            assert isinstance(self.config.search_paths, pl.DataFrame)
            csv_files_df = find_files(self.config.search_paths)

            aops = AuditOps(table_schema)

            with engine.begin() as connection:
                csv_files_df = aops.filter_unprocessed_items(
                    csv_files_df, "path", table_name, connection
                ).sort("created_at")

                if scrape_limit > 0:
                    csv_files_df = csv_files_df.head(scrape_limit)

                aops.prevalidate_new_items(table_name, csv_files_df, connection)

            timings = Clock()

            for i, (csv_file, file_time) in enumerate(csv_files_df.rows()):
                LOGGER.info(
                    "[%d/%d] processing file mtime=%s",
                    i + 1,
                    len(csv_files_df),
                    file_time,
                )

                start_time = time.perf_counter()

                partitions = self._scrape_file(
                    Path(csv_file), file_time, dataset, tables
                )

                async def commit_fn(connection: Connection) -> bool:
                    result: bool = aops.add_entry(
                        "dsv",
                        Path(csv_file).absolute().as_posix(),
                        table_name,
                        connection,
                        file_time,
                    )
                    return result

                yield partitions, commit_fn

                pipeline_time = time.perf_counter() - start_time
                timings.add_timing("pipeline", pipeline_time)
                LOGGER.debug(
                    "avg pipeline time %f seconds", timings.get_avg("pipeline")
                )
                LOGGER.debug(
                    "eta: %s", str(timings.eta("pipeline", len(csv_files_df) - i - 1))
                )

            LOGGER.info("stopped scrape - %s", table_name)

        return _generator()
