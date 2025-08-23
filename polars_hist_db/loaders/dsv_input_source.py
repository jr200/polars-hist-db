from datetime import datetime
import logging
from pathlib import Path
import time
from typing import AsyncGenerator, Awaitable, Callable, List, Tuple, Union

import polars as pl
from sqlalchemy import Connection, Engine

from polars_hist_db.utils.exceptions import NonRetryableException

from .transform import apply_transformations

from ..config.dataset import DatasetConfig
from ..config.input.dsv_crawler import DsvCrawlerInputConfig
from ..config.table import TableConfigs
from ..core.audit import AuditOps
from .dsv.dsv_loader import load_typed_dsv
from .dsv.file_search import find_files
from .input_source import InputSource
from ..utils.clock import Clock

LOGGER = logging.getLogger(__name__)


class DsvCrawlerInputSource(InputSource[DsvCrawlerInputConfig]):
    def __init__(
        self,
        tables: TableConfigs,
        dataset: DatasetConfig,
        config: DsvCrawlerInputConfig,
    ):
        super().__init__(tables, dataset, config)

    async def cleanup(self) -> None:
        pass

    def files(self) -> pl.DataFrame:
        assert isinstance(self.config.search_paths, pl.DataFrame)
        csv_files_df = find_files(self.config.search_paths)
        return csv_files_df

    def _process_payload(
        self, payload: Union[Path, bytes], payload_time: datetime
    ) -> List[Tuple[datetime, pl.DataFrame]]:
        df = load_typed_dsv(
            payload, self.column_definitions, null_values=self.dataset.null_values
        )
        LOGGER.debug("loaded %d rows", len(df))

        df = apply_transformations(df, self.column_definitions)
        partitions = self._apply_time_partitioning(df, payload_time)

        return partitions

    async def next_df(
        self, engine: Engine
    ) -> AsyncGenerator[
        Tuple[
            List[Tuple[datetime, pl.DataFrame]], Callable[[Connection], Awaitable[bool]]
        ],
        None,
    ]:
        async def _generator() -> AsyncGenerator[
            Tuple[
                List[Tuple[datetime, pl.DataFrame]],
                Callable[[Connection], Awaitable[bool]],
            ],
            None,
        ]:
            table_schema, table_name = self.dataset.pipeline.get_main_table_name()

            if self.config.has_payload():
                assert isinstance(self.config.payload, str)
                assert self.config.payload_time is not None

                partitions = self._process_payload(
                    bytes(self.config.payload, "UTF8"), self.config.payload_time
                )

                async def commit_fn(connection: Connection) -> bool:
                    # payload was send directly to the pipeline, rather than a filepath
                    # difficult generate an audit id in this case
                    # for now, these messages are not audited - but might need to require
                    # an audit_id field to be provided
                    return True

                yield partitions, commit_fn
                return

            # Handle file-based case
            if self.config.search_paths is None:
                raise ValueError("Either payload or search_paths must be provided")

            csv_files_df = self._search_and_filter_files(
                self.files(), table_schema, table_name, engine
            )

            timings = Clock()

            for i, (csv_file, file_time) in enumerate(csv_files_df.rows()):
                LOGGER.info(
                    "[%d/%d] processing file mtime=%s",
                    i + 1,
                    len(csv_files_df),
                    file_time,
                )

                start_time = time.perf_counter()

                partitions = self._process_payload(Path(csv_file), file_time)

                async def commit_fn(connection: Connection) -> bool:
                    aops = AuditOps(table_schema)
                    path = Path(csv_file).absolute()
                    result: bool = aops.add_entry(
                        "dsv",
                        path.as_posix(),
                        table_name,
                        connection,
                        file_time,
                    )

                    if not result:
                        LOGGER.error(
                            "audit for [%s.%s - %s]: FAILED",
                            table_schema,
                            table_name,
                            path.name,
                        )
                        raise NonRetryableException("Failed to update audit log")

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
