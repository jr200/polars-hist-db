import asyncio
import logging
import time
from collections.abc import AsyncGenerator
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import polars as pl
from sqlalchemy import Connection, Engine

from polars_hist_db.utils.exceptions import NonRetryableException

from ..config.dataset import DatasetConfig
from ..config.input.dsv_crawler import DsvCrawlerInputConfig
from ..config.table import TableConfigs
from ..core.audit import AuditOps
from ..utils.clock import Clock
from .dsv.dsv_loader import load_typed_dsv
from .dsv.file_search import find_files
from .input_source import BatchFinalizer, InputSource
from .transform import apply_transformations

LOGGER = logging.getLogger(__name__)


def _make_dsv_finalizer(
    data_source: str,
    bound_file_time: Any,
) -> BatchFinalizer:
    def write_audit_before_commit(
        connection: Connection,
        modified_tables: list[tuple[str, str]],
    ) -> bool:
        result = True
        for modified_schema, modified_table in modified_tables:
            aops = AuditOps(modified_schema)
            result = aops.add_entry(
                "dsv",
                data_source,
                modified_table,
                connection,
                bound_file_time,
            )

            if not result:
                LOGGER.error(
                    "audit for [%s.%s - %s]: FAILED",
                    modified_schema,
                    modified_table,
                    data_source,
                )
                raise NonRetryableException("Failed to update audit log")

        return result

    return BatchFinalizer(write_audit_before_commit)


def _make_dsv_file_finalizer(csv_file: str, file_time: Any) -> BatchFinalizer:
    return _make_dsv_finalizer(Path(csv_file).absolute().as_posix(), file_time)


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
        self, payload: Path | bytes, payload_time: datetime
    ) -> list[tuple[datetime, pl.DataFrame]]:
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
        tuple[
            list[tuple[datetime, pl.DataFrame]],
            BatchFinalizer,
        ],
        None,
    ]:
        async def _generator() -> AsyncGenerator[
            tuple[
                list[tuple[datetime, pl.DataFrame]],
                BatchFinalizer,
            ],
            None,
        ]:
            table_schema, table_name = self.dataset.pipeline.get_main_table_name()

            if self.config.has_payload():
                assert isinstance(self.config.payload, str)
                assert self.config.payload_time is not None

                payload = bytes(self.config.payload, "UTF8")
                data_source = f"payload:{sha256(payload).hexdigest()}"
                candidates = pl.DataFrame(
                    {
                        "__path": [data_source],
                        "__created_at": [self.config.payload_time],
                    }
                )
                filtered = await asyncio.to_thread(
                    self._search_and_filter_files,
                    candidates,
                    table_schema,
                    table_name,
                    engine,
                )
                if filtered.is_empty():
                    return
                yield (
                    await asyncio.to_thread(
                        self._process_payload, payload, self.config.payload_time
                    ),
                    (_make_dsv_finalizer(data_source, self.config.payload_time)),
                )
                return

            # Handle file-based case
            if self.config.search_paths is None:
                raise ValueError("Either payload or search_paths must be provided")

            csv_files_df = await asyncio.to_thread(self.files)
            csv_files_df = await asyncio.to_thread(
                self._search_and_filter_files,
                csv_files_df,
                table_schema,
                table_name,
                engine,
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

                partitions = await asyncio.to_thread(
                    self._process_payload, Path(csv_file), file_time
                )

                yield partitions, _make_dsv_file_finalizer(csv_file, file_time)

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
