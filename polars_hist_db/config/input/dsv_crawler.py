from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union
import os
import logging
from datetime import datetime
import polars as pl
from io import StringIO
import csv

from .input_source import InputConfig

LOGGER = logging.getLogger(__name__)


@dataclass
class DsvCrawlerInputConfig(InputConfig):
    search_paths: Optional[Union[pl.DataFrame, List[Dict[str, Any]]]] = None
    payload: Optional[str] = None
    payload_time: Optional[datetime] = None

    @staticmethod
    def clean_dsv_string(data: str) -> str:
        input_io = StringIO(data.strip())
        output_io = StringIO()

        reader = csv.reader(input_io)
        writer = csv.writer(output_io, quoting=csv.QUOTE_MINIMAL)

        for row in reader:
            cleaned_row = [field.strip() if field is not None else "" for field in row]
            writer.writerow(cleaned_row)

        csv_output = output_io.getvalue().strip() + "\n"
        return csv_output

    def set_payload(self, payload: str, payload_time: datetime):
        self.payload = DsvCrawlerInputConfig.clean_dsv_string(payload)
        self.payload_time = payload_time

    def has_payload(self) -> bool:
        return self.payload is not None and self.payload_time is not None

    def __post_init__(self):
        if self.search_paths and not isinstance(self.search_paths, pl.DataFrame):
            for search_path in self.search_paths:
                if "root_path" in search_path:
                    path = search_path["root_path"]
                    if not os.path.isabs(path):
                        if self.config_file_path is None:
                            LOGGER.warning(
                                "No config_file_path provided, using current working directory as base for relative path"
                            )
                            base_path = os.getcwd()
                        else:
                            base_path = os.path.dirname(
                                os.path.abspath(self.config_file_path)
                            )
                        abs_path = os.path.normpath(os.path.join(base_path, path))
                        search_path["root_path"] = abs_path

            self.search_paths = pl.from_records(self.search_paths)
