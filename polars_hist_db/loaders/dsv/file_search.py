import logging
import os
import re
from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import Any

import polars as pl
import pytz
from scandir_rs import Scandir

LOGGER = logging.getLogger(__name__)

TzInfo = pytz.tzinfo.BaseTzInfo


def _parse_time(path, pattern: str, src_tz: TzInfo, target_tz: TzInfo) -> datetime:
    m = re.match(pattern, path)
    if m is None:
        raise ValueError(f"failed to parse timestamp from file {path}")

    d = {k: int(v) for k, v in m.groupdict().items()}
    src_dt = src_tz.localize(
        datetime(  # noqa: DTZ001 -- pytz requires localizing a naive datetime
            d["y"],
            d["m"],
            d["d"],
            d.get("H", 0),
            d.get("M", 0),
            d.get("S", 0),
            d.get("u", 0),
        )
    )
    target_dt: datetime = src_dt.astimezone(target_tz)
    return target_dt


def find_files(search_paths: pl.DataFrame) -> pl.DataFrame:
    files: pl.DataFrame = (
        pl.concat(
            [
                _find_files_with_timestamps(**search_path)
                for search_path in search_paths.iter_rows(named=True)
            ]
        )
        .unique()
        .sort("__created_at")
    )

    if len(files) > 0:
        LOGGER.info("found total %d files", files.shape[0])

    return files


def _find_files_with_timestamps(
    root_path: str,
    file_include: list[str],
    timestamp: Mapping[str, Any],
    is_enabled: bool,
    max_depth: int = 4,
    dir_include: Iterable[str] = (),
    dir_exclude: Iterable[str] = (),
    file_exclude: Iterable[str] = (),
) -> pl.DataFrame:
    LOGGER.info("searching files %s in %s", file_include, root_path)

    return_schema: Mapping[str, pl.DataType] = {
        "__path": pl.Utf8(),
        "__created_at": pl.Datetime("us", "UTC"),
    }

    if not is_enabled:
        return pl.DataFrame(schema=return_schema)

    target_tz = pytz.utc
    source_tz = pytz.timezone(timestamp.get("source_tz", str(target_tz)))
    tz_method = timestamp.get("method")

    sd = Scandir(
        root_path=root_path,
        dir_include=dir_include,
        dir_exclude=dir_exclude,
        file_include=file_include,
        file_exclude=file_exclude,
        max_depth=max_depth,
    )

    if tz_method not in {"regex", "manual", "mtime"}:
        raise ValueError(f"unknown tz_method: '{tz_method}'")

    manual_time = (
        source_tz.localize(timestamp["datetime"]).astimezone(target_tz)
        if tz_method == "manual"
        else None
    )
    rows = []
    for entry in sd:
        if not entry.is_file:
            continue
        path = os.path.normpath(os.path.join(root_path, entry.path))
        mtime = (
            entry.st_mtime
            if isinstance(entry.st_mtime, datetime)
            else datetime.fromtimestamp(entry.st_mtime, tz=source_tz)
        ).astimezone(target_tz)
        created_at = (
            _parse_time(path, timestamp["datetime_regex"], source_tz, target_tz)
            if tz_method == "regex"
            else manual_time or mtime
        )
        rows.append({"__path": path, "__created_at": created_at})

    return pl.DataFrame(rows, schema=return_schema).sort("__created_at")
