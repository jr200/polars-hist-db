from datetime import datetime
from typing import Any, Mapping, Iterable, List
import logging
import os
import re

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
        datetime(
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
    file_include: List[str],
    timestamp: Mapping[str, Any],
    is_enabled: bool,
    max_depth: int = 4,
    dir_include: Iterable[str] = (),
    dir_exclude: Iterable[str] = (),
    file_exclude: Iterable[str] = (),
) -> pl.DataFrame:
    LOGGER.info("searching files %s in %s", file_include, root_path)

    return_schema: Mapping[str, pl.PolarsDataType] = {
        "__path": pl.Utf8,
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

    entries = list(sd)

    df = pl.DataFrame({"root_path": root_path, "entry": entries}).filter(
        pl.col("entry").map_elements(
            lambda x: x.is_file, skip_nulls=True, return_dtype=pl.Boolean
        )
    )

    if df.is_empty():
        return pl.DataFrame(schema=return_schema)

    df = df.with_columns(
        __path=pl.concat_str(
            [
                pl.lit(root_path),
                pl.col("entry").map_elements(lambda x: x.path, return_dtype=pl.Utf8),
            ],
            separator=os.sep,
        ),
        mtime=pl.col("entry").map_elements(
            lambda x: (
                x.st_mtime
                if isinstance(x.st_mtime, datetime)
                else source_tz.localize(datetime.fromtimestamp(x.st_mtime))
            ).astimezone(target_tz),
            skip_nulls=True,
            return_dtype=pl.Datetime("us", "UTC"),
        ),
    ).with_columns(
        __path=pl.col("__path").map_elements(
            lambda x: os.path.normpath(str(x)), skip_nulls=True, return_dtype=pl.Utf8
        )
    )

    if tz_method == "regex":
        datetime_regex = timestamp["datetime_regex"]
        df = df.with_columns(
            __created_at=pl.col("__path").map_elements(
                lambda x: _parse_time(x, datetime_regex, source_tz, target_tz),
                skip_nulls=True,
                return_dtype=pl.Datetime("us", "UTC"),
            )
        )

    elif tz_method == "manual":
        dt_value = source_tz.localize(timestamp["datetime"]).astimezone(target_tz)
        df = df.with_columns(__created_at=pl.lit(dt_value))

    elif tz_method == "mtime":
        df = df.with_columns(__created_at=pl.col("mtime"))

    else:
        raise ValueError(f"unknown tz_method: '{tz_method}'")

    df = df.select(return_schema.keys()).sort("__created_at")

    return df
