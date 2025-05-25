import glob
import os
from typing import Mapping
from zipfile import ZipFile

import polars as pl


def read_zipfile(filename: str, schema: Mapping[str, pl.DataType]) -> pl.DataFrame:
    meta_schema = {
        "zip_file": pl.Categorical,
        "csv_file": pl.Categorical,
        "in_memory_size": pl.Float64,
    }

    dfs = []
    info = []

    zf = ZipFile(filename)
    for csv_file in zf.namelist():
        df = pl.read_csv(
            zf.open(csv_file).read(), schema_overrides=schema
        ).with_columns(pl.col(pl.Utf8).cast(pl.Categorical))

        info.append([filename, csv_file, df.estimated_size("mb")])
        dfs.append(df)

    metadata = pl.from_records(info, orient="row", schema=meta_schema)
    print(metadata)
    if len(dfs) > 0:
        result = pl.concat(dfs, how="vertical")
    else:
        result = pl.DataFrame()

    return result


def convert_single_zipped_csv_to_parquet(
    filename: str, schema: Mapping[str, pl.DataType], remove_original: bool
) -> None:
    df = read_zipfile(filename, schema)
    parquet_filename = filename.removesuffix(".zip") + ".parquet"
    df.write_parquet(parquet_filename)

    if remove_original:
        os.remove(filename)


def convert_zipped_csvs_to_parquet(
    folder: str, schema: Mapping[str, pl.DataType], remove_original: bool
) -> None:
    for filename in glob.glob(os.path.join(folder, "*.zip")):
        convert_single_zipped_csv_to_parquet(filename, schema, remove_original)
