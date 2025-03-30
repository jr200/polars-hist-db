import logging
from pathlib import Path
from types import MappingProxyType
from typing import Optional, Mapping, Sequence, Tuple, Union

import polars as pl

from ..config import ColumnConfig
from .fn_registry import FunctionRegistry
from ..types import PolarsType

LOGGER = logging.getLogger(__name__)


def _parse_header_row(
    input: Union[Path, bytes], known_delimiter: Optional[str]
) -> Tuple[str, Sequence[str]]:
    delimiters = [known_delimiter] if known_delimiter else [",", ";", "|", ":"]

    failed_guess_errors = []
    for delimiter in delimiters:
        try:
            df = pl.read_csv(
                input,
                has_header=True,
                infer_schema_length=0,
                n_rows=0,
                n_threads=1,
                separator=delimiter,
            )

            if known_delimiter or df.shape[1] > 1:
                return delimiter, df.columns

        except Exception as e:
            failed_guess_errors.append(e)
            continue

    LOGGER.error(failed_guess_errors)
    raise ValueError(f"couldn't infer delimiter of dsv {str(input)}")


def _apply_header_transforms(df: pl.DataFrame, col_def: ColumnConfig) -> pl.DataFrame:
    fn_reg = FunctionRegistry()
    for fn_name, fn_args in col_def.transforms.items():
        if col_def.name not in df.columns:
            df = fn_reg.call_function(fn_name, df, [col_def.header, *fn_args])

    return df


def _get_column_type(
    column_name: str, column_configs: Sequence[ColumnConfig]
) -> pl.DataType:
    for cfg in column_configs:
        if cfg.header == column_name:
            return PolarsType.from_sql(cfg.data_type)

    raise ValueError("bad configuration. missing column definition for {column_name}")


def load_typed_dsv(
    file_or_bytes: Union[Path, bytes],
    column_configs: Sequence[ColumnConfig],
    schema_overrides: Mapping[str, pl.DataType] = MappingProxyType({}),
    delimiter: Optional[str] = None,
) -> pl.DataFrame:
    LOGGER.info("loading csv %s", str(file_or_bytes))

    sep, headers = _parse_header_row(file_or_bytes, delimiter)

    def _is_forced_dtype(dtype: pl.DataType) -> bool:
        return (
            dtype.is_temporal() or dtype.is_decimal() or dtype in {pl.String, pl.Utf8}
        )

    header_schema = {
        cfg.header: PolarsType.from_sql(cfg.data_type)
        for cfg in column_configs
        if cfg.header
    }

    forced_schema = {
        header: dtype
        for header, dtype in {**header_schema, **schema_overrides}.items()
        if _is_forced_dtype(dtype) and header in headers
    }

    dsv_df = (
        pl.read_csv(
            file_or_bytes,
            separator=sep,
            has_header=True,
            schema_overrides=forced_schema,
            null_values=["", "None"],
        )
        .drop("", strict=False)
        .unique(maintain_order=True)
    )

    dsv_df.columns = [c.strip() for c in dsv_df.columns]

    for col_cfg in column_configs:
        dsv_df = dsv_df.pipe(_apply_header_transforms, col_cfg)

    dsv_df = dsv_df.pipe(
        PolarsType.apply_schema_to_dataframe, **header_schema, **schema_overrides
    )

    agg_headers = {cfg.header for cfg in column_configs if cfg.aggregation is not None}
    if agg_headers:
        dsv_df = dsv_df.group_by(pl.exclude(agg_headers), maintain_order=True).agg(
            pl.sum(c) for c in agg_headers.intersection(dsv_df.columns)
        )

    expected_headers: Mapping[str, str] = {
        col_def.header: col_def.name
        for col_def in column_configs
        if not col_def.header or not col_def.deduce_foreign_key
    }

    undefined_headers = (
        set(dsv_df.columns)
        .difference(expected_headers.keys())
        .difference(schema_overrides.keys())
    )

    if len(undefined_headers) > 0:
        undefined_headers_str = "],[".join(undefined_headers)
        raise ValueError(f"missing header definitions for [{undefined_headers_str}]")

    defined_but_missing_headers = set(expected_headers.keys()).difference(
        dsv_df.columns
    )
    if len(defined_but_missing_headers) > 0:
        LOGGER.info(
            "added %d defined headers that were not in dsv: %s",
            len(defined_but_missing_headers),
            ",".join(defined_but_missing_headers),
        )

        missing_columns_df = pl.DataFrame().with_columns(
            pl.lit(None)
            .cast(_get_column_type(h, column_configs))
            .alias(expected_headers[h])
            for h in defined_but_missing_headers
        )

        LOGGER.debug(missing_columns_df)

    header_to_db_col_map = {
        dsv_header: db_col_name
        for dsv_header, db_col_name in expected_headers.items()
        if dsv_header in dsv_df.columns
    }

    df = dsv_df.rename(header_to_db_col_map)
    df = df.with_columns(
        pl.col(col_cfg.name).cast(PolarsType.from_sql(col_cfg.data_type))
        for col_cfg in column_configs
        if col_cfg.name in df.columns
    ).pipe(PolarsType.cast_str_to_cat)

    return df
