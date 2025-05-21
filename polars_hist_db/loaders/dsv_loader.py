import logging
from pathlib import Path
from types import MappingProxyType
from typing import Optional, Mapping, Sequence, Tuple, Union

import polars as pl

from ..config.parser_config import ParserColumnConfig
from ..config.fn_registry import FunctionRegistry
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


def _apply_header_transforms(
    df: pl.DataFrame, col_def: ParserColumnConfig
) -> pl.DataFrame:
    if not col_def.transforms:
        return df

    if col_def.target is None:
        raise ValueError(f"missing target column for {col_def}")

    if col_def.target not in df.columns:
        fn_reg = FunctionRegistry()
        for fn_name, fn_args in col_def.transforms.items():
            if fn_args is None:
                continue

            source_col = col_def.target if col_def.source is None else col_def.source
            df = fn_reg.call_function(fn_name, df, source_col, col_def.target, fn_args)

    result_col_dtype = PolarsType.from_sql(col_def.data_type)
    df = df.with_columns(pl.col(col_def.target).cast(result_col_dtype))

    return df


def _get_column_dtype(
    column_name: str, column_configs: Sequence[ParserColumnConfig]
) -> pl.DataType:
    for cfg in column_configs:
        if cfg.source == column_name:
            return PolarsType.from_sql(cfg.data_type)

    raise ValueError("bad configuration. missing column definition for {column_name}")


def load_typed_dsv(
    file_or_bytes: Union[Path, bytes],
    column_configs: Sequence[ParserColumnConfig],
    schema_overrides: Mapping[str, pl.DataType] = MappingProxyType({}),
    delimiter: Optional[str] = None,
) -> pl.DataFrame:
    LOGGER.info("loading csv %s", str(file_or_bytes))

    sep, headers = _parse_header_row(file_or_bytes, delimiter)

    def _is_forced_dtype(dtype: pl.DataType) -> bool:
        return (
            dtype.is_temporal() or dtype.is_decimal() or dtype in {pl.String, pl.Utf8}
        )

    header_configs = [
        cfg
        for cfg in column_configs
        if cfg.source and cfg.column_type in ["data", "dsv_only"]
    ]

    header_schema: Mapping[str, pl.DataType] = {
        cfg.source: PolarsType.from_sql(cfg.data_type)
        for cfg in header_configs
        if cfg.source
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

    agg_headers = {
        cfg.source
        for cfg in column_configs
        if cfg.aggregation is not None and cfg.source
    }
    if agg_headers:
        dsv_df = dsv_df.group_by(pl.exclude(agg_headers), maintain_order=True).agg(
            pl.sum(c) for c in agg_headers.intersection(dsv_df.columns)
        )

    expected_headers = [c.source for c in header_configs if c.source]

    headers_no_config = (
        set(dsv_df.columns)
        .difference(expected_headers)
        .difference(
            [
                c.target
                for c in column_configs
                if c.column_type == "time_partition_only" or c.source is None
            ]
        )
        .difference(schema_overrides.keys())
    )

    if len(headers_no_config) > 0:
        headers_no_config_str = "],[".join(headers_no_config)
        LOGGER.warning(f"dsv-headers skipped/unknown [{headers_no_config_str}]")
        dsv_df = dsv_df.drop(headers_no_config)

    defined_but_missing_headers = list(set(expected_headers).difference(dsv_df.columns))

    if len(defined_but_missing_headers) > 0:
        assert isinstance(defined_but_missing_headers, list)
        LOGGER.warning(
            "added %s defined headers that were not in dsv: %s",
            len(defined_but_missing_headers),
            ",".join(defined_but_missing_headers),
        )

        missing_columns_df = pl.DataFrame()
        missing_columns_df = missing_columns_df.with_columns(
            [
                pl.lit(None).cast(_get_column_dtype(h, header_configs)).alias(h)
                for h in defined_but_missing_headers
            ]
        ).clear()

        LOGGER.debug(missing_columns_df)

    # # drop headers only used in temporary calc
    df = dsv_df.drop(
        [
            col_cfg.source
            for col_cfg in column_configs
            if col_cfg.column_type == "dsv_only" and col_cfg.source in dsv_df.columns
        ]
    )

    # df = df.with_columns(
    #     pl.col(col_cfg.source).cast(PolarsType.from_sql(col_cfg.data_type))
    #     for col_cfg in column_configs
    #     if col_cfg.source in dsv_df.columns
    # ).pipe(PolarsType.cast_str_to_cat)

    return df
