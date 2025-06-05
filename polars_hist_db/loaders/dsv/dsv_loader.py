import logging
from pathlib import Path
from types import MappingProxyType
from typing import Optional, Mapping, Sequence, Tuple, Union

import polars as pl


from ...config.parser_config import IngestionColumnConfig
from ..transform import header_configs
from ...types import PolarsType

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


def _get_column_target_dtype(
    column_name: str, column_configs: Sequence[IngestionColumnConfig]
) -> pl.DataType:
    for cfg in column_configs:
        if cfg.source == column_name:
            return PolarsType.from_sql(cfg.target_data_type)

    raise ValueError("bad configuration. missing column definition for {column_name}")


def load_typed_dsv(
    file_or_bytes: Union[Path, bytes],
    column_configs: Sequence[IngestionColumnConfig],
    schema_overrides: Mapping[str, pl.DataType] = MappingProxyType({}),
    delimiter: Optional[str] = None,
    null_values: Optional[Sequence[str]] = None,
) -> pl.DataFrame:
    if isinstance(file_or_bytes, Path):
        LOGGER.info("loading csv from path %s", str(file_or_bytes))
    else:
        LOGGER.info("loading csv from string (%d bytes)", len(file_or_bytes))

    sep, headers = _parse_header_row(file_or_bytes, delimiter)

    def _is_forced_dtype(dtype: pl.DataType) -> bool:
        return (
            dtype.is_temporal() or dtype.is_decimal() or dtype in {pl.String, pl.Utf8}
        )

    headers_ingestion_schema: Mapping[str, pl.DataType] = {
        cfg.source: PolarsType.from_sql(cfg.ingestion_data_type)
        for cfg in header_configs(column_configs)
        if cfg.source
    }

    forced_schema = {
        header: dtype
        for header, dtype in {**headers_ingestion_schema, **schema_overrides}.items()
        if _is_forced_dtype(dtype) and header in headers
    }

    if null_values is None:
        null_values = ["", "None"]

    valid_col_configs = set()
    valid_col_configs |= {c.source for c in column_configs if c.source}
    valid_col_configs |= {c.target for c in column_configs if c.target}
    source_cols: Sequence[str] = list(
        {h for h in headers if h in valid_col_configs or h.startswith("__")}
    )

    dsv_df = (
        pl.read_csv(
            file_or_bytes,
            columns=source_cols,
            separator=sep,
            has_header=True,
            schema_overrides=forced_schema,
            null_values=null_values,
        )
        .drop("", strict=False)
        .unique(maintain_order=True)
    )

    dsv_df.columns = [c.strip() for c in dsv_df.columns]

    dsv_df = _validate_expected_columns(dsv_df, column_configs, schema_overrides)

    return dsv_df


def _validate_expected_columns(
    dsv_df: pl.DataFrame,
    column_configs: Sequence[IngestionColumnConfig],
    schema_overrides: Mapping[str, pl.DataType] = MappingProxyType({}),
) -> pl.DataFrame:
    header_cfgs = header_configs(column_configs)
    expected_headers = [c.source for c in header_cfgs if c.source]

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
                pl.lit(None).cast(_get_column_target_dtype(h, header_cfgs)).alias(h)
                for h in defined_but_missing_headers
            ]
        ).clear()

        LOGGER.debug(missing_columns_df)

    return dsv_df
