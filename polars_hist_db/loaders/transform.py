import logging
from collections.abc import Mapping, Sequence
from types import MappingProxyType

import polars as pl

from ..config.parser_config import IngestionColumnConfig
from ..config.transform_fn_registry import TransformFnRegistry
from ..core.dataframe import DataframeOps
from ..types import PolarsType
from ..utils.exceptions import NonRetryableException

LOGGER = logging.getLogger(__name__)


class InputSchemaError(TypeError, NonRetryableException):
    """An input DataFrame does not satisfy its resolved pipeline schema."""


def enforce_input_schema(
    df: pl.DataFrame, column_configs: Sequence[IngestionColumnConfig]
) -> pl.DataFrame:
    sources: dict[str, tuple[pl.DataType, bool, bool]] = {}
    generated: dict[str, pl.DataType] = {}
    for config in column_configs:
        if config.source is None:
            if config.target is not None and config.column_type == "computed":
                generated[config.target] = PolarsType.from_sql(config.target_data_type)
            continue

        dtype = PolarsType.from_sql(config.ingestion_data_type)
        required = config.required
        nullable = True if config.nullable is None else config.nullable
        previous = sources.get(config.source)
        if previous is not None and previous[0] != dtype:
            raise InputSchemaError(
                f"conflicting ingestion types for {config.source!r}: "
                f"{previous[0]} and {dtype}"
            )
        sources[config.source] = (
            dtype,
            required or (previous[1] if previous else False),
            nullable and (previous[2] if previous else True),
        )

    accepted = set(sources).union(generated)
    unknown = set(df.columns).difference(accepted)
    null_structural_columns = {
        name
        for name in unknown
        if df[name].null_count() == df.height
        and any(source.startswith(f"{name}.") for source in sources)
    }
    unknown_names = sorted(unknown.difference(null_structural_columns))
    if unknown_names:
        raise InputSchemaError("undeclared input columns: " + ", ".join(unknown_names))

    result = df.drop(null_structural_columns)
    for name, (dtype, required, nullable) in sources.items():
        if name not in result.columns:
            if required or not nullable:
                raise InputSchemaError(f"missing required input column: {name}")
            result = result.with_columns(pl.lit(None).cast(dtype).alias(name))
        else:
            try:
                result = PolarsType.apply_dtype_to_column(result, name, dtype)
            except Exception as exc:
                raise InputSchemaError(
                    f"input column {name!r} cannot be converted to {dtype}"
                ) from exc
        if not nullable and result[name].null_count() > 0:
            raise InputSchemaError(f"nulls in required input column: {name}")

    for name, dtype in generated.items():
        if name in result.columns:
            try:
                result = PolarsType.apply_dtype_to_column(result, name, dtype)
            except Exception as exc:
                raise InputSchemaError(
                    f"computed input column {name!r} cannot be converted to {dtype}"
                ) from exc

    return result.select(
        [*sources, *(name for name in generated if name in result.columns)]
    )


def header_configs(
    column_configs: Sequence[IngestionColumnConfig],
) -> Sequence[IngestionColumnConfig]:
    return [
        cfg
        for cfg in column_configs
        if cfg.source and cfg.column_type in ["data", "input_only", "dsv_only"]
    ]


def apply_transformations(
    dsv_df: pl.DataFrame, column_configs: Sequence[IngestionColumnConfig]
) -> pl.DataFrame:
    for col_cfg in column_configs:
        # skip columns already computed
        if (
            col_cfg.source is None
            and col_cfg.target in dsv_df.columns
            and col_cfg.column_type == "computed"
        ):
            LOGGER.debug("Skipping already-transformed column %s", col_cfg.target)
            continue

        dsv_df = dsv_df.pipe(_apply_header_transforms, col_cfg)

    # # drop headers only used in temporary calc
    dsv_df = dsv_df.drop(
        [
            col_cfg.source
            for col_cfg in column_configs
            if col_cfg.column_type in {"input_only", "dsv_only"}
            and col_cfg.source in dsv_df.columns
        ]
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

    missing_values_map = {
        c.source: c.value_if_missing
        for c in column_configs
        if c.value_if_missing and c.source
    }
    dsv_df = DataframeOps.fill_nulls_with_defaults(dsv_df, missing_values_map)

    dsv_df = _apply_target_schema(dsv_df, column_configs)

    return dsv_df


def _apply_header_transforms(
    df: pl.DataFrame, col_def: IngestionColumnConfig
) -> pl.DataFrame:
    if not col_def.transforms:
        return df

    input_col = col_def.target if col_def.source is None else col_def.source
    if input_col is None:
        raise ValueError(f"missing source-output column for {col_def}")

    fn_reg = TransformFnRegistry()
    for fn_name, fn_args in col_def.transforms.items():
        if fn_args is None:
            continue

        # source_col = col_def.target if col_def.source is None else col_def.source
        df = fn_reg.call_function(fn_name, df, input_col, fn_args)

    result_col_dtype = PolarsType.from_sql(col_def.target_data_type)
    df = df.with_columns(pl.col(input_col).cast(result_col_dtype))

    return df


def _apply_target_schema(
    dsv_df: pl.DataFrame,
    column_configs: Sequence[IngestionColumnConfig],
    schema_overrides: Mapping[str, pl.DataType] = MappingProxyType({}),
) -> pl.DataFrame:
    headers_target_schema: Mapping[str, pl.DataType] = {
        cfg.source: PolarsType.from_sql(cfg.target_data_type)
        for cfg in header_configs(column_configs)
        if cfg.source
    }

    dsv_df = dsv_df.pipe(
        PolarsType.apply_schema_to_dataframe,
        **headers_target_schema,
        **schema_overrides,
    )

    return dsv_df
