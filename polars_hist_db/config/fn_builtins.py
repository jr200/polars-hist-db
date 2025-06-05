import re
import sys
from typing import Any, List
import polars as pl


def null_if_gte(df: pl.DataFrame, result_col: str, args: List[Any]) -> pl.DataFrame:
    threshold_value = args[0]
    df = df.with_columns(
        pl.when(result_col >= pl.lit(threshold_value))
        .then(None)
        .otherwise(result_col)
        .alias(result_col)
    )

    return df


def parse_date(df: pl.DataFrame, result_col: str, args: List[Any]) -> pl.DataFrame:
    date_formats = args
    df = df.with_columns(
        pl.coalesce(
            *[
                pl.col(result_col).str.strptime(pl.Datetime, fmt, strict=False)
                for fmt in date_formats
            ]
        ).alias(result_col)
    )

    return df


def apply_type_casts(
    df: pl.DataFrame, result_col: str, args: List[Any]
) -> pl.DataFrame:
    dtypes = args[0:]

    for polars_dtype_str in dtypes:
        polars_dtype = getattr(sys.modules["polars"], polars_dtype_str)
        df = df.with_columns(pl.col(result_col).cast(polars_dtype))

    return df


def combine_columns(df: pl.DataFrame, result_col: str, args: List[Any]) -> pl.DataFrame:
    values = args[0:]

    def _make_combine_expr(components: List[str]) -> pl.Expr:
        exprs = []
        pattern = r"[$][{](?P<col_name>.*?)[}]"
        for c in components:
            m = re.match(pattern, c)
            expr = None if m is None else m.groupdict().get("col_name", None)
            if expr is None:
                exprs.append(pl.lit(c))
            else:
                exprs.append(pl.col(expr))

        result = pl.concat_str(exprs)
        return result

    combine_expr = _make_combine_expr(values)
    df = df.with_columns(combine_expr.alias(result_col))

    return df


def map_to_true(df: pl.DataFrame, result_col: str, args: List[Any]) -> pl.DataFrame:
    true_values = args

    df = df.with_columns(
        pl.when(pl.col(result_col).is_in(true_values))
        .then(True)
        .otherwise(False)
        .fill_null(False)
        .alias(result_col)
    )

    return df
