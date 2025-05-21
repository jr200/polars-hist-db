import re
import sys
from typing import Any, List
import polars as pl


def null_if_gte(
    df: pl.DataFrame, input_col: str, result_col: str, args: List[Any]
) -> pl.DataFrame:
    threshold_value = args[0]
    df = df.with_columns(
        pl.when(input_col >= pl.lit(threshold_value))
        .then(None)
        .otherwise(input_col)
        .alias(result_col)
    )

    return df


def apply_type_casts(
    df: pl.DataFrame, input_col: str, result_col: str, args: List[Any]
) -> pl.DataFrame:
    dtypes = args[0:]

    for polars_dtype_str in dtypes:
        polars_dtype = getattr(sys.modules["polars"], polars_dtype_str)
        df = df.with_columns(pl.col(input_col).cast(polars_dtype))

    df = df.with_columns(pl.col(input_col).alias(result_col))
    return df


def combine_columns(
    df: pl.DataFrame, _input_col: str, result_col: str, args: List[Any]
) -> pl.DataFrame:
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

        result = pl.concat_str(exprs).alias(result_col)
        return result

    combine_expr = _make_combine_expr(values)
    df = df.with_columns(combine_expr)

    return df
