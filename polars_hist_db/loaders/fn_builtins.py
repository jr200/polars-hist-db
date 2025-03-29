import re
import sys
from typing import Any, List
import polars as pl


def null_if_gte(df: pl.DataFrame, args: List[Any]) -> pl.DataFrame:
    col_header = args[0]
    threshold_value = args[1]
    df = df.with_columns(
        pl.when(pl.col(col_header) >= pl.lit(threshold_value))
        .then(None)
        .otherwise(pl.col(col_header))
        .alias(col_header)
    )

    return df


def apply_type_casts(df: pl.DataFrame, args: List[Any]) -> pl.DataFrame:
    col_header = args[0]
    dtypes = args[1:]
    for polars_dtype_str in dtypes:
        polars_dtype = getattr(sys.modules["polars"], polars_dtype_str)
        df = df.with_columns(pl.col(col_header).cast(polars_dtype).alias(col_header))

    return df


def combine_columns(df: pl.DataFrame, args: List[Any]) -> pl.DataFrame:
    col_header = args[0]
    values = args[1:]

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

        result = pl.concat_str(exprs).alias(col_header)
        return result

    combine_expr = _make_combine_expr(values)
    df = df.with_columns(combine_expr)

    return df
