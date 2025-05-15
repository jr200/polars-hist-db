import re
import sys
from typing import Any, List
import polars as pl

from .fn_transform import DataFrameTransform


class NullIfGte(DataFrameTransform):

    @classmethod
    def name(cls):
        return "null_if_gte"

    def __init__(self, args: List[Any]):
        self.col_header = args[0]
        self.threshold_value = args[1]

    def dependencies(self) -> List[str]:
        return [self.col_header]

    def apply(self, df: pl.DataFrame) -> pl.DataFrame:
        df = df.with_columns(
            pl.when(pl.col(self.col_header) >= pl.lit(self.threshold_value))
            .then(None)
            .otherwise(pl.col(self.col_header))
            .alias(self.col_header)
        )

        return df
        


class ApplyTypeCasts(DataFrameTransform):

    @classmethod
    def name(cls):
        return "apply_type_casts"

    def __init__(self, args: List[Any]):
        self.col_header = args[0]
        self.dtypes = args[1:]

    def dependencies(self) -> List[str]:
        return [self.col_header]

    def apply(self, df: pl.DataFrame) -> pl.DataFrame:
        for polars_dtype_str in self.dtypes:
            polars_dtype = getattr(sys.modules["polars"], polars_dtype_str)
            df = df.with_columns(pl.col(self.col_header).cast(polars_dtype).alias(self.col_header))

        return df




class CombineColumns(DataFrameTransform):

    @classmethod
    def name(cls):
        return "combine_columns"

    def __init__(self, args: List[Any]):
        self.col_header = args[0]
        self.values = args[1:]

    def dependencies(self) -> List[str]:
        return [self.col_header] + self.values

    def apply(self, df: pl.DataFrame) -> pl.DataFrame:
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

            result = pl.concat_str(exprs).alias(self.col_header)
            return result

        combine_expr = _make_combine_expr(self.values)
        df = df.with_columns(combine_expr)
        return df

