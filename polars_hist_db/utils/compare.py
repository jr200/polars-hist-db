import itertools
import logging
from typing import List, Optional, Tuple

import polars as pl

LOGGER = logging.getLogger(__name__)


def compute_diff(
    df: pl.DataFrame,
    cols: List[Tuple[str, str]],
    suffix: str = "_diff",
    float_tol: float = 10e-10,
) -> pl.DataFrame:
    for x, y in cols:
        dtype_x = df[x].dtype
        dtype_y = df[y].dtype
        diff_col = f"{x}{suffix}"

        try:
            if dtype_x.is_decimal() or dtype_y.is_decimal():
                df = df.with_columns(
                    (
                        (
                            (
                                pl.col(x).cast(pl.Float64) - pl.col(y).cast(pl.Float64)
                            ).abs()
                            > float_tol
                        )
                        | (pl.col(x).is_null() != pl.col(y).is_null())
                    ).alias(diff_col)
                )

            elif dtype_x.is_float() or dtype_y.is_float():
                df = df.with_columns(
                    (
                        ((pl.col(x) - pl.col(y)).abs() > float_tol)
                        | (pl.col(x).is_null() != pl.col(y).is_null())
                    ).alias(diff_col)
                )
            else:
                df = df.with_columns(
                    (
                        (pl.col(x) != pl.col(y))
                        | (pl.col(x).is_null() != pl.col(y).is_null())
                    ).alias(diff_col)
                )

        except Exception as e:
            LOGGER.error(
                "compute_diff failed (%s, %s) and (%s, %s)",
                x,
                dtype_x,
                y,
                dtype_y,
                exc_info=e,
            )

    return df


def compare_dataframes(
    lhs: pl.DataFrame,
    rhs: pl.DataFrame,
    on: List[str],
    cmp_cols: Optional[List[str]] = None,
    suffixes: Tuple[str, str, str] = ("_lhs", "_rhs", "_diff"),
):
    _lhs, _rhs, _diff = suffixes

    rhs_missing_cols = [
        f"missing:{c}{_rhs}" for c in set(lhs.columns).difference(rhs.columns)
    ]

    lhs_missing_cols = [
        f"missing:{c}{_lhs}" for c in set(rhs.columns).difference(lhs.columns)
    ]

    missing_cols = sorted(lhs_missing_cols) + sorted(rhs_missing_cols)
    intersect_cols = sorted(set(rhs.columns).intersection(lhs.columns))
    if cmp_cols is None:
        cmp_cols = sorted(set(intersect_cols).difference(on))
    else:
        intersect_cols = sorted(set(intersect_cols).intersection(cmp_cols).union(on))

    if len(cmp_cols) == 0:
        raise ValueError("No columns to compare")

    diffs_df = (
        lhs.select(intersect_cols)
        .select(pl.col(c).cast(rhs[c].dtype) for c in intersect_cols)
        .join(rhs, on=on, how="full", suffix=f"{_rhs}", coalesce=True)
        .pipe(compute_diff, [(c, f"{c}{_rhs}") for c in cmp_cols])
        .select(
            itertools.chain(
                on,
                *(
                    [
                        (pl.col(c), pl.col(f"{c}{_rhs}"), pl.col(f"{c}{_diff}"))
                        for c in cmp_cols
                    ]
                ),
            )
        )
        .filter(pl.any_horizontal(pl.col(f"^.*{_diff}$")))
    )

    diffs_df = diffs_df.select(
        itertools.chain(
            on,
            *[
                (pl.col(c).alias(f"{c}{_lhs}"), pl.col(f"{c}{_rhs}"))
                for c in cmp_cols
                if diffs_df[f"{c}{_diff}"].any()
            ],
        )
    ).sort(on)

    return diffs_df, missing_cols
