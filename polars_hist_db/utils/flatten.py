# reference: https://github.com/pola-rs/polars/issues/7078#issuecomment-2258225305
# modified the above to explode list columns, rather than unnesting them

import polars as pl


def prefix_field(field: str) -> pl.Expr:
    """Prefix struct fields with parent column name"""
    return pl.col(field).name.prefix_fields(f"{field}.")


def flatten(df: pl.DataFrame) -> pl.DataFrame:
    """Flatten one level of struct and prefix flattened fields
    with parent column name, and explode list columns
    """
    struct_cols = [
        col for col, dtype in zip(df.columns, df.dtypes) if type(dtype) is pl.Struct
    ]
    list_cols = [
        col for col, dtype in zip(df.columns, df.dtypes) if type(dtype) is pl.List
    ]

    if len(list_cols) > 0:
        df = df.explode(list_cols)

    if len(struct_cols) > 0:
        df = df.with_columns(*map(prefix_field, struct_cols)).unnest(*struct_cols)

    return df


def recursive_flatten(df: pl.DataFrame) -> pl.DataFrame:
    """Recursively flatten list and struct columns"""
    while any(type(dtype) in (pl.Struct, pl.List) for dtype in df.dtypes):
        df = flatten(df)
    return df
