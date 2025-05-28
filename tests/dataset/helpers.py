from typing import Any, List
import polars as pl


def custom_try_to_usd(
    df: pl.DataFrame, result_col: str, args: List[Any]
) -> pl.DataFrame:
    usdtry_fx_rates = pl.from_dict(
        {
            "Year": [
                2010,
                2011,
                2012,
                2013,
                2014,
                2015,
                2016,
                2017,
                2018,
                2019,
                2020,
                2021,
                2022,
                2023,
            ],
            "fx_usdtry": [
                1.507,
                1.674,
                1.802,
                1.915,
                2.188,
                2.724,
                3.020,
                3.646,
                4.830,
                5.680,
                7.004,
                8.886,
                16.566,
                23.085,
            ],
        },
        schema={"Year": pl.Int64, "fx_usdtry": pl.Decimal(10, 4)},
    )

    col_try = args[0]
    col_year = args[1]
    df = (
        df.join(usdtry_fx_rates, left_on=col_year, right_on="Year", how="left")
        .with_columns(
            (pl.col(col_try) * 1.0 / pl.col("fx_usdtry"))
            .round(4, mode="half_away_from_zero")
            .cast(pl.Decimal(10, 4))
            .alias(result_col)
        )
        .drop("fx_usdtry")
    )

    return df
