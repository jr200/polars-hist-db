from os import getenv
import polars as pl

pl.enable_string_cache()

if getenv("POLARS_HIST_DB_DEBUG") == "1":
    pl.Config.set_tbl_cols(-1)
    pl.Config.set_tbl_rows(-1)
    pl.Config.set_tbl_width_chars(1000)
    pl.Config.set_fmt_str_lengths(1000)
