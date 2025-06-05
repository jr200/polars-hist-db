import polars

polars.Config.set_tbl_cols(-1)
polars.Config.set_tbl_rows(-1)
polars.Config.set_tbl_width_chars(1000)
polars.Config.set_fmt_str_lengths(1000)
