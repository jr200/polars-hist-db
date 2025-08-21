import logging
from typing import Any, Dict, List, Optional
import polars as pl
from sqlalchemy import ColumnElement, and_, Column, Connection, select
from sqlalchemy.sql.functions import coalesce

from ..core import DataframeOps, TableOps
from ..config import TableConfig

LOGGER = logging.getLogger(__name__)


def _get_column_info(
    table_config: TableConfig, col_map: Dict[str, str], col_name: str
) -> Optional[Dict[str, Any]]:
    """Retrieve column information for the given column name."""
    col_info = table_config.columns_df()
    row = col_info.filter(pl.col("name") == col_map[col_name])
    if row.is_empty():
        return None
    return {"nullable": row[0, "nullable"], "default_value": row[0, "default_value"]}


def _coalesce_with_default(
    col: Column[Any],
    table_config: TableConfig,
    col_map: Dict[str, str],
    coalesce_disabled: bool = True,
) -> ColumnElement[Any]:
    """Apply coalesce with default value if column is not nullable and coalesce is enabled."""
    if coalesce_disabled:
        return col

    col_info = _get_column_info(table_config, col_map, col.name)
    if col_info and not col_info["nullable"] and col_info["default_value"] is not None:
        return coalesce(col, col_info["default_value"])

    return col


def _get_foreign_key_columns(col_info: pl.DataFrame) -> List[str]:
    """Retrieve columns that need foreign key deducing."""
    return col_info.filter("deduce_foreign_key")["source"].to_list()


def _get_value_columns(col_info: pl.DataFrame) -> Dict[str, str]:
    """Retrieve columns that do not need foreign key deducing."""
    df = col_info.filter(pl.col("deduce_foreign_key").not_()).select("source", "target")
    return dict(df.iter_rows())


def _prepare_population_set(
    table_schema: str,
    src_table_name: str,
    parent_table_config: TableConfig,
    col_info: pl.DataFrame,
    connection: Connection,
) -> pl.DataFrame:
    """Prepare population set to deduce foreign keys."""
    src_tbo = TableOps(table_schema, src_table_name, connection)
    src_tbl = src_tbo.get_table_metadata()
    parent_tbo = TableOps(
        parent_table_config.schema, parent_table_config.name, connection
    )
    parent_tbl = parent_tbo.get_table_metadata()

    src_primary_keys = [src_tbl.c[k] for k in src_tbl.primary_key.columns.keys()]
    parent_implied_cols = [
        parent_tbl.c[col]
        for col in col_info.filter("deduce_foreign_key").select("target").iter_rows()
    ]

    value_col_map = _get_value_columns(col_info)
    src_value_cols = [src_tbl.c[col] for col in value_col_map.keys()]

    on_clause = [
        parent_tbl.c[t]
        == _coalesce_with_default(
            src_tbl.c[s], parent_table_config, value_col_map, coalesce_disabled=False
        )
        for s, t in value_col_map.items()
    ]

    population_set = select(
        *src_primary_keys, *parent_implied_cols, *src_value_cols
    ).select_from(src_tbl.outerjoin(parent_tbl, and_(*on_clause)))

    result = DataframeOps(connection).from_selectable(population_set)
    return result


def deduce_foreign_keys(
    src_table_schema: str,
    src_table_name: str,
    parent_table_config: TableConfig,
    col_info: pl.DataFrame,
    connection: Connection,
):
    src_implied_col_names = _get_foreign_key_columns(col_info)
    if not src_implied_col_names:
        return

    LOGGER.debug(
        "Deducing foreign keys for %s from parent table %s",
        src_table_name,
        parent_table_config.name,
    )
    src_parent_col_map = dict(col_info.select("source", "target").iter_rows())

    population_set_df = _prepare_population_set(
        src_table_schema, src_table_name, parent_table_config, col_info, connection
    )

    new_items_columns = list(_get_value_columns(col_info).keys())
    new_items_to_insert_in_parent = (
        population_set_df.filter(
            pl.col([src_parent_col_map[c] for c in src_implied_col_names]).is_null()
        )
        .select(new_items_columns)
        .rename({k: v for k, v in src_parent_col_map.items() if k in new_items_columns})
    )

    if not new_items_to_insert_in_parent.is_empty():
        missing_values_map: Dict[str, str] = {
            c.name: c.default_value
            for c in parent_table_config.columns
            if c.default_value is not None and c.name
        }
        new_items_to_insert_in_parent = DataframeOps.fill_nulls_with_defaults(
            new_items_to_insert_in_parent, missing_values_map
        )
        LOGGER.debug(
            "Creating %d new entries in %s.%s",
            len(new_items_to_insert_in_parent),
            parent_table_config.schema,
            parent_table_config.name,
        )

        dfo = DataframeOps(connection)
        dfo.table_insert(
            new_items_to_insert_in_parent,
            parent_table_config.schema,
            parent_table_config.name,
            prefill_nulls_with_default=False,
            uniqueness_col_set=(),
        )

    implied_df = _prepare_population_set(
        src_table_schema, src_table_name, parent_table_config, col_info, connection
    )
    implied_df = implied_df.rename(
        {v: k for k, v in src_parent_col_map.items() if v in implied_df.columns}
    )

    # update the source table with any new rows
    if not implied_df.is_empty():
        implied_df = implied_df.select(
            set(src_parent_col_map.keys()) | set(src_implied_col_names)
        )
        num_nulls = implied_df.null_count().select(sum=pl.sum_horizontal(pl.all()))[
            0, "sum"
        ]

        if num_nulls > 0:
            raise ValueError("Attempted to update with NULL keys")

        dfo = DataframeOps(connection)
        dfo.table_update(
            implied_df,
            src_table_schema,
            src_table_name,
            primary_keys_override=new_items_columns,
        )
