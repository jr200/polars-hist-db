import logging
from collections.abc import Callable, Iterable, Mapping
from datetime import UTC, datetime
from typing import Any, Literal

import polars as pl

from ..config import DeltaConfig, TableConfig, ValidTimeConfig
from ..types import PolarsType
from .xtdb_arrow import (
    _prepare_xtdb_insert_dataframe,
    _xtdb_document_id_cast_type,
    _xtdb_document_id_columns,
    _xtdb_document_id_is_encoded,
)
from .xtdb_dataframe import (
    XtdbAdbcDataframeOps,
    XtdbDataframeOps,
    _uploaded_xtdb_relation,
)
from .xtdb_query import _xtdb_temporal_basis_clause
from .xtdb_staging import _fill_xtdb_defaults, _materialize_xtdb_missing_columns
from .xtdb_transport import (
    _execute_xtdb_dml,
    _is_xtdb_adbc_ingest_unavailable,
    _is_xtdb_table_not_found_error,
    _qualified_table_name,
    _rollback_xtdb_connection,
    _xtdb_timestamp_literal,
)

_XTDB_READONLY_SYSTEM_COLUMNS = {"_system_from", "_system_to"}
LOGGER = logging.getLogger(__name__)


def _xtdb_temporal_upsert(
    df: pl.DataFrame,
    table_schema: str,
    table_name: str,
    *,
    connection: Any | None = None,
    dataframe_ops: XtdbDataframeOps | XtdbAdbcDataframeOps | None = None,
    table_config: TableConfig | None = None,
    delta_config: DeltaConfig | None = None,
    update_time: datetime | None = None,
    valid_time: ValidTimeConfig | None = None,
    dropout_close_time: datetime | None = None,
    max_rows_per_insert: int | None = None,
    dataframe_factory: Callable[[Any], XtdbDataframeOps] | None = None,
) -> int:
    def pgwire_dataframes(connection: Any) -> XtdbDataframeOps:
        if dataframe_factory is not None:
            return dataframe_factory(connection)
        return XtdbDataframeOps(connection, max_rows_per_insert)

    if dataframe_ops is None:
        if connection is None:
            raise ValueError(
                "XTDB temporal upsert requires connection or dataframe_ops"
            )
        dataframe_ops = pgwire_dataframes(connection)

    df = _apply_xtdb_valid_time_mapping(df, valid_time)
    df = _apply_xtdb_non_temporal_valid_from(df, table_config, valid_time)

    deleted_count = 0
    if delta_config is not None:
        if delta_config.row_finality not in {"disabled", "dropout"}:
            raise NotImplementedError(
                "XTDB temporal upsert currently only supports "
                "row_finality='disabled' or 'dropout'"
            )
        if table_config is None:
            raise ValueError("XTDB delta upsert requires table_config")
        df = _materialize_xtdb_missing_columns(
            df,
            table_config,
            use_defaults=delta_config.prefill_nulls_with_default,
        )
        if delta_config.prefill_nulls_with_default:
            df = _fill_xtdb_defaults(df, table_config)
        df = _apply_xtdb_duplicate_policy(df, table_config, delta_config)
        if delta_config.row_finality == "dropout":
            if not isinstance(dataframe_ops, XtdbDataframeOps):
                raise NotImplementedError(
                    "XTDB row_finality='dropout' currently requires the "
                    "pgwire dataframe path"
                )
            deleted_count = _delete_xtdb_missing_rows(
                df,
                table_schema,
                table_name,
                table_config,
                dataframe_ops,
                update_time,
                dropout_close_time,
            )
        if delta_config.drop_unchanged_rows:
            df = _filter_xtdb_unchanged_rows(
                df,
                table_schema,
                table_name,
                table_config,
                dataframe_ops,
                update_time,
            )

    insert_kwargs: dict[str, Any] = {"table_config": table_config}
    if update_time is not None:
        insert_kwargs["update_time"] = update_time

    try:
        inserted_count = dataframe_ops.table_insert(
            df,
            table_schema,
            table_name,
            **insert_kwargs,
        )
    except Exception as exc:
        if not (
            isinstance(dataframe_ops, XtdbAdbcDataframeOps)
            and _is_xtdb_adbc_ingest_unavailable(exc)
        ):
            raise
        if connection is None:
            raise
        LOGGER.warning(
            "XTDB ADBC ingest unavailable — falling back to pgwire (%s)", exc
        )
        inserted_count = pgwire_dataframes(connection).table_insert(
            df,
            table_schema,
            table_name,
            **insert_kwargs,
        )
    return deleted_count + inserted_count


def _apply_xtdb_duplicate_policy(
    df: pl.DataFrame,
    table_config: TableConfig,
    delta_config: DeltaConfig,
) -> pl.DataFrame:
    if df.is_empty():
        return df

    row_index = "__xtdb_duplicate_row_index"
    indexed_df = df.with_row_index(row_index)
    prepared = _prepare_xtdb_insert_dataframe(indexed_df, table_config)

    duplicate_count_column = "__xtdb_duplicate_count"
    duplicate_keys = (
        prepared.group_by("_id")
        .agg(pl.len().alias(duplicate_count_column))
        .filter(pl.col(duplicate_count_column) > 1)
    )
    if duplicate_keys.is_empty():
        return df

    if delta_config.on_duplicate_key == "error":
        raise ValueError("XTDB delta upsert found duplicate source keys")

    keep: Literal["first", "last"] = (
        "first" if delta_config.on_duplicate_key == "take_first" else "last"
    )
    selected_rows = (
        prepared.select([row_index, "_id"])
        .unique(subset=["_id"], keep=keep, maintain_order=True)
        .select(row_index)
    )
    return indexed_df.join(selected_rows, on=row_index, how="inner").drop(row_index)


def _delete_xtdb_missing_rows(
    df: pl.DataFrame,
    table_schema: str,
    table_name: str,
    table_config: TableConfig,
    dataframe_ops: "XtdbDataframeOps",
    update_time: datetime | None,
    dropout_close_time: datetime | None,
) -> int:
    document_id_columns = _xtdb_document_id_columns(table_config)
    incoming_ids = _prepare_xtdb_insert_dataframe(
        df.select(document_id_columns).unique(maintain_order=True),
        table_config,
    ).get_column("_id")
    table_sql = _qualified_table_name(table_schema, table_name)

    def delete_missing(missing_predicate: str) -> int:
        try:
            missing_count = int(
                dataframe_ops.from_raw_sql(
                    "SELECT COUNT(*) AS missing_count FROM "
                    f"{table_sql}{_xtdb_temporal_basis_clause(update_time)} "
                    f"WHERE {missing_predicate}"
                ).item()
            )
        except Exception as exc:
            if _is_xtdb_table_not_found_error(exc):
                _rollback_xtdb_connection(dataframe_ops.connection)
                return 0
            raise
        if missing_count == 0:
            return 0
        valid_time_clause = ""
        close_time = _xtdb_dropout_close_time(df, dropout_close_time, update_time)
        if close_time is not None:
            valid_time_clause = (
                " FOR PORTION OF VALID_TIME FROM "
                f"{_xtdb_timestamp_literal(close_time)} TO NULL"
            )
        delete_sql = (
            f"DELETE FROM {table_sql}{valid_time_clause} WHERE {missing_predicate}"
        )
        _execute_xtdb_dml(
            dataframe_ops.connection,
            delete_sql,
            system_time=update_time,
        )
        return missing_count

    if incoming_ids.is_empty():
        return delete_missing("TRUE")
    with _uploaded_xtdb_relation(
        dataframe_ops,
        pl.DataFrame({"_id": incoming_ids}),
        table_schema,
    ) as key_table:
        return delete_missing(
            "_id NOT IN (SELECT _id FROM "
            f"{key_table} FOR VALID_TIME ALL FOR SYSTEM_TIME ALL)"
        )


def _xtdb_dropout_close_time(
    df: pl.DataFrame,
    dropout_close_time: datetime | None,
    update_time: datetime | None,
) -> datetime | None:
    if dropout_close_time is not None:
        return dropout_close_time
    if "_valid_from" not in df.columns or df.is_empty():
        return update_time

    close_times = df.select("_valid_from").drop_nulls().unique()
    if close_times.height == 0:
        return update_time
    if close_times.height > 1:
        raise ValueError(
            "XTDB row_finality='dropout' requires a single _valid_from value "
            "when update_time is not provided"
        )

    close_time = close_times.item()
    if not isinstance(close_time, datetime):
        raise TypeError(
            "XTDB row_finality='dropout' _valid_from values must be datetimes"
        )
    return close_time


def _xtdb_value_compare_dtypes(table_config: TableConfig) -> dict[str, pl.DataType]:
    document_id_columns = _xtdb_document_id_columns(table_config)
    uses_single_source_key = len(
        document_id_columns
    ) == 1 and not _xtdb_document_id_is_encoded(table_config)
    dtypes = {}
    for column in table_config.columns:
        target_name = (
            "_id"
            if uses_single_source_key and column.name == document_id_columns[0]
            else column.name
        )
        dtypes[target_name] = PolarsType.from_sql(column.data_type)
    if uses_single_source_key and document_id_columns != ["_id"]:
        dtypes["_id"] = PolarsType.from_sql(_xtdb_document_id_cast_type(table_config))
    return dtypes


def _apply_xtdb_compare_dtypes(
    df: pl.DataFrame,
    columns: Iterable[str],
    dtypes: Mapping[str, pl.DataType],
) -> pl.DataFrame:
    casts = [
        pl.col(column).cast(dtypes[column])
        for column in columns
        if column in df.columns and column in dtypes
    ]
    if not casts:
        return df
    return df.with_columns(casts)


def _filter_xtdb_unchanged_rows(
    df: pl.DataFrame,
    table_schema: str,
    table_name: str,
    table_config: TableConfig,
    dataframe_ops: Any,
    update_time: datetime | None,
) -> pl.DataFrame:
    if df.is_empty():
        return df

    row_index = "__xtdb_row_index"
    exists_column = "__xtdb_exists"
    indexed_df = df.with_row_index(row_index)
    incoming = _prepare_xtdb_insert_dataframe(indexed_df, table_config)

    compare_columns = [
        column
        for column in incoming.columns
        if column not in {row_index, "_valid_from", *_XTDB_READONLY_SYSTEM_COLUMNS}
    ]
    query_columns = [
        "__valid_to" if column == "_valid_to" else column for column in compare_columns
    ]
    try:
        current = dataframe_ops.table_query(
            table_schema,
            table_name,
            incoming.select("_id").unique(maintain_order=True),
            query_columns,
            table_config=table_config,
            basis_time=update_time,
        )
    except Exception as exc:
        if _is_xtdb_table_not_found_error(exc):
            _rollback_xtdb_connection(dataframe_ops.connection)
            return df
        raise
    if "__valid_to" in current.columns:
        current = current.rename({"__valid_to": "_valid_to"})
    if current.is_empty():
        return df

    compare_columns = [
        column for column in compare_columns if column in current.columns
    ]
    if "_id" not in compare_columns:
        raise ValueError("XTDB delta upsert requires current rows to include _id")

    value_columns = [column for column in compare_columns if column != "_id"]
    compare_dtypes = _xtdb_value_compare_dtypes(table_config)
    incoming = _apply_xtdb_compare_dtypes(incoming, value_columns, compare_dtypes)
    current = _apply_xtdb_compare_dtypes(current, value_columns, compare_dtypes)
    current_projection = current.select(compare_columns).with_columns(
        pl.lit(True).alias(exists_column)
    )
    current_projection = current_projection.rename(
        {column: f"{column}__xtdb_current" for column in value_columns}
    )

    joined = incoming.join(current_projection, on="_id", how="left")
    keep_expr = pl.col(exists_column).is_null()
    for column in value_columns:
        current_column = f"{column}__xtdb_current"
        equal_expr = (
            (pl.col(column) == pl.col(current_column))
            | (pl.col(column).is_null() & pl.col(current_column).is_null())
        ).fill_null(False)
        keep_expr = keep_expr | equal_expr.not_()

    changed_row_indexes = joined.filter(keep_expr).select(row_index)
    return indexed_df.join(changed_row_indexes, on=row_index, how="inner").drop(
        row_index
    )


_XTDB_NON_TEMPORAL_VALID_FROM = datetime(1970, 1, 1, tzinfo=UTC)


def _apply_xtdb_valid_time_mapping(
    df: pl.DataFrame, valid_time: ValidTimeConfig | None
) -> pl.DataFrame:
    if valid_time is None:
        return df

    mappings = {
        valid_time.from_column: "_valid_from",
    }
    if valid_time.to_column is not None:
        mappings[valid_time.to_column] = "_valid_to"

    missing_columns = [source for source in mappings if source not in df.columns]
    if missing_columns:
        raise ValueError(
            "XTDB valid-time mapping references missing source column(s): "
            + ", ".join(missing_columns)
        )

    null_columns = [source for source in mappings if df[source].null_count() > 0]
    if null_columns:
        raise ValueError(
            "XTDB valid-time mapping references null source value(s): "
            + ", ".join(null_columns)
        )

    for source, target in mappings.items():
        if target in df.columns and source != target:
            raise ValueError(
                f"XTDB valid-time mapping cannot write {target}; "
                "the dataframe already contains that column"
            )

    return df.with_columns(
        pl.col(source).alias(target)
        for source, target in mappings.items()
        if source != target
    )


def _apply_xtdb_non_temporal_valid_from(
    df: pl.DataFrame,
    table_config: TableConfig | None,
    valid_time: ValidTimeConfig | None,
) -> pl.DataFrame:
    """Reference tables (``is_temporal: false``) with no ``valid_time`` mapping
    must be readable as-of any historical timestamp — they represent an
    always-valid dimension, not a bitemporal fact.

    XTDB otherwise defaults an unspecified ``_valid_from`` to transaction
    time, so ``FOR VALID_TIME AS OF <t>`` on any ``t`` before the upload
    returns zero rows. Pin ``_valid_from`` to the unix epoch so those
    queries resolve.
    """
    if table_config is None or table_config.is_temporal:
        return df
    if valid_time is not None:
        return df
    if "_valid_from" in df.columns:
        return df
    return df.with_columns(pl.lit(_XTDB_NON_TEMPORAL_VALID_FROM).alias("_valid_from"))
