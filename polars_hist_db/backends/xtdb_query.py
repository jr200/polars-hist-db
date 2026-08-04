from datetime import datetime

from ..config import TableConfig
from ..core import TimeHint
from .xtdb_arrow import _xtdb_document_id_is_encoded
from .xtdb_transport import (
    _xtdb_column_identifier,
    _xtdb_timestamp_literal,
)

_XTDB_SYSTEM_COLUMNS = {"_valid_from", "_valid_to", "_system_from", "_system_to"}


def _xtdb_temporal_basis_clause(update_time: datetime | None) -> str:
    if update_time is None:
        return ""
    return (
        f" FOR VALID_TIME AS OF {_xtdb_timestamp_literal(update_time)}"
        f" FOR SYSTEM_TIME AS OF {_xtdb_timestamp_literal(update_time)}"
    )


def _xtdb_valid_time_clause(time_hint: TimeHint | None) -> str:
    if time_hint is None or time_hint.mode == "none":
        return ""
    if time_hint.mode == "all":
        return " FOR VALID_TIME ALL"
    if time_hint.mode == "asof":
        assert isinstance(time_hint.asof_utc, datetime)
        return f" FOR VALID_TIME AS OF {_xtdb_timestamp_literal(time_hint.asof_utc)}"
    if time_hint.mode == "span":
        assert isinstance(time_hint.asof_utc, datetime)
        assert time_hint.history_span is not None
        if time_hint.history_span.total_seconds() == 0:
            return (
                f" FOR VALID_TIME AS OF {_xtdb_timestamp_literal(time_hint.asof_utc)}"
            )
        start_date_utc = time_hint.asof_utc - time_hint.history_span
        return (
            " FOR VALID_TIME BETWEEN "
            f"{_xtdb_timestamp_literal(start_date_utc)} AND "
            f"{_xtdb_timestamp_literal(time_hint.asof_utc)}"
        )

    raise ValueError(f"invalid TimeHint mode: {time_hint.mode}")


def _xtdb_single_primary_key_alias(table_config: TableConfig) -> str | None:
    if _xtdb_document_id_is_encoded(table_config):
        return None
    primary_keys = list(table_config.primary_keys)
    if len(primary_keys) != 1:
        return None
    primary_key = primary_keys[0]
    if primary_key != "_id":
        return primary_key
    return None


def _xtdb_table_query_output_columns(
    table_config: TableConfig,
    column_selection: list[str] | None,
) -> list[str]:
    if column_selection is not None:
        return column_selection

    columns = []
    single_key_alias = _xtdb_single_primary_key_alias(table_config)
    if single_key_alias is not None:
        columns.append(single_key_alias)

    columns.extend(
        column.name
        for column in table_config.columns
        if column.name not in {"_id", single_key_alias, *_XTDB_SYSTEM_COLUMNS}
    )
    columns.extend(["__valid_from", "__valid_to"])
    return columns


def _xtdb_table_query_select_expr(column: str, table_config: TableConfig) -> str:
    single_key_alias = _xtdb_single_primary_key_alias(table_config)
    if single_key_alias == column:
        return f"t._id AS {_xtdb_column_identifier(column)}"
    if column == "__valid_from":
        return "t._valid_from AS __valid_from"
    if column == "__valid_to":
        return "t._valid_to AS __valid_to"
    return f"t.{_xtdb_column_identifier(column)}"


def _xtdb_table_query_target_column(column: str, table_config: TableConfig) -> str:
    if _xtdb_single_primary_key_alias(table_config) == column:
        return "_id"
    return _xtdb_column_identifier(column)
