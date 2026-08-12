import math
import re
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any
from urllib.parse import quote

import polars as pl
import pyarrow as pa
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ..config import TableConfig
from ..utils.arrow import require_unique_arrow_field_names
from .config import DbEngineConfig

_XTDB_LAST_SYSTEM_TIME_KEY = "polars_hist_db_xtdb_last_system_time"
_XTDB_ACTIVE_TRANSACTION_KEY = "polars_hist_db_xtdb_active_transaction"
_XTDB_BUFFERED_TRANSACTION_KEY = "polars_hist_db_xtdb_buffered_transaction"
_XTDB_BUFFERING_PAUSED_KEY = "polars_hist_db_xtdb_buffering_paused"
_XTDB_RESERVED_IDENTIFIERS = {"flag", "timestamp"}
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _load_flight_sql() -> Any:
    try:
        from adbc_driver_flightsql import dbapi as flight_sql
    except ImportError as exc:
        raise RuntimeError(
            "XTDB ADBC support requires the 'xtdb' extra "
            "(adbc-driver-flightsql). Install with polars-hist-db[xtdb]."
        ) from exc
    return flight_sql


def _create_xtdb_engine(config: DbEngineConfig, engine_factory: Any) -> Engine:
    database = config.database or "xtdb"
    auth = ""
    if config.username:
        auth = quote(config.username, safe="")
        if config.password:
            auth = f"{auth}:{quote(config.password, safe='')}"
        auth = f"{auth}@"
    url = f"postgresql+psycopg://{auth}{config.hostname}:{config.port}/{database}"
    engine = engine_factory(
        url,
        connect_args={"prepare_threshold": None},
        pool_size=config.pool_size,
        max_overflow=config.max_overflow,
        use_native_hstore=False,
    )
    # XTDB pgwire currently trips SQLAlchemy's PostgreSQL dialect when it
    # probes SHOW standard_conforming_strings. XTDB uses standard strings,
    # so skip that PostgreSQL-specific initialization query.
    engine.dialect._set_backslash_escapes = lambda connection: setattr(  # type: ignore[attr-defined, method-assign]
        engine.dialect, "_backslash_escapes", False
    )
    return engine


@contextmanager
def _xtdb_connection_scope(engine: Engine) -> Iterator[Any]:
    """Open a scope for XTDB stores, which manage their own transactions."""
    with engine.connect() as connection:
        yield connection
        connection.commit()


def _xtdb_adbc_uri(config: DbEngineConfig) -> str:
    adbc_port = config.adbc_port or 9832
    return f"grpc://{config.hostname}:{adbc_port}"


def _create_xtdb_adbc_connection(config: DbEngineConfig, flight_loader: Any) -> Any:
    return flight_loader().connect(_xtdb_adbc_uri(config), autocommit=True)


def _close_xtdb_adbc_connection(connection: Any | None) -> None:
    if connection is not None:
        connection.close()


def __getattr__(name: str) -> Any:
    """Keep legacy private XTDB imports working during the split."""
    from . import xtdb_arrow, xtdb_dataframe, xtdb_query

    try:
        return getattr(xtdb_arrow, name)
    except AttributeError:
        try:
            return getattr(xtdb_query, name)
        except AttributeError:
            return getattr(xtdb_dataframe, name)


def _is_xtdb_adbc_ingest_unavailable(exc: Exception) -> bool:
    message = str(exc).lower()
    class_name = exc.__class__.__name__
    return (
        class_name in {"NotImplementedError", "NotSupportedError"}
        and "executeingest" in message
        and ("not implemented" in message or "not_implemented" in message)
    )


def _is_xtdb_table_not_found_error(exc: Exception) -> bool:
    return "Table not found:" in str(exc)


def _execute_xtdb_transaction(connection: Any, statements: Iterable[str]) -> None:
    """Submit one serialized XTDB DML transaction."""
    with _xtdb_transaction_scope(connection) as driver_connection:
        for statement in statements:
            driver_connection.execute(statement)


def _xtdb_transaction_active(connection: Any) -> bool:
    info = getattr(connection, "info", None)
    return isinstance(info, dict) and (
        _XTDB_ACTIVE_TRANSACTION_KEY in info or _XTDB_BUFFERED_TRANSACTION_KEY in info
    )


@contextmanager
def _xtdb_buffering_paused(connection: Any) -> Iterator[None]:
    info = getattr(connection, "info", None)
    if not isinstance(info, dict) or _XTDB_BUFFERED_TRANSACTION_KEY not in info:
        yield
        return
    info[_XTDB_BUFFERING_PAUSED_KEY] = True
    try:
        yield
    finally:
        info.pop(_XTDB_BUFFERING_PAUSED_KEY, None)


@contextmanager
def _xtdb_buffered_transaction_scope(
    connection: Any,
    system_time: datetime | None = None,
) -> Iterator[None]:
    """Buffer ingest DML so reads can finish before one atomic XTDB commit."""
    info = getattr(connection, "info", None)
    if not isinstance(info, dict):
        info = {}
        connection.info = info
    if _xtdb_transaction_active(connection):
        raise ValueError("nested XTDB transactions are not supported")

    operations: list[tuple[str, tuple[Any, ...]]] = []
    info[_XTDB_BUFFERED_TRANSACTION_KEY] = (system_time, operations)
    try:
        yield
    except BaseException:
        operations.clear()
        raise
    else:
        if operations:
            info.pop(_XTDB_BUFFERED_TRANSACTION_KEY, None)

            def flush(transaction_time: datetime | None) -> None:
                with _xtdb_transaction_scope(connection, transaction_time):
                    for operation, arguments in operations:
                        if operation == "dml":
                            sql, rows, parameters, operation_time = arguments
                            _execute_xtdb_dml(
                                connection,
                                sql,
                                rows,
                                parameters=parameters,
                                system_time=(
                                    operation_time
                                    if transaction_time is not None
                                    else None
                                ),
                            )
                        else:
                            table_sql, df, operation_time = arguments
                            _execute_xtdb_arrow_copy(
                                connection,
                                table_sql,
                                df,
                                system_time=(
                                    operation_time
                                    if transaction_time is not None
                                    else None
                                ),
                            )

            try:
                flush(system_time)
            except Exception as exc:
                if system_time is None or not _is_xtdb_invalid_system_time_error(exc):
                    raise
                flush(None)
    finally:
        info.pop(_XTDB_BUFFERED_TRANSACTION_KEY, None)
        info.pop(_XTDB_BUFFERING_PAUSED_KEY, None)


@contextmanager
def _xtdb_transaction_scope(
    connection: Any,
    system_time: datetime | None = None,
) -> Iterator[Any]:
    driver_connection = _driver_connection(connection)
    if driver_connection is None:
        raise ValueError("XTDB transactions require a live DBAPI connection")
    info = getattr(connection, "info", None)
    if not isinstance(info, dict):
        info = {}
        connection.info = info
    if _XTDB_ACTIVE_TRANSACTION_KEY in info:
        raise ValueError("nested XTDB transactions are not supported")

    _rollback_xtdb_connection(connection)
    autocommit = getattr(driver_connection, "autocommit", None)
    if autocommit is not None:
        driver_connection.autocommit = True
    requested_system_time = system_time
    begin_sql = "BEGIN READ WRITE"
    if system_time is not None:
        system_time = _next_xtdb_system_time(connection, system_time)
        if system_time is not None:
            begin_sql += f" WITH (SYSTEM_TIME = {_xtdb_timestamp_literal(system_time)})"
    try:
        driver_connection.execute(begin_sql)
    except Exception as exc:
        if system_time is None or not _is_xtdb_invalid_system_time_error(exc):
            if autocommit is not None:
                driver_connection.autocommit = autocommit
            raise
        driver_connection.rollback()
        driver_connection.execute("BEGIN READ WRITE")
    info[_XTDB_ACTIVE_TRANSACTION_KEY] = requested_system_time
    try:
        yield driver_connection
        driver_connection.execute("COMMIT")
    except BaseException:
        driver_connection.execute("ROLLBACK")
        driver_connection.rollback()
        raise
    finally:
        info.pop(_XTDB_ACTIVE_TRANSACTION_KEY, None)
        if autocommit is not None:
            driver_connection.autocommit = autocommit


def _validate_identifier(identifier: str) -> str:
    if not _IDENTIFIER_RE.match(identifier):
        raise ValueError(f"Unsupported XTDB identifier: {identifier!r}")
    return identifier


def _quote_identifier(identifier: str) -> str:
    if (
        _IDENTIFIER_RE.match(identifier)
        and identifier == identifier.lower()
        and identifier not in _XTDB_RESERVED_IDENTIFIERS
    ):
        return identifier
    return '"' + identifier.replace('"', '""') + '"'


def _xtdb_column_identifier(column_name: str) -> str:
    from .xtdb_arrow import _xtdb_physical_column_name

    return _quote_identifier(_xtdb_physical_column_name(column_name))


def _xtdb_sql_literal(value: Any, cast_type: str) -> str:
    if value is None:
        return f"NULL::{cast_type}"
    if isinstance(value, bool):
        return f"{'TRUE' if value else 'FALSE'}::{cast_type}"
    if isinstance(value, int):
        return f"{value}::{cast_type}"
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("XTDB insert does not support non-finite float values")
        return f"{value}::{cast_type}"
    if isinstance(value, Decimal):
        return f"{value}::{cast_type}"
    if isinstance(value, datetime):
        escaped = value.isoformat().replace("'", "''")
        return f"'{escaped}'::{cast_type}"
    if isinstance(value, date):
        escaped = value.isoformat().replace("'", "''")
        return f"'{escaped}'::{cast_type}"
    if isinstance(value, bytes):
        return f"X('{value.hex()}')::{cast_type}"

    escaped = str(value).replace("'", "''")
    return f"'{escaped}'::{cast_type}"


def _qualified_table_name(table_schema: str, table_name: str) -> str:
    return f"{_validate_identifier(table_schema)}.{_validate_identifier(table_name)}"


def _driver_connection(connection: Any) -> Any | None:
    proxied_connection = getattr(connection, "connection", None)
    return getattr(proxied_connection, "driver_connection", None)


def _xtdb_timestamp_literal(value: datetime) -> str:
    escaped = value.isoformat().replace("'", "''")
    timestamp_type = "TIMESTAMP WITH TIME ZONE" if value.tzinfo else "TIMESTAMP"
    return f"{timestamp_type} '{escaped}'"


def _latest_xtdb_system_time(connection: Any) -> datetime | None:
    execute = getattr(connection, "execute", None)
    if not callable(execute):
        return None
    result = execute(
        text("SELECT system_time FROM xt.txs ORDER BY system_time DESC LIMIT 1")
    )
    latest = result.scalar_one_or_none()
    _rollback_xtdb_connection(connection)
    return latest if isinstance(latest, datetime) else None


def _next_xtdb_system_time(connection: Any, system_time: datetime) -> datetime | None:
    info = getattr(connection, "info", None)
    if not isinstance(info, dict):
        return system_time

    last_system_time = info.get(_XTDB_LAST_SYSTEM_TIME_KEY)
    if not isinstance(last_system_time, datetime):
        latest_system_time = _latest_xtdb_system_time(connection)
        if latest_system_time is not None and system_time < latest_system_time:
            return None
    if isinstance(last_system_time, datetime) and system_time <= last_system_time:
        system_time = last_system_time + timedelta(microseconds=1)
    info[_XTDB_LAST_SYSTEM_TIME_KEY] = system_time
    return system_time


def _is_xtdb_invalid_system_time_error(exc: Exception) -> bool:
    message = str(exc)
    return "invalid-system-time" in message or "specified system-time older" in message


def _rollback_xtdb_connection(connection: Any) -> None:
    rollback = getattr(connection, "rollback", None)
    if callable(rollback):
        rollback()


def _configure_xtdb_pgwire_parameter_adapters(driver_connection: Any) -> None:
    adapters = getattr(driver_connection, "adapters", None)
    register_dumper = getattr(adapters, "register_dumper", None)
    if not callable(register_dumper):
        return

    try:
        from psycopg.types.string import StrDumper
    except ModuleNotFoundError:
        return

    register_dumper(str, StrDumper)


def _xtdb_parameter_value(value: Any, cast_type: str) -> Any:
    if (
        isinstance(value, datetime)
        and cast_type.upper().startswith("TIMESTAMP")
        and value.tzinfo is not None
    ):
        value = value.astimezone(UTC)
        if "WITH TIME ZONE" not in cast_type.upper():
            value = value.replace(tzinfo=None)
    return value


def _normalize_xtdb_timestamp_columns(
    df: pl.DataFrame,
    table_config: TableConfig | None,
) -> pl.DataFrame:
    from .xtdb_arrow import _xtdb_insert_casts

    casts = _xtdb_insert_casts(df, table_config)
    expressions = []
    for column, cast_type in zip(df.columns, casts, strict=True):
        dtype = df.schema[column]
        if (
            isinstance(dtype, pl.Datetime)
            and dtype.time_zone is not None
            and cast_type.upper().startswith("TIMESTAMP")
        ):
            expression = pl.col(column).dt.convert_time_zone("UTC")
            if "WITH TIME ZONE" not in cast_type.upper():
                expression = expression.dt.replace_time_zone(None)
            expressions.append(expression)
    if not expressions:
        return df
    return df.with_columns(expressions)


def _execute_xtdb_dml(
    connection: Any,
    sql: str,
    rows: list[tuple[Any, ...]] | None = None,
    *,
    parameters: tuple[Any, ...] | None = None,
    system_time: datetime | None = None,
) -> int:
    if rows is not None and parameters is not None:
        raise ValueError("XTDB DML accepts rows or parameters, not both")
    info = getattr(connection, "info", None)
    if (
        isinstance(info, dict)
        and _XTDB_BUFFERED_TRANSACTION_KEY in info
        and _XTDB_BUFFERING_PAUSED_KEY not in info
    ):
        transaction_time, operations = info[_XTDB_BUFFERED_TRANSACTION_KEY]
        if system_time is not None and transaction_time != system_time:
            raise ValueError("XTDB transaction cannot mix system times")
        operations.append(("dml", (sql, rows, parameters, system_time)))
        return len(rows) if rows is not None else 0

    driver_connection = _driver_connection(connection)
    if driver_connection is None:
        if rows is not None or parameters is not None or system_time is not None:
            raise ValueError(
                "XTDB dataframe writes with rows or system-time require a live "
                "DBAPI connection"
            )
        connection.execute(text(sql))
        return 0

    info = getattr(connection, "info", None)
    if isinstance(info, dict) and _XTDB_ACTIVE_TRANSACTION_KEY in info:
        transaction_time = info[_XTDB_ACTIVE_TRANSACTION_KEY]
        if system_time is not None and transaction_time != system_time:
            raise ValueError("XTDB transaction cannot mix system times")
        if rows is None and parameters is None:
            driver_connection.execute(sql)
            return 0
        _configure_xtdb_pgwire_parameter_adapters(driver_connection)
        if parameters is not None:
            driver_connection.execute(sql, parameters)
            return 0
        assert rows is not None
        cursor = driver_connection.cursor()
        try:
            cursor.executemany(sql, rows)
        finally:
            close = getattr(cursor, "close", None)
            if callable(close):
                close()
        return len(rows)

    _rollback_xtdb_connection(connection)
    autocommit = getattr(driver_connection, "autocommit", None)
    if autocommit is not None:
        driver_connection.autocommit = True

    begin_sql = "BEGIN READ WRITE"
    if system_time is not None:
        system_time = _next_xtdb_system_time(connection, system_time)
        if system_time is not None:
            begin_sql = (
                "BEGIN READ WRITE WITH "
                f"(SYSTEM_TIME = {_xtdb_timestamp_literal(system_time)})"
            )
    driver_connection.execute(begin_sql)
    try:
        if rows is None and parameters is None:
            driver_connection.execute(sql)
            row_count = 0
        elif parameters is not None:
            _configure_xtdb_pgwire_parameter_adapters(driver_connection)
            driver_connection.execute(sql, parameters)
            row_count = 0
        else:
            assert rows is not None
            _configure_xtdb_pgwire_parameter_adapters(driver_connection)
            cursor = driver_connection.cursor()
            try:
                cursor.executemany(sql, rows)
            finally:
                close = getattr(cursor, "close", None)
                if callable(close):
                    close()
            row_count = len(rows)
        driver_connection.execute("COMMIT")
    except Exception as exc:
        driver_connection.execute("ROLLBACK")
        driver_connection.rollback()
        if system_time is not None and _is_xtdb_invalid_system_time_error(exc):
            return _execute_xtdb_dml(
                connection,
                sql,
                rows,
                parameters=parameters,
                system_time=None,
            )
        raise
    finally:
        if autocommit is not None:
            driver_connection.autocommit = autocommit
    return row_count


def _execute_xtdb_arrow_copy(
    connection: Any,
    table_sql: str,
    df: pl.DataFrame,
    *,
    system_time: datetime | None = None,
) -> None:
    info = getattr(connection, "info", None)
    if (
        isinstance(info, dict)
        and _XTDB_BUFFERED_TRANSACTION_KEY in info
        and _XTDB_BUFFERING_PAUSED_KEY not in info
    ):
        transaction_time, operations = info[_XTDB_BUFFERED_TRANSACTION_KEY]
        if system_time is not None and transaction_time != system_time:
            raise ValueError("XTDB transaction cannot mix system times")
        operations.append(("arrow", (table_sql, df, system_time)))
        return

    from .xtdb_arrow import _normalize_xtdb_ingest_arrow

    driver_connection = _driver_connection(connection)
    if driver_connection is None:
        raise ValueError("XTDB Arrow COPY requires a live DBAPI connection")

    info = getattr(connection, "info", None)
    active_transaction = isinstance(info, dict) and _XTDB_ACTIVE_TRANSACTION_KEY in info
    if active_transaction and system_time is not None:
        assert isinstance(info, dict)
        if info[_XTDB_ACTIVE_TRANSACTION_KEY] != system_time:
            raise ValueError("XTDB transaction cannot mix system times")

    autocommit = None
    begin_sql = "BEGIN READ WRITE"
    if not active_transaction:
        _rollback_xtdb_connection(connection)
        autocommit = getattr(driver_connection, "autocommit", None)
        if autocommit is not None:
            driver_connection.autocommit = True
        if system_time is not None:
            system_time = _next_xtdb_system_time(connection, system_time)
            if system_time is not None:
                begin_sql = (
                    "BEGIN READ WRITE WITH "
                    f"(SYSTEM_TIME = {_xtdb_timestamp_literal(system_time)})"
                )

    arrow_table = _normalize_xtdb_ingest_arrow(df.to_arrow())
    require_unique_arrow_field_names(
        arrow_table.schema, context="XTDB Arrow COPY schema"
    )
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, arrow_table.schema) as writer:
        writer.write_table(arrow_table)

    if active_transaction:
        cursor = driver_connection.cursor()
        try:
            with cursor.copy(
                f"COPY {table_sql} FROM STDIN WITH (FORMAT 'arrow-stream')"
            ) as copy:
                copy.write(sink.getvalue().to_pybytes())
        finally:
            close = getattr(cursor, "close", None)
            if callable(close):
                close()
        return

    driver_connection.execute(begin_sql)
    try:
        cursor = driver_connection.cursor()
        try:
            with cursor.copy(
                f"COPY {table_sql} FROM STDIN WITH (FORMAT 'arrow-stream')"
            ) as copy:
                copy.write(sink.getvalue().to_pybytes())
        finally:
            close = getattr(cursor, "close", None)
            if callable(close):
                close()
        driver_connection.execute("COMMIT")
    except Exception as exc:
        driver_connection.execute("ROLLBACK")
        driver_connection.rollback()
        if system_time is not None and _is_xtdb_invalid_system_time_error(exc):
            _execute_xtdb_arrow_copy(connection, table_sql, df, system_time=None)
            return
        raise
    finally:
        if autocommit is not None:
            driver_connection.autocommit = autocommit
