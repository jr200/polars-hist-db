from datetime import datetime, timezone
import logging
import time
from typing import Any, Optional

import polars as pl
from sqlalchemy import (
    Connection,
    CursorResult,
    Executable,
    text,
)
from sqlalchemy.engine.interfaces import _CoreAnyExecuteParams

from ..utils.clock import Clock


LOGGER = logging.getLogger(__name__)


class DbOps:
    def __init__(self, connection: Connection):
        self.connection = connection

    def db_create(self, table_schema: str) -> None:
        sql = f"""
        CREATE DATABASE IF NOT EXISTS {table_schema}
        CHARACTER SET utf8mb4 
        COLLATE utf8mb4_unicode_ci
        """
        _result = self.execute_sqlalchemy(
            f"sql.base.create_db.{table_schema}", text(sql)
        )

    @staticmethod
    def enable_engine_logging(level: int):
        logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)

    def execute_sqlalchemy(
        self,
        description: str,
        statement: Executable,
        parameters: Optional[_CoreAnyExecuteParams] = None,
        disable_foreign_key_checks: bool = False,
        disable_keys: Optional[str] = None,
    ) -> CursorResult[Any]:
        timings = Clock()
        try:
            if disable_foreign_key_checks:
                self.connection.execute(text("SET FOREIGN_KEY_CHECKS=0;"))

            if disable_keys:
                self.connection.execute(
                    text(f"ALTER TABLE {disable_keys} DISABLE KEYS;")
                )

            start_time = time.perf_counter()
            result = self.connection.execute(statement, parameters)
        finally:
            if disable_keys:
                self.connection.execute(
                    text(f"ALTER TABLE {disable_keys} ENABLE KEYS;")
                )

            if disable_foreign_key_checks:
                self.connection.execute(text("SET FOREIGN_KEY_CHECKS=1;"))

        sql_time = time.perf_counter() - start_time
        timings.add_timing(description, sql_time)
        return result

    def get_all_variables(self, filter: Optional[str] = None) -> pl.DataFrame:
        if filter is None:
            sql = text("SHOW variables;")
        else:
            sql = text(f"SHOW variables like '{filter}';")

        result = self.execute_sqlalchemy("sql.op.get_all_variables", sql).fetchall()
        df = pl.from_dicts([{k: v for k, v in result}])
        return df

    def get_system_versioning_time(self) -> datetime:
        t_as_str = self.get_all_variables("timestamp").item()
        t = datetime.fromtimestamp(float(t_as_str), tz=timezone.utc)
        return t

    def set_system_versioning_time(self, t: Optional[datetime]):
        if t is None:
            arg = "DEFAULT"
        else:
            arg = f"UNIX_TIMESTAMP('{t.isoformat()}')"

        sql = text(f"SET @@timestamp = {arg};")
        _result = self.execute_sqlalchemy("sql.op.set_system_versioning_time", sql)

        LOGGER.debug("System versioning time set to: %s", arg)
