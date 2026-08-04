import logging
import re
from collections.abc import Collection, Mapping
from typing import cast

import dateutil.parser
import polars as pl
import sqlalchemy
from sql_metadata import Parser
from sqlalchemy import (
    ClauseElement,
    ColumnClause,
    Compiled,
    Connection,
    MetaData,
    Table,
)
from sqlalchemy.dialects import mysql
from sqlalchemy.sql.sqltypes import NullType
from sqlalchemy.types import TypeEngine

from .observability import record_database_type_contract
from .utils.exceptions import NonRetryableException

LOGGER = logging.getLogger(__name__)


class TypeContractError(TypeError, NonRetryableException):
    """A DataFrame does not satisfy a database table's declared types."""


def is_polars_type(
    dtype: pl.DataType,
    *candidates: pl.DataType | type[pl.DataType],
) -> bool:
    """Compare Polars dtypes without relying on inconsistent dtype hashing."""
    return any(dtype == candidate for candidate in candidates)


def _polars_type_family(dtype: pl.DataType) -> str:
    if is_polars_type(dtype, pl.String, pl.Utf8, pl.Categorical):
        return "string"
    if dtype.is_integer():
        return "integer"
    if dtype.is_float():
        return "float"
    if dtype.is_decimal():
        return "decimal"
    if dtype == pl.Boolean:
        return "boolean"
    if dtype == pl.Date:
        return "date"
    if dtype == pl.Time:
        return "time"
    if isinstance(dtype, pl.Datetime):
        return "timestamp_tz" if dtype.time_zone is not None else "timestamp"
    if dtype == pl.Null:
        return "null"
    return "other"


# This mapping is used by all three converters.
TYPE_PRIORITY_MAP: list[tuple[str, pl.DataType, TypeEngine]] = [
    ("BIGINT", pl.Int64(), sqlalchemy.types.BIGINT()),
    ("BINARY", pl.Binary(), sqlalchemy.types.BINARY()),
    ("BIT", pl.Int32(), sqlalchemy.types.Integer()),
    ("BOOL", pl.Boolean(), sqlalchemy.types.Boolean()),
    ("BLOB", pl.Binary(), sqlalchemy.types.LargeBinary()),
    ("CHAR", pl.Utf8(), sqlalchemy.types.CHAR()),
    ("DATETIME", pl.Datetime(), sqlalchemy.types.DATETIME()),
    ("DATE", pl.Date(), sqlalchemy.types.DATE()),
    ("DOUBLE", pl.Float64(), sqlalchemy.types.DOUBLE()),
    ("FLOAT", pl.Float32(), sqlalchemy.types.FLOAT()),
    ("INT", pl.Int32(), sqlalchemy.types.INTEGER()),
    ("JSON", pl.Utf8(), sqlalchemy.types.JSON()),
    ("REAL", pl.Float64(), sqlalchemy.types.REAL()),
    (
        "TIMESTAMP WITH TIME ZONE",
        pl.Datetime("us", "UTC"),
        sqlalchemy.types.TIMESTAMP(timezone=True),
    ),
    (
        "TIMESTAMPTZ",
        pl.Datetime("us", "UTC"),
        sqlalchemy.types.TIMESTAMP(timezone=True),
    ),
    ("TIMESTAMP", pl.Datetime(), sqlalchemy.types.TIMESTAMP()),
    ("TIME", pl.Time(), sqlalchemy.types.TIME()),
    ("TINYINT(DISPLAY_WIDTH=1)", pl.Boolean(), sqlalchemy.types.BOOLEAN()),
    ("TINYINT", pl.Int32(), sqlalchemy.types.INTEGER()),
    ("VARBINARY", pl.Binary(), sqlalchemy.types.VARBINARY()),
    ("MEDIUMINT", pl.Int64(), sqlalchemy.types.BIGINT()),
    ("SMALLINT", pl.Int32(), sqlalchemy.types.INTEGER()),
]


# -----------------------------------------------------------------------------
# Private utility class for shared conversion helpers.
# -----------------------------------------------------------------------------
class _TypeConversionUtils:
    @staticmethod
    def _rowidx_from_sql_type(t: str) -> int:
        for i, (sql_type, _, _) in enumerate(TYPE_PRIORITY_MAP):
            if t.startswith(sql_type):
                return i
        return -1

    @staticmethod
    def _rowidx_from_polars_type(t: pl.DataType) -> int:
        for i, (_, pl_type, _) in enumerate(TYPE_PRIORITY_MAP):
            if t == pl_type:
                return i
        return -1

    @staticmethod
    def _parse_parameterised_type(
        sql_type: str,
    ) -> tuple[str, list[str], dict[str, str]]:
        pattern = r"^(?P<type>[A-Z]+)[(](?P<params>.*)[)]"
        sql_type = sql_type.upper()
        m = re.match(pattern, sql_type)
        if m is None:
            return sql_type, [], {}

        type_name = m.group("type")
        params = m.group("params").lower()
        if "=" not in params:
            return type_name, params.split(","), {}

        param_dict = {}
        for param in params.split(","):
            key, value = param.split("=")
            param_dict[key.strip()] = value.strip()
        return type_name, [], param_dict


# -----------------------------------------------------------------------------
# Converter for SQL type strings (from Polars types)
# -----------------------------------------------------------------------------
class SQLType:
    @staticmethod
    def from_polars(pl_dtype: pl.DataType, default_varchar_length: int = 255) -> str:
        if isinstance(pl_dtype, pl.List):
            inner = cast(pl.DataType, pl_dtype.inner)
            return f"ARRAY({SQLType.from_polars(inner, default_varchar_length)})"

        idx = _TypeConversionUtils._rowidx_from_polars_type(pl_dtype)
        if idx >= 0:
            return TYPE_PRIORITY_MAP[idx][0]

        # For a Decimal type, we expect an instance with precision and scale.
        if isinstance(pl_dtype, pl.Decimal):
            return f"NUMERIC({pl_dtype.precision},{pl_dtype.scale})"

        if is_polars_type(pl_dtype, pl.Utf8, pl.String, pl.Categorical):
            return f"VARCHAR({default_varchar_length})"

        raise ValueError(f"Unknown Polars data type: {pl_dtype}.")

    @staticmethod
    def from_table(tbl: Table) -> Mapping[str, str]:
        sql_type_schema = {}
        for col_cfg in tbl.columns:
            col_name = col_cfg.name
            col_sql_type = repr(col_cfg.type)
            sql_type_schema[col_name] = col_sql_type
        return sql_type_schema


# -----------------------------------------------------------------------------
# Converter for Polars types (from SQL type strings)
# -----------------------------------------------------------------------------
class PolarsType:
    @staticmethod
    def from_sql(sql_type: str) -> pl.DataType:
        t = sql_type.upper()
        idx = _TypeConversionUtils._rowidx_from_sql_type(t)
        if idx >= 0:
            return TYPE_PRIORITY_MAP[idx][1]

        type_name, params, param_dict = _TypeConversionUtils._parse_parameterised_type(
            t
        )
        if type_name == "ARRAY" and len(params) == 1:
            return pl.List(PolarsType.from_sql(params[0]))

        if type_name == "VARCHAR" or type_name.endswith("TEXT"):
            return pl.Utf8()

        if type_name in {"BINARY", "VARBINARY"} or type_name.endswith("BLOB"):
            return pl.Binary()

        if type_name in ["NUMERIC", "DECIMAL", "DEC", "FIXED"]:
            precision = int(
                param_dict.get("precision") or param_dict.get("m") or params[0]
            )
            scale = int(param_dict.get("scale") or param_dict.get("d") or params[1])
            return pl.Decimal(precision=precision, scale=scale)

        raise ValueError(f"Unknown SQL data type: {sql_type}.")

    @staticmethod
    def get_dataframe_schema_from_sqltext(
        sql_statement: str, connection: Connection
    ) -> dict[str, pl.DataType]:
        dtype_schema: dict[str, pl.DataType] = {}
        for fqtn in Parser(sql_statement).tables:
            table_schema, table_name = fqtn.split(".")
            metadata = MetaData(schema=table_schema)
            tbl = Table(table_name, metadata, autoload_with=connection)
            table_dtype_schema = PolarsType._get_polars_dtypes_from_table(tbl)
            dtype_schema.update(table_dtype_schema)
        return dtype_schema

    @staticmethod
    def get_dataframe_schema_from_selectable(
        selectable: ClauseElement | None | Compiled,
    ) -> dict[str, pl.DataType]:
        dtype_schema: dict[str, pl.DataType] = {}
        unknown_types = {}
        if isinstance(selectable, Compiled):
            selectable = selectable.statement
        assert isinstance(selectable, ClauseElement)

        for clause_element in selectable.get_children():
            if isinstance(clause_element, ColumnClause):
                col_name = clause_element.name
                col_type = clause_element.type
                if isinstance(col_type, NullType):
                    unknown_types[col_name] = col_type
                else:
                    dtype_schema[col_name] = PolarsType.from_sql(repr(col_type))
            elif isinstance(clause_element, Table):
                table_dtype_schema = PolarsType._get_polars_dtypes_from_table(
                    clause_element
                )
                dtype_schema.update(table_dtype_schema)
            # continue iterating other children
        if unknown_types:
            LOGGER.error("Unable to determine types of columns %s", unknown_types)
        return dtype_schema

    @staticmethod
    def _get_polars_dtypes_from_table(tbl: Table) -> Mapping[str, pl.DataType]:
        sql_types = SQLType.from_table(tbl)
        return {
            name: PolarsType.from_sql(sql_type) for name, sql_type in sql_types.items()
        }

    @staticmethod
    def apply_dtype_to_column(
        df: pl.DataFrame, col: str, target_type: pl.DataType
    ) -> pl.DataFrame:
        # not able to work directly on a pl.Expr
        # https://github.com/pola-rs/polars/issues/16974

        if df[col].dtype == target_type:
            return df

        source = pl.col(col)
        source_type = df.schema[col]
        if isinstance(target_type, pl.Datetime) and is_polars_type(
            source_type, pl.String
        ):
            source = source.str.to_datetime(
                time_unit=target_type.time_unit,
                time_zone=target_type.time_zone,
                strict=True,
            )
        elif isinstance(target_type, pl.Date) and is_polars_type(
            source_type, pl.String
        ):
            source = source.str.to_date(strict=True)
        else:
            source = source.cast(target_type, strict=True)
        return df.with_columns(source)

    @staticmethod
    def apply_schema_to_dataframe(
        df: pl.DataFrame, **schema_overrides: pl.DataType
    ) -> pl.DataFrame:
        for col_name in df.columns:
            try:
                if col_name not in schema_overrides:
                    continue
                target_type = schema_overrides[col_name]
                df = PolarsType.apply_dtype_to_column(df, col_name, target_type)
            except Exception:
                LOGGER.exception(
                    "Failed to type column %s with type %s",
                    col_name,
                    target_type,
                )
                raise
        df = PolarsType.cast_str_to_cat(df)
        return df

    @staticmethod
    def enforce_database_schema(
        df: pl.DataFrame,
        expected_schema: Mapping[str, pl.DataType],
        *,
        backend: str,
        operation: str,
        force_type_coercion: bool = False,
    ) -> pl.DataFrame:
        """Fail closed on implicit database-boundary type conversions."""
        result = df
        for column, expected in expected_schema.items():
            if column not in df.columns:
                continue
            actual = df.schema[column]
            actual_family = _polars_type_family(actual)
            expected_family = _polars_type_family(expected)
            if actual == expected or actual_family == expected_family == "string":
                continue
            if actual == pl.Null:
                result = result.with_columns(pl.col(column).cast(expected))
                continue

            message = (
                f"{backend} {operation} type contract failed for column {column!r}: "
                f"expected {expected}, received {actual}"
            )
            if not force_type_coercion:
                record_database_type_contract(
                    backend=backend,
                    operation=operation,
                    expected_type=_polars_type_family(expected),
                    actual_type=_polars_type_family(actual),
                    forced=False,
                    outcome="rejected",
                )
                LOGGER.error(message)
                raise TypeContractError(
                    f"{message}; pass force_type_coercion=True for an explicit "
                    "strict conversion"
                )
            LOGGER.warning("%s; applying explicit strict conversion", message)
            source = pl.col(column)
            if is_polars_type(actual, pl.Categorical) and expected_family != "string":
                source = source.cast(pl.String)
            try:
                result = result.with_columns(source.cast(expected, strict=True))
            except Exception:
                record_database_type_contract(
                    backend=backend,
                    operation=operation,
                    expected_type=_polars_type_family(expected),
                    actual_type=_polars_type_family(actual),
                    forced=True,
                    outcome="failed",
                )
                raise
            record_database_type_contract(
                backend=backend,
                operation=operation,
                expected_type=_polars_type_family(expected),
                actual_type=_polars_type_family(actual),
                forced=True,
                outcome="coerced",
            )

        return result

    @staticmethod
    def cast_str_to_cat(
        df: pl.DataFrame, ignore_cols: Collection[str] | None = None
    ) -> pl.DataFrame:
        if ignore_cols is None:
            ignore_cols = []
        df = df.with_columns(
            pl.col([pl.String, pl.Utf8]).exclude(ignore_cols).cast(pl.Categorical)
        )
        return df

    @staticmethod
    def convert_str_value(v: str, target_type: pl.DataType) -> pl.Expr:
        if target_type == pl.Boolean:
            bool_map = {
                "false": False,
                "true": True,
                "0": False,
                "1": True,
                "f": False,
                "t": True,
            }
            return pl.lit(v.lower()).replace_strict(bool_map).cast(pl.Boolean)
        if target_type.is_temporal():
            temporal_v = dateutil.parser.parse(v)
            return pl.lit(temporal_v).cast(target_type)
        return pl.lit(v).cast(target_type)


# -----------------------------------------------------------------------------
# Converter for SQLAlchemy types (from SQL type strings)
# -----------------------------------------------------------------------------
class SQLAlchemyType:
    @staticmethod
    def from_sql(sql_type: str) -> TypeEngine:
        t = sql_type.upper()
        type_name, params, param_dict = _TypeConversionUtils._parse_parameterised_type(
            t
        )
        if type_name == "BINARY":
            return sqlalchemy.types.BINARY(length=int(params[0]) if params else None)
        if type_name == "VARBINARY":
            return sqlalchemy.types.VARBINARY(length=int(params[0]) if params else None)
        if type_name == "DATETIME" and params and params[0]:
            return mysql.DATETIME(fsp=int(params[0]))
        idx = _TypeConversionUtils._rowidx_from_sql_type(t)
        if idx >= 0:
            return TYPE_PRIORITY_MAP[idx][2]

        if type_name == "VARCHAR":
            length = int(param_dict.get("length") or param_dict.get("a") or params[0])
            return sqlalchemy.types.VARCHAR(length=length)
        if type_name.endswith("BLOB"):
            return sqlalchemy.types.LargeBinary()
        if type_name == "TEXT":
            return sqlalchemy.types.TEXT()
        if type_name == "MEDIUMTEXT":
            return mysql.MEDIUMTEXT()
        if type_name == "LONGTEXT":
            return mysql.LONGTEXT()

        if type_name in ["NUMERIC", "DECIMAL", "DEC", "FIXED"]:
            precision = int(
                param_dict.get("precision") or param_dict.get("m") or params[0]
            )
            scale = int(param_dict.get("scale") or param_dict.get("d") or params[1])
            return sqlalchemy.types.NUMERIC(precision=precision, scale=scale)

        raise ValueError(f"Unhandled SQL type: {sql_type}")
