import dateutil.parser
import logging
import re
from typing import Collection, Dict, List, Mapping, Optional, Tuple, Union

import polars as pl
from sql_metadata import Parser
from sqlalchemy import (
    ClauseElement,
    ColumnClause,
    Compiled,
    Connection,
    MetaData,
    Table,
)
import sqlalchemy
from sqlalchemy.types import TypeEngine
from sqlalchemy.sql.sqltypes import NullType

LOGGER = logging.getLogger(__name__)

# This mapping is used by all three converters.
TYPE_PRIORITY_MAP: List[Tuple[str, pl.DataType, TypeEngine]] = [
    ("BIGINT", pl.Int64(), sqlalchemy.types.BIGINT()),
    ("BIT", pl.Int32(), sqlalchemy.types.Integer()),
    ("BOOL", pl.Boolean(), sqlalchemy.types.Boolean()),
    ("CHAR", pl.Utf8(), sqlalchemy.types.CHAR()),
    ("DATETIME", pl.Datetime(), sqlalchemy.types.DATETIME()),
    ("DATE", pl.Date(), sqlalchemy.types.DATE()),
    ("DOUBLE", pl.Float64(), sqlalchemy.types.DOUBLE()),
    ("FLOAT", pl.Float32(), sqlalchemy.types.FLOAT()),
    ("INT", pl.Int32(), sqlalchemy.types.INTEGER()),
    ("REAL", pl.Float64(), sqlalchemy.types.REAL()),
    ("TIMESTAMP", pl.Datetime(), sqlalchemy.types.TIMESTAMP()),
    ("TIME", pl.Time(), sqlalchemy.types.TIME()),
    ("TINYINT(DISPLAY_WIDTH=1)", pl.Boolean(), sqlalchemy.types.BOOLEAN()),
    ("TINYINT", pl.Int32(), sqlalchemy.types.INTEGER()),
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
    ) -> Tuple[str, List[str], Dict[str, str]]:
        pattern = r"^(?P<type>[A-Z]+)[(](?P<params>.*)[)]"
        sql_type = sql_type.upper()
        m = re.match(pattern, sql_type)
        if m is None:
            return sql_type, [], dict()

        type_name = m.group("type")
        params = m.group("params").lower()
        if "=" not in params:
            return type_name, params.split(","), dict()

        param_dict = dict()
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
        idx = _TypeConversionUtils._rowidx_from_polars_type(pl_dtype)
        if idx >= 0:
            return TYPE_PRIORITY_MAP[idx][0]

        # For a Decimal type, we expect an instance with precision and scale.
        if isinstance(pl_dtype, pl.Decimal):
            return f"NUMERIC({pl_dtype.precision},{pl_dtype.scale})"

        if pl_dtype in [pl.Utf8, pl.String, pl.Categorical]:
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
        if type_name in ["VARCHAR", "TEXT"]:
            return pl.Utf8()

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
    ) -> Dict[str, pl.DataType]:
        dtype_schema: Dict[str, pl.DataType] = {}
        for fqtn in Parser(sql_statement).tables:
            table_schema, table_name = fqtn.split(".")
            metadata = MetaData(schema=table_schema)
            tbl = Table(table_name, metadata, autoload_with=connection)
            table_dtype_schema = PolarsType._get_polars_dtypes_from_table(tbl)
            dtype_schema.update(table_dtype_schema)
        return dtype_schema

    @staticmethod
    def get_dataframe_schema_from_selectable(
        selectable: Union[Optional[ClauseElement], Compiled],
    ) -> Dict[str, pl.DataType]:
        dtype_schema: Dict[str, pl.DataType] = {}
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

        if target_type.is_integer():
            df = df.with_columns(pl.col(col).cast(pl.Float64).cast(target_type))
        elif target_type.is_decimal():
            assert isinstance(target_type, pl.Decimal)
            if target_type.scale == 0:
                df = df.with_columns(
                    pl.col(col).cast(pl.Float64).cast(pl.Int64).cast(target_type)
                )
        elif isinstance(target_type, pl.Datetime):
            df = df.with_columns(pl.col(col).str.to_datetime())
        elif isinstance(target_type, pl.Date):
            df = df.with_columns(pl.col(col).str.to_date())
        else:
            df = df.with_columns(pl.col(col).cast(target_type))
        return df

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
            except Exception as e:
                LOGGER.exception(
                    "Failed to type column %s with type %s",
                    col_name,
                    target_type,
                    exc_info=e,
                )
        df = PolarsType.cast_str_to_cat(df)
        return df

    @staticmethod
    def cast_str_to_cat(
        df: pl.DataFrame, ignore_cols: Optional[Collection[str]] = None
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
        idx = _TypeConversionUtils._rowidx_from_sql_type(t)
        if idx >= 0:
            return TYPE_PRIORITY_MAP[idx][2]

        type_name, params, param_dict = _TypeConversionUtils._parse_parameterised_type(
            t
        )
        if type_name in ["VARCHAR", "TEXT"]:
            length = int(param_dict.get("length") or param_dict.get("a") or params[0])
            return sqlalchemy.types.VARCHAR(length=length)

        if type_name in ["NUMERIC", "DECIMAL", "DEC", "FIXED"]:
            precision = int(
                param_dict.get("precision") or param_dict.get("m") or params[0]
            )
            scale = int(param_dict.get("scale") or param_dict.get("d") or params[1])
            return sqlalchemy.types.NUMERIC(precision=precision, scale=scale)

        raise ValueError(f"Unhandled SQL type: {sql_type}")
