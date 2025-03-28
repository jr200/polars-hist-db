import csv
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from io import StringIO
import os
from pathlib import Path
import random
import tempfile
import textwrap
from types import MappingProxyType
from typing import Any, Dict, List, Literal, Optional, Tuple

import polars as pl
import sqlalchemy
from sqlalchemy import Engine, Select, select
from sqlalchemy.dialects import mysql

from polars_db_engine.loaders import load_typed_dsv
from polars_db_engine.config import BaseConfig, TableConfig, TableConfigs
from polars_db_engine.core import (
    AuditOps,
    DataframeOps,
    DbOps,
    DeltaTableOps,
    TableConfigOps,
    TableOps,
    make_engine,
)
from polars_db_engine.config.parser import parse_col_exprs
from polars_db_engine.types import SQLAlchemyType, PolarsType

# some test defaults for debugging
pl.Config(
    set_tbl_cols=100,
    fmt_str_lengths=1000,
    tbl_width_chars=1000,
)


def _tests_dir():
    tests_dir = Path(__file__).parent.absolute()
    return tests_dir


def get_table_config(filename: str):
    data_dir = _tests_dir() / "_testdata_table_configs"
    return data_dir / filename


def get_dataset_config(filename: str):
    data_dir = _tests_dir() / "_testdata_dataset_configs"
    return data_dir / filename


def get_dataset_data(filename: str):
    data_dir = _tests_dir() / "_testdata_dataset_data"
    return data_dir / filename


def mariadb_engine_test(**kwargs) -> Engine:
    engine = make_engine(
        backend="mariadb",
        hostname="127.0.0.1",
        port=3306,
        username="root",
        password="admin",
        **kwargs,
    )

    return engine


def create_temp_file_tree(dircnt: int, depth: int, filecnt: int):
    print(
        f"Create temporary directory with {dircnt} directories with depth {depth} and {3 * filecnt} files"
    )
    tempDir = tempfile.TemporaryDirectory(prefix="scandir_rs_")  # noqa: F821
    for dn in range(dircnt):
        dirName = f"{tempDir.name}/dir{dn}"
        for depth in range(depth):
            os.makedirs(dirName)
            for fn in range(filecnt):
                open(f"{dirName}/file{fn}.bin", "wb").close()
                open(f"{dirName}/file{fn}.txt", "wb").close()
                open(f"{dirName}/file{fn}.log", "wb").close()
            dirName = f"{dirName}/dir{depth}"
    return tempDir


def from_test_result(x: str, table_config: TableConfig) -> pl.DataFrame:
    def _clean_csv_data(data: str) -> str:
        input_io = StringIO(data.strip())
        output_io = StringIO()

        reader = csv.reader(input_io)
        writer = csv.writer(output_io, quoting=csv.QUOTE_MINIMAL)

        for row in reader:
            cleaned_row = [field.strip() if field is not None else "" for field in row]
            writer.writerow(cleaned_row)

        csv_output = output_io.getvalue().strip() + "\n"
        return csv_output

    x_cleaned = _clean_csv_data(x)
    df = load_typed_dsv(
        bytes(textwrap.dedent(x_cleaned), "UTF8"),
        table_config.column_definitions.column_definitions,
        schema_overrides={
            **{c: pl.Datetime("us") for c in TableOps.system_versioning_columns()},
            "__is_override": pl.Boolean(),
        },
        delimiter=",",
    )

    df = df.with_columns(
        pl.col(k).cast(t) for k, t in table_config.dtypes().items() if k in df.columns
    ).pipe(PolarsType.cast_str_to_cat)

    renamings = {v: k for k, v, _ in parse_col_exprs(table_config.columns)}
    df = df.rename(renamings)

    return df


def setup_fixture_tableconfigs(*test_files: str):
    engine = mariadb_engine_test()
    table_configs = TableConfigs.from_yamls(*[get_table_config(f) for f in test_files])
    table_schema = table_configs.schemas()[0]

    with engine.begin() as connection:
        TableConfigOps(connection).drop_all(table_configs)

        for tc in table_configs.table_configs:
            DbOps(connection).db_create(tc.schema)
            TableConfigOps(connection).create(tc)
            if tc.schema != table_schema:
                raise ValueError(
                    "mixed-schema tests not supported (to keep things simple)"
                )

    yield engine, table_configs, table_schema

    with engine.begin() as connection:
        TableConfigOps(connection).drop_all(table_configs)


def setup_fixture_dataset(test_file: str):
    engine = mariadb_engine_test()
    config = BaseConfig.from_yaml(get_dataset_config(test_file))
    table_schema = config.tables.schemas()[0]
    table_configs = config.tables
    audit_table = AuditOps(table_schema)

    with engine.begin() as connection:
        TableConfigOps(connection).drop_all(table_configs)
        audit_table.drop(connection)

        for tc in table_configs.table_configs:
            DbOps(connection).db_create(tc.schema)
            TableConfigOps(connection).create(tc)
            if tc.schema != table_schema:
                raise ValueError(
                    "mixed-schema tests not supported (to keep things simple)"
                )

    yield engine, config

    with engine.begin() as connection:
        TableConfigOps(connection).drop_all(table_configs)
        audit_table.drop(connection)


def read_df_from_db(
    engine: Engine,
    table_schema: str,
    table_config: TableConfig,
    asof_date: datetime = datetime.now(timezone.utc),
    return_view: bool = False,
) -> Tuple[pl.DataFrame, Optional[pl.DataFrame]]:
    # table_name = (
    #     table_config.view_name
    #     if return_view and table_config.view_name
    #     else table_config.name
    # )

    table_name = table_config.name
    primary_keys = list(table_config.primary_keys)

    with engine.begin() as connection:
        tbo = TableOps(table_schema, table_name, connection)
        table = tbo.get_table_metadata()
        df_read = (
            DataframeOps(connection).from_selectable(select(table)).sort(primary_keys)
        )

        if not table_config.is_temporal or return_view:
            return df_read, None

        history_tbo = TableOps(table_schema, f"{table_name}", connection)
        history_tbl = history_tbo.get_table_metadata()
        compiled_stmt = (
            select(history_tbl)
            .with_hint(
                history_tbl,
                f"FOR SYSTEM_TIME ALL WHERE __valid_to <= '{asof_date.isoformat()}'",
            )
            .compile(dialect=mysql.dialect())
        )
        assert isinstance(compiled_stmt.statement, Select)
        df_read_history = (
            DataframeOps(connection)
            .from_selectable(compiled_stmt.statement)
            .sort(primary_keys + ["__valid_from"])
        )
        return df_read, df_read_history


def modify_and_read(
    engine: Engine,
    df: pl.DataFrame,
    table_schema: str,
    table_config: TableConfig,
    app_time: datetime,
    operation: Literal["delete", "upload"],
    as_override: bool = False,
    return_view: bool = False,
) -> Tuple[pl.DataFrame, Optional[pl.DataFrame]]:
    with engine.begin() as connection:
        # config = (
        #     table_config.generate_overrides_config() if as_override else table_config
        # )
        config = table_config
        if operation == "delete":
            DataframeOps(connection).table_delete_rows_temporal(
                df, table_schema, config.name, app_time
            )
        elif operation == "upload":
            assert config.delta_config is not None
            DataframeOps(connection).table_upsert_temporal(
                df,
                table_schema,
                config.name,
                config.delta_config,
                app_time,
                src_tgt_colname_map=MappingProxyType({}),
            )
    return read_df_from_db(engine, table_schema, table_config, app_time, return_view)


def upsert_then_read_nontemporal(
    engine: Engine,
    table_schema: str,
    source_table_config: TableConfig,
    target_table_config: TableConfig,
    source_columns: List[str],
    return_view: bool = False,
) -> Tuple[int, int, pl.DataFrame]:
    with engine.begin() as connection:
        tbo = TableOps(table_schema, source_table_config.name, connection)
        scs = tbo.get_column_intersection(source_columns)
        num_inserts, num_updates = DeltaTableOps(
            table_schema,
            source_table_config.name,
            target_table_config.delta_config,
            connection,
        )._table_upsert_nontemporal(
            table_schema,
            source_table_config.name,
            target_table_config.name,
            source_columns=[sc.name for sc in scs],
        )
    df, _ = read_df_from_db(
        engine,
        table_schema,
        target_table_config,
        datetime.now(timezone.utc),
        return_view,
    )
    return num_inserts, num_updates, df


def set_random_seed(seed: int):
    random.seed(seed)


def add_random_row(
    df: pl.DataFrame,
    table_config: TableConfig,
    primary_key: Optional[Dict[str, Any]] = None,
):
    new_row = dict(primary_key) if primary_key else {}
    if primary_key:
        df = df.filter(
            pl.col(list(primary_key.keys())[0]) != list(primary_key.values())[0]
        )

    sa_schema = {
        c.name: SQLAlchemyType.from_sql(c.data_type)
        for c in table_config.column_definitions.column_definitions
    }
    for col_name, c_type in sa_schema.items():
        if col_name in new_row:
            continue
        c_val: Any = None
        if isinstance(c_type, sqlalchemy.types.Integer):
            c_val = random.randint(0, 1000000)
        elif isinstance(c_type, sqlalchemy.types.Float):
            c_val = random.uniform(0, 1000)
        elif isinstance(c_type, sqlalchemy.types.Numeric):
            assert isinstance(c_type, sqlalchemy.types.Numeric)
            c_val = Decimal(str(random.uniform(0, 1000)))
        elif isinstance(c_type, sqlalchemy.types.Boolean):
            c_val = random.choice([True, False])
        elif isinstance(c_type, sqlalchemy.types.String):
            assert isinstance(c_type, sqlalchemy.types.String)
            length = c_type.length or 1
            c_val = "".join(
                random.choices(
                    "abcdefghijklmnopqrstuvwxyz", k=random.randint(1, length)
                )
            )
        elif isinstance(c_type, sqlalchemy.types.Date):
            c_val = (
                datetime.now() + timedelta(days=random.randint(-1000, 1000))
            ).date()
        elif isinstance(c_type, sqlalchemy.types.DateTime):
            c_val = datetime.now() + timedelta(
                days=random.randint(-1000, 1000), seconds=random.randint(0, 86399)
            )
        elif isinstance(c_type, sqlalchemy.types.Time):
            c_val = (datetime.min + timedelta(seconds=random.randint(0, 86399))).time()
        elif isinstance(c_type, sqlalchemy.types.Interval):
            c_val = timedelta(seconds=random.randint(0, 86400 * 365))
        elif isinstance(c_type, sqlalchemy.types.ARRAY):
            c_val = [random.randint(0, 100) for _ in range(random.randint(1, 5))]
        elif isinstance(c_type, sqlalchemy.types.JSON):
            c_val = {"key": random.randint(0, 100), "value": random.random()}
        elif isinstance(c_type, sqlalchemy.types.BINARY):
            c_val = bytes(
                [random.randint(0, 255) for _ in range(random.randint(16, 32))]
            )
        else:
            raise NotImplementedError(f"not implemented {c_type} (col={col_name})")
        new_row[col_name] = c_val

    return pl.concat(
        [df, pl.DataFrame(new_row, schema=dict(zip(df.columns, df.dtypes)))]
    )
