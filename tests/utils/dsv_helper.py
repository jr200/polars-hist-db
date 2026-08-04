import csv
import os
import random
import subprocess
import tempfile
import textwrap
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from io import StringIO
from pathlib import Path
from types import MappingProxyType
from typing import Any, Literal, Optional

import polars as pl
import pytest
import sqlalchemy
from sqlalchemy import Engine, Select, create_engine, select, text
from sqlalchemy.dialects import mysql

from polars_hist_db.backends import DbEngineConfig, backend_from_config
from polars_hist_db.backends.xtdb import XtdbBackend
from polars_hist_db.config import (
    DatasetConfig,
    PolarsHistDbConfig,
    TableConfig,
    TableConfigs,
)
from polars_hist_db.config.parser_config import IngestionColumnConfig
from polars_hist_db.core import (
    AuditOps,
    DataframeOps,
    DbOps,
    TableOps,
)
from polars_hist_db.loaders import load_typed_dsv
from polars_hist_db.types import PolarsType, SQLAlchemyType


def _tests_dir():
    tests_dir = Path(__file__).parent.parent.parent.absolute()
    tests_dir = os.path.join(tests_dir, "tests")
    return tests_dir


def get_test_config(filename: str):
    data_dir = os.path.join(_tests_dir(), "_data", "config")
    return os.path.join(data_dir, filename)


def get_dataset_data(filename: str):
    data_dir = os.path.join(_tests_dir(), "_testdata_dataset_data")
    return os.path.join(data_dir, filename)


def mariadb_engine_test() -> Engine:
    port = int(os.environ.get("MARIADB_PORT", "13307"))
    url = f"mariadb+pymysql://root:admin@127.0.0.1:{port}"
    return create_engine(
        url,
        pool_recycle=3600,
        pool_size=3,
        max_overflow=2,
        connect_args={"client_flag": 0},
    )


def backend_params():
    return [
        "mariadb",
        pytest.param(
            "xtdb",
            marks=pytest.mark.skipif(
                os.environ.get("POLARS_HIST_DB_XTDB_LIVE") != "1",
                reason="set POLARS_HIST_DB_XTDB_LIVE=1 to run XTDB parity tests",
            ),
        ),
    ]


def _docker(args: list[str]) -> str:
    return subprocess.check_output(["docker", *args], text=True).strip()


def _published_port(container_id: str, container_port: str) -> int:
    output = _docker(["port", container_id, container_port])
    first_binding = output.splitlines()[0]
    return int(first_binding.rsplit(":", 1)[1])


def xtdb_engine_test() -> tuple[Engine, str, DbEngineConfig]:
    container_id = _docker(
        [
            "run",
            "--rm",
            "-d",
            "-p",
            "5432",
            "-p",
            "9832",
            "ghcr.io/xtdb/xtdb:nightly",
        ]
    )
    config = DbEngineConfig(
        backend="xtdb",
        hostname="127.0.0.1",
        port=_published_port(container_id, "5432/tcp"),
        adbc_port=_published_port(container_id, "9832/tcp"),
    )
    engine = XtdbBackend().create_engine(config)
    deadline = time.monotonic() + 60
    while True:
        try:
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            break
        except Exception:
            if time.monotonic() > deadline:
                engine.dispose()
                subprocess.run(["docker", "rm", "-f", container_id], check=False)
                raise
            time.sleep(1)

    return engine, container_id, config


def _backend_from_engine(engine: Engine):
    return getattr(
        engine, "_polars_hist_db_backend", backend_from_config(DbEngineConfig())
    )


def create_temp_file_tree(dircnt: int, depth: int, filecnt: int):
    print(
        f"Create temporary directory with {dircnt} directories with depth {depth} and {3 * filecnt} files"
    )
    tempDir = tempfile.TemporaryDirectory(prefix="scandir_rs_")
    for dn in range(dircnt):
        dirName = f"{tempDir.name}/dir{dn}"
        for _depth_level in range(depth):
            os.makedirs(dirName)
            for fn in range(filecnt):
                open(f"{dirName}/file{fn}.bin", "wb").close()
                open(f"{dirName}/file{fn}.txt", "wb").close()
                open(f"{dirName}/file{fn}.log", "wb").close()
            dirName = f"{dirName}/dir{_depth_level}"
    return tempDir


def _infer_input_columns_from_tables(
    table_configs: TableConfigs,
) -> list[IngestionColumnConfig]:
    items: list[IngestionColumnConfig] = []
    for table_config in table_configs.items:
        for column in table_config.columns:
            dc = IngestionColumnConfig(
                column_type="data",
                schema=table_config.schema,
                table=table_config.name,
                ingestion_data_type=column.data_type,
                target_data_type=column.data_type,
                source=column.name,
                target=column.name,
                nullable=column.nullable,
            )
            items.append(dc)

    return items


def clean_dsv_string(data: str) -> str:
    input_io = StringIO(data.strip())
    output_io = StringIO()

    reader = csv.reader(input_io)
    writer = csv.writer(output_io, quoting=csv.QUOTE_MINIMAL)

    for row in reader:
        cleaned_row = [field.strip() if field is not None else "" for field in row]
        writer.writerow(cleaned_row)

    csv_output = output_io.getvalue().strip() + "\n"
    return csv_output


def from_test_result(
    x: str,
    table_name: str,
    tables: TableConfigs,
    dataset: DatasetConfig | None = None,
    skip_time_partition: bool = True,
    schema_overrides: dict[str, pl.DataType] | None = None,
) -> pl.DataFrame:
    x_cleaned = clean_dsv_string(x)

    if dataset:
        column_definitions = [
            c
            for c in dataset.pipeline.build_ingestion_column_definitions(tables)
            if c.table == table_name
        ]
    else:
        column_definitions = _infer_input_columns_from_tables(tables)

    if skip_time_partition:
        column_definitions = [
            c for c in column_definitions if c.column_type != "time_partition_only"
        ]

    if not schema_overrides:
        schema_overrides = {}

    schema_overrides.update(
        {
            **{c: pl.Datetime("us") for c in TableOps.system_versioning_columns()},
            "__is_override": pl.Boolean(),
        }
    )

    df = load_typed_dsv(
        bytes(textwrap.dedent(x_cleaned), "UTF8"),
        column_definitions,
        schema_overrides=schema_overrides,
        delimiter=",",
    )

    # df = df.with_columns(
    #     pl.col(k).cast(t) for k, t in table_config.dtypes().items() if k in df.columns
    # ).pipe(PolarsType.cast_str_to_cat)

    renamings = {
        c.source: c.target
        for c in column_definitions
        if c.source and c.target and c.source in df.columns
    }
    df = (
        df.rename(renamings)
        .with_columns(
            [
                pl.col(col_def.target).cast(
                    PolarsType.from_sql(col_def.target_data_type)
                )
                for col_def in column_definitions
                if col_def.target
            ]
        )
        .pipe(PolarsType.cast_str_to_cat)
    )

    return df


def setup_fixture_dataset(test_file: str, backend_name: str = "mariadb"):
    config = PolarsHistDbConfig.from_yaml(get_test_config(test_file))
    container_id = None
    if backend_name == "mariadb":
        engine = mariadb_engine_test()
        backend_config = DbEngineConfig(backend="mariadb")
        backend = backend_from_config(backend_config)
    elif backend_name == "xtdb":
        engine, container_id, backend_config = xtdb_engine_test()
        backend = backend_from_config(backend_config)
    else:
        raise ValueError(f"unsupported test backend {backend_name!r}")
    config.db_config = backend_config
    engine._polars_hist_db_backend = backend  # type: ignore[attr-defined]

    table_schema = config.tables.schemas()[0]
    table_configs = config.tables
    audit_table = AuditOps(table_schema)

    connection_context = engine.connect if backend.name == "xtdb" else engine.begin
    with connection_context() as connection:
        backend.table_configs(connection).drop_all(table_configs)
        if backend.name == "mariadb":
            audit_table.drop(connection)

        for tc in table_configs.items:
            if backend.name == "mariadb":
                DbOps(connection).db_create(tc.schema)
            backend.table_configs(connection).create(tc)
            if tc.schema != table_schema:
                raise ValueError(
                    "mixed-schema tests not supported (to keep things simple)"
                )

    yield engine, config

    try:
        with connection_context() as connection:
            backend.table_configs(connection).drop_all(table_configs)
            if backend.name == "mariadb":
                audit_table.drop(connection)
    finally:
        engine.dispose()
        if container_id is not None:
            subprocess.run(["docker", "rm", "-f", container_id], check=False)


def _normalise_xtdb_dataframe(
    df: pl.DataFrame,
    table_config: TableConfig,
    include_temporal_columns: bool,
) -> pl.DataFrame:
    primary_keys = list(table_config.primary_keys)
    if "_id" in df.columns:
        if len(primary_keys) == 1 and primary_keys[0] not in df.columns:
            df = df.rename({"_id": primary_keys[0]})
        else:
            df = df.drop("_id")

    if include_temporal_columns and table_config.is_temporal:
        for column in ["_valid_from", "_valid_to"]:
            if (
                column in df.columns
                and isinstance(df[column].dtype, pl.Datetime)
                and getattr(df[column].dtype, "time_zone", None) is not None
            ):
                df = df.with_columns(pl.col(column).dt.replace_time_zone(None))
        if "_valid_to" in df.columns:
            df = df.with_columns(
                pl.col("_valid_to").fill_null(
                    datetime.fromisoformat("2106-02-07T06:28:15.999999")
                )
            )
        df = df.rename(
            {
                col: f"_{col}"
                for col in ["_valid_from", "_valid_to"]
                if col in df.columns
            }
        )
    else:
        df = df.drop([col for col in ["_valid_from", "_valid_to"] if col in df.columns])

    configured_columns = [column.name for column in table_config.columns]
    if include_temporal_columns and table_config.is_temporal:
        configured_columns = [*configured_columns, "__valid_from", "__valid_to"]
    selected_columns = [column for column in configured_columns if column in df.columns]
    df = df.select(selected_columns)

    configured_dtypes = {
        column.name: PolarsType.from_sql(column.data_type)
        for column in table_config.columns
        if column.name in df.columns
    }
    if include_temporal_columns and table_config.is_temporal:
        for column in ["_valid_from", "_valid_to", "__valid_from", "__valid_to"]:
            if column in df.columns:
                configured_dtypes[column] = pl.Datetime("us")
    return df.with_columns(
        pl.col(column).cast(dtype) for column, dtype in configured_dtypes.items()
    ).pipe(PolarsType.cast_str_to_cat)


def read_df_from_db(
    engine: Engine,
    table_schema: str,
    table_config: TableConfig,
    asof_date: datetime | None = None,
    return_view: bool = False,
) -> tuple[pl.DataFrame, pl.DataFrame | None]:
    # table_name = (
    #     table_config.view_name
    #     if return_view and table_config.view_name
    #     else table_config.name
    # )

    if asof_date is None:
        asof_date = datetime.now(UTC)
    table_name = table_config.name
    primary_keys = list(table_config.primary_keys)
    backend = _backend_from_engine(engine)

    connection_context = engine.connect if backend.name == "xtdb" else engine.begin
    with connection_context() as connection:
        if backend.name == "xtdb":
            dataframe_ops = backend.dataframes(connection)
            if table_config.is_temporal and not return_view:
                df_read = dataframe_ops.from_raw_sql(
                    f"SELECT *, _valid_from, _valid_to FROM {table_schema}.{table_name}"
                )
            else:
                df_read = dataframe_ops.from_table(table_schema, table_name)
            df_read = _normalise_xtdb_dataframe(
                df_read,
                table_config,
                include_temporal_columns=table_config.is_temporal and not return_view,
            ).sort(primary_keys)

            if not table_config.is_temporal or return_view:
                return df_read, None

            df_read_history = dataframe_ops.from_raw_sql(
                f"""
                SELECT *, _valid_from, _valid_to
                FROM {table_schema}.{table_name}
                FOR VALID_TIME ALL
                WHERE _valid_to IS NOT NULL
                  AND _valid_to <= TIMESTAMP '{asof_date.isoformat()}'
                """
            )
            df_read_history = _normalise_xtdb_dataframe(
                df_read_history,
                table_config,
                include_temporal_columns=True,
            ).sort(primary_keys + ["__valid_from"])
            return df_read, df_read_history

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


def read_raw_sql_from_db(
    engine: Engine,
    query: str,
    table_config: TableConfig | None = None,
) -> pl.DataFrame:
    backend = _backend_from_engine(engine)
    connection_context = engine.connect if backend.name == "xtdb" else engine.begin
    with connection_context() as connection:
        df = backend.dataframes(connection).from_raw_sql(query)

    if backend.name == "xtdb" and table_config is not None:
        df = _normalise_xtdb_dataframe(
            df,
            table_config,
            include_temporal_columns=False,
        )
    return df


def get_test_backend_name(engine: Engine) -> str:
    return _backend_from_engine(engine).name


def connection_context_for_engine(engine: Engine):
    backend = _backend_from_engine(engine)
    return engine.connect if backend.name == "xtdb" else engine.begin


def dataframe_ops_for_engine(engine: Engine, connection):
    return _backend_from_engine(engine).dataframes(connection)


def normalise_query_result_for_backend(
    df: pl.DataFrame,
    engine: Engine,
    table_config: TableConfig,
) -> pl.DataFrame:
    if _backend_from_engine(engine).name != "xtdb":
        return df
    return _normalise_xtdb_dataframe(
        df,
        table_config,
        include_temporal_columns=table_config.is_temporal,
    )


def table_config_ops_for_engine(engine: Engine, connection):
    return _backend_from_engine(engine).table_configs(connection)


def commit_xtdb_connection(engine: Engine, connection):
    if _backend_from_engine(engine).name == "xtdb":
        connection.commit()


def modify_and_read(
    engine: Engine,
    df: pl.DataFrame,
    dataset: DatasetConfig,
    table_schema: str,
    table_config: TableConfig,
    app_time: datetime,
    operation: Literal["delete", "upload"],
    as_override: bool = False,
    return_view: bool = False,
) -> tuple[pl.DataFrame, pl.DataFrame | None]:
    backend = _backend_from_engine(engine)
    schema = {
        column.name: PolarsType.from_sql(column.data_type)
        for column in table_config.columns
        if column.name in df.columns
    }
    df = PolarsType.apply_schema_to_dataframe(df, **schema)
    if backend.name == "xtdb":
        datetime_columns = [
            column.name
            for column in table_config.columns
            if column.name in df.columns
            and isinstance(df.schema[column.name], pl.Datetime)
            and getattr(df.schema[column.name], "time_zone", None) is None
            and column.data_type.upper() in {"DATETIME", "TIMESTAMPTZ"}
        ]
        if datetime_columns:
            df = df.with_columns(
                pl.col(column).dt.replace_time_zone("UTC")
                for column in datetime_columns
            )
    connection_context = engine.connect if backend.name == "xtdb" else engine.begin
    with connection_context() as connection:
        # config = (
        #     table_config.generate_overrides_config() if as_override else table_config
        # )
        config = table_config
        if backend.name == "xtdb":
            if operation == "delete":
                raise NotImplementedError("XTDB test helper delete is not implemented")
            assert dataset.delta_config is not None
            if app_time is not None and "_valid_from" not in df.columns:
                df = df.with_columns(
                    pl.lit(app_time.astimezone(UTC)).alias("_valid_from")
                )
            backend.temporal_upsert(
                df,
                table_schema,
                config.name,
                connection=connection,
                table_config=config,
                delta_config=dataset.delta_config,
                valid_time=dataset.valid_time_for_table(table_schema, config.name),
                dropout_close_time=(
                    app_time.astimezone(UTC) if app_time is not None else None
                ),
            )
        elif operation == "delete":
            DataframeOps(connection).table_delete_rows_temporal(
                df, table_schema, config.name, app_time
            )
        elif operation == "upload":
            assert dataset.delta_config is not None
            DataframeOps(connection).table_upsert_temporal(
                df,
                table_schema,
                config.name,
                dataset.delta_config,
                app_time,
                src_tgt_colname_map=MappingProxyType({}),
            )
    return read_df_from_db(engine, table_schema, table_config, app_time, return_view)


def set_random_seed(seed: int):
    random.seed(seed)


def add_random_row(
    df: pl.DataFrame,
    table_config: TableConfig,
    primary_key: dict[str, Any] | None = None,
):
    new_row = dict(primary_key) if primary_key else {}
    if primary_key:
        df = df.filter(
            pl.col(next(iter(primary_key.keys()))) != next(iter(primary_key.values()))
        )

    sa_schema = {
        c.name: SQLAlchemyType.from_sql(c.data_type) for c in table_config.columns
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
            c_val = datetime.now(UTC).date() + timedelta(
                days=random.randint(-1000, 1000)
            )
        elif isinstance(c_type, sqlalchemy.types.DateTime):
            c_val = datetime.now(UTC).replace(tzinfo=None) + timedelta(
                days=random.randint(-1000, 1000), seconds=random.randint(0, 86399)
            )
        elif isinstance(c_type, sqlalchemy.types.Time):
            c_val = (
                datetime.min.replace(tzinfo=UTC)
                + timedelta(seconds=random.randint(0, 86399))
            ).time()
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
        [
            df,
            pl.DataFrame(new_row, schema=dict(zip(df.columns, df.dtypes, strict=True))),
        ]
    )
