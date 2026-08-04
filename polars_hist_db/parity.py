from __future__ import annotations

import argparse
import asyncio
from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl
from sqlalchemy.engine import Engine

from .backends import backend_from_config
from .backends.config import DbEngineConfig
from .config import PolarsHistDbConfig, TableConfig
from .core import DbOps
from .dataset import run_datasets
from .types import PolarsType


@dataclass(frozen=True)
class BackendParityTarget:
    name: str
    db_config: DbEngineConfig


@dataclass(frozen=True)
class SemanticForeignKey:
    source_table: str
    source_column: str
    target_table: str
    target_column: str
    target_columns: tuple[str, ...]


@dataclass(frozen=True)
class TableParityMismatch:
    table: str
    reason: str
    left_rows: int | None
    right_rows: int | None
    details: tuple[str, ...] = ()


@dataclass(frozen=True)
class BackendParityResult:
    left_name: str
    right_name: str
    matches: list[str]
    mismatches: list[TableParityMismatch]
    semantic_foreign_key_diagnostics: tuple[str, ...] = ()

    @property
    def is_match(self) -> bool:
        return not self.mismatches


def prepare_parity_config(
    source_config: Any,
    target: BackendParityTarget,
    *,
    dataset_name: str | None = None,
    payload: str | None = None,
    payload_time: datetime | None = None,
) -> Any:
    config = deepcopy(source_config)
    config.db_config = target.db_config
    if payload is None:
        return config

    if dataset_name is None:
        raise ValueError("dataset_name is required when payload is provided")
    if payload_time is None:
        payload_time = datetime.now(UTC)

    dataset = config.datasets[dataset_name]
    if dataset is None:
        raise ValueError(f"Dataset {dataset_name!r} not found in config")
    input_config = dataset.input_config
    if not hasattr(input_config, "set_payload"):
        raise ValueError(f"Dataset {dataset_name!r} input_config cannot accept payload")
    input_config.set_payload(payload, payload_time)
    return config


def compare_backend_tables(
    *,
    left_name: str,
    right_name: str,
    left_tables: Mapping[str, pl.DataFrame],
    right_tables: Mapping[str, pl.DataFrame],
    table_order: Sequence[str],
    ignored_columns_by_table: Mapping[str, frozenset[str]] | None = None,
    semantic_foreign_keys: Sequence[SemanticForeignKey] = (),
    mismatch_sample_limit: int = 5,
) -> BackendParityResult:
    matches: list[str] = []
    mismatches: list[TableParityMismatch] = []
    left_tables, right_tables, semantic_fk_diagnostics = _prepare_tables_for_comparison(
        left_name=left_name,
        right_name=right_name,
        left_tables=left_tables,
        right_tables=right_tables,
        ignored_columns_by_table=ignored_columns_by_table or {},
        semantic_foreign_keys=semantic_foreign_keys,
    )

    for table in table_order:
        left_df = left_tables.get(table)
        right_df = right_tables.get(table)
        if left_df is None or right_df is None:
            mismatches.append(
                TableParityMismatch(
                    table=table,
                    reason="missing table result",
                    left_rows=None if left_df is None else left_df.height,
                    right_rows=None if right_df is None else right_df.height,
                )
            )
            continue

        if left_df.height != right_df.height:
            mismatches.append(
                TableParityMismatch(
                    table=table,
                    reason="row count mismatch",
                    left_rows=left_df.height,
                    right_rows=right_df.height,
                )
            )
            continue

        if not left_df.equals(right_df):
            mismatches.append(
                TableParityMismatch(
                    table=table,
                    reason="data mismatch",
                    left_rows=left_df.height,
                    right_rows=right_df.height,
                    details=_data_mismatch_details(
                        left_df,
                        right_df,
                        sample_limit=mismatch_sample_limit,
                    ),
                )
            )
            continue

        matches.append(table)

    return BackendParityResult(
        left_name=left_name,
        right_name=right_name,
        matches=matches,
        mismatches=mismatches,
        semantic_foreign_key_diagnostics=semantic_fk_diagnostics,
    )


def format_backend_parity_result(result: BackendParityResult) -> Sequence[str]:
    lines: list[str] = []
    for table in result.matches:
        lines.append(f"PASS {table}")
    for diagnostic in result.semantic_foreign_key_diagnostics:
        lines.append(f"WARN {diagnostic}")
    for mismatch in result.mismatches:
        lines.append(
            f"FAIL {mismatch.table}: {mismatch.reason} "
            f"({result.left_name}={mismatch.left_rows}, "
            f"{result.right_name}={mismatch.right_rows})"
        )
        lines.extend(f"  {detail}" for detail in mismatch.details)
    return lines


def _data_mismatch_details(
    left_df: pl.DataFrame,
    right_df: pl.DataFrame,
    *,
    sample_limit: int,
) -> tuple[str, ...]:
    details: list[str] = []
    left_columns = set(left_df.columns)
    right_columns = set(right_df.columns)
    columns_only_left = sorted(left_columns - right_columns)
    columns_only_right = sorted(right_columns - left_columns)
    if columns_only_left:
        details.append(f"columns_only_left={','.join(columns_only_left)}")
    if columns_only_right:
        details.append(f"columns_only_right={','.join(columns_only_right)}")

    common_columns = [column for column in left_df.columns if column in right_columns]
    differing_columns = [
        column
        for column in common_columns
        if not left_df.select(column).equals(right_df.select(column))
    ]
    if differing_columns:
        details.append(f"differing_columns={','.join(differing_columns)}")

    left_rows = left_df.to_dicts()
    right_rows = right_df.to_dicts()
    differing_row_pairs = [
        (left_row, right_row)
        for left_row, right_row in zip(left_rows, right_rows, strict=True)
        if left_row != right_row
    ]
    for sample_index, (left_row, right_row) in enumerate(
        differing_row_pairs[:sample_limit]
    ):
        details.append(f"sample[{sample_index}] left={left_row!r} right={right_row!r}")

    omitted_rows = len(differing_row_pairs) - sample_limit
    if omitted_rows > 0:
        details.append(f"{omitted_rows} additional differing rows omitted")

    return tuple(details)


def _prepare_tables_for_comparison(
    *,
    left_name: str,
    right_name: str,
    left_tables: Mapping[str, pl.DataFrame],
    right_tables: Mapping[str, pl.DataFrame],
    ignored_columns_by_table: Mapping[str, frozenset[str]],
    semantic_foreign_keys: Sequence[SemanticForeignKey],
) -> tuple[dict[str, pl.DataFrame], dict[str, pl.DataFrame], tuple[str, ...]]:
    left_prepared = dict(left_tables)
    right_prepared = dict(right_tables)
    semantic_fk_diagnostics: list[str] = []

    for semantic_fk in semantic_foreign_keys:
        left_diagnostic = _resolve_semantic_foreign_key(
            left_prepared,
            semantic_fk,
            backend_name=left_name,
        )
        right_diagnostic = _resolve_semantic_foreign_key(
            right_prepared,
            semantic_fk,
            backend_name=right_name,
        )
        if left_diagnostic is not None:
            semantic_fk_diagnostics.append(left_diagnostic)
        if right_diagnostic is not None:
            semantic_fk_diagnostics.append(right_diagnostic)

    for table, ignored_columns in ignored_columns_by_table.items():
        for tables in (left_prepared, right_prepared):
            df = tables.get(table)
            if df is None:
                continue
            columns_to_drop = [
                column for column in ignored_columns if column in df.columns
            ]
            if columns_to_drop:
                tables[table] = df.drop(columns_to_drop)

    return (
        {
            table: _canonicalise_comparison_dataframe(df)
            for table, df in left_prepared.items()
        },
        {
            table: _canonicalise_comparison_dataframe(df)
            for table, df in right_prepared.items()
        },
        tuple(semantic_fk_diagnostics),
    )


def _resolve_semantic_foreign_key(
    tables: dict[str, pl.DataFrame],
    semantic_fk: SemanticForeignKey,
    *,
    backend_name: str,
) -> str | None:
    source_df = tables.get(semantic_fk.source_table)
    target_df = tables.get(semantic_fk.target_table)
    if source_df is None or target_df is None:
        return None
    if semantic_fk.source_column not in source_df.columns:
        return None

    target_required_columns = [
        semantic_fk.target_column,
        *semantic_fk.target_columns,
    ]
    if any(column not in target_df.columns for column in target_required_columns):
        return None

    resolved_marker = f"__{semantic_fk.source_column}__resolved"
    renamed_columns = {
        semantic_fk.target_column: semantic_fk.source_column,
        **{
            column: f"{semantic_fk.source_column}__{column}"
            for column in semantic_fk.target_columns
        },
    }
    lookup_df = (
        target_df.select(target_required_columns)
        .unique(subset=[semantic_fk.target_column], maintain_order=True)
        .rename(renamed_columns)
        .with_columns(pl.lit(True).alias(resolved_marker))
    )
    joined_df = source_df.join(
        lookup_df,
        on=semantic_fk.source_column,
        how="left",
    )
    unresolved_rows = joined_df.filter(
        pl.col(semantic_fk.source_column).is_not_null()
        & pl.col(resolved_marker).is_null()
    ).height
    tables[semantic_fk.source_table] = joined_df.drop(
        semantic_fk.source_column,
        resolved_marker,
    )
    if unresolved_rows == 0:
        return None
    return (
        f"{backend_name} unresolved semantic_fk "
        f"{semantic_fk.source_table}.{semantic_fk.source_column} -> "
        f"{semantic_fk.target_table}.{semantic_fk.target_column} "
        f"using {','.join(semantic_fk.target_columns)}: "
        f"{unresolved_rows}/{source_df.height} source rows"
    )


def _canonicalise_comparison_dataframe(df: pl.DataFrame) -> pl.DataFrame:
    if not df.columns:
        return df
    return df.select(sorted(df.columns)).sort(sorted(df.columns))


def _qualified_table(table_config: TableConfig) -> str:
    return f"{table_config.schema}.{table_config.name}"


def _table_read_sql(
    backend_name: str,
    table_config: TableConfig,
    *,
    system_time: datetime | None = None,
) -> str | None:
    if backend_name == "xtdb":
        query = f"SELECT * FROM {_qualified_table(table_config)}"
        if table_config.is_temporal:
            query += " FOR VALID_TIME ALL"
        elif system_time is not None:
            query += f" FOR VALID_TIME AS OF TIMESTAMP '{system_time.isoformat()}'"
        if system_time is not None:
            query += f" FOR SYSTEM_TIME AS OF TIMESTAMP '{system_time.isoformat()}'"
        return query
    return None


def _normalise_backend_dataframe(
    df: pl.DataFrame,
    table_config: TableConfig,
) -> pl.DataFrame:
    primary_keys = list(table_config.primary_keys)
    if "_id" in df.columns:
        if len(primary_keys) == 1 and primary_keys[0] not in df.columns:
            df = df.rename({"_id": primary_keys[0]})
        else:
            df = df.drop("_id")

    configured_columns = [column.name for column in table_config.columns]
    selected_columns = [column for column in configured_columns if column in df.columns]
    df = df.select(selected_columns)

    casts = {
        column.name: PolarsType.from_sql(column.data_type)
        for column in table_config.columns
        if column.name in df.columns
    }
    if casts:
        df = df.with_columns(
            pl.col(column).cast(dtype) for column, dtype in casts.items()
        )
    df = df.pipe(PolarsType.cast_str_to_cat)

    sort_columns = [column for column in primary_keys if column in df.columns]
    if sort_columns:
        df = df.sort(sort_columns)
    return df


def read_parity_tables(
    engine: Engine,
    config: PolarsHistDbConfig,
    *,
    table_names: Sequence[str] | None = None,
    system_time: datetime | None = None,
) -> dict[str, pl.DataFrame]:
    backend = backend_from_config(config.db_config)
    table_configs = list(config.tables.items)
    selected = set(table_names) if table_names is not None else None
    if selected is not None:
        table_configs = [
            table_config
            for table_config in table_configs
            if _qualified_table(table_config) in selected
            or table_config.name in selected
        ]

    tables: dict[str, pl.DataFrame] = {}
    with backend.connection_scope(engine) as connection:
        dataframe_ops = backend.dataframes(connection)
        for table_config in table_configs:
            read_sql = _table_read_sql(
                backend.name,
                table_config,
                system_time=system_time,
            )
            if read_sql is None:
                df = dataframe_ops.from_table(table_config.schema, table_config.name)
            else:
                df = dataframe_ops.from_raw_sql(read_sql)
            tables[_qualified_table(table_config)] = _normalise_backend_dataframe(
                df,
                table_config,
            )
    return tables


def _create_config_tables(config: PolarsHistDbConfig, engine: Engine) -> None:
    backend = backend_from_config(config.db_config)
    with backend.connection_scope(engine) as connection:
        if backend.name == "mariadb":
            for schema in config.tables.schemas():
                DbOps(connection).db_create(schema)
        backend.table_configs(connection).drop_all(config.tables)
        backend.table_configs(connection).create_all(config.tables)


def _should_prepare_backend_tables(backend_name: str) -> bool:
    return backend_name != "xtdb"


async def run_backend_parity(
    source_config: PolarsHistDbConfig,
    *,
    left: BackendParityTarget,
    right: BackendParityTarget,
    dataset_name: str | None = None,
    table_names: Sequence[str] | None = None,
    payload: str | None = None,
    payload_time: datetime | None = None,
    ignored_columns_by_table: Mapping[str, frozenset[str]] | None = None,
    semantic_foreign_keys: Sequence[SemanticForeignKey] = (),
    mismatch_sample_limit: int = 5,
) -> BackendParityResult:
    left_config = prepare_parity_config(
        source_config,
        left,
        dataset_name=dataset_name,
        payload=payload,
        payload_time=payload_time,
    )
    right_config = prepare_parity_config(
        source_config,
        right,
        dataset_name=dataset_name,
        payload=payload,
        payload_time=payload_time,
    )

    left_backend = backend_from_config(left_config.db_config)
    right_backend = backend_from_config(right_config.db_config)
    left_engine = left_backend.create_engine(left_config.db_config)
    right_engine = right_backend.create_engine(right_config.db_config)
    try:
        if _should_prepare_backend_tables(left_backend.name):
            _create_config_tables(left_config, left_engine)
        if _should_prepare_backend_tables(right_backend.name):
            _create_config_tables(right_config, right_engine)
        await run_datasets(
            left_config,
            left_engine,
            dataset_name,
            raise_on_error=True,
        )
        await run_datasets(
            right_config,
            right_engine,
            dataset_name,
            raise_on_error=True,
        )
        verification_system_time = (
            payload_time + timedelta(seconds=1) if payload_time is not None else None
        )
        left_tables = read_parity_tables(
            left_engine,
            left_config,
            table_names=table_names,
            system_time=verification_system_time,
        )
        right_tables = read_parity_tables(
            right_engine,
            right_config,
            table_names=table_names,
            system_time=verification_system_time,
        )
    finally:
        left_engine.dispose()
        right_engine.dispose()

    table_order = [
        _qualified_table(table_config)
        for table_config in source_config.tables.items
        if table_names is None
        or _qualified_table(table_config) in table_names
        or table_config.name in table_names
    ]
    config_ignored_columns = _group_ignored_columns(
        _parity_ignore_columns_from_config(source_config)
    )
    effective_ignored_columns = _merge_ignored_columns(
        config_ignored_columns,
        ignored_columns_by_table or {},
    )
    effective_semantic_foreign_keys = (
        *_parity_semantic_foreign_keys_from_config(source_config),
        *semantic_foreign_keys,
    )
    return compare_backend_tables(
        left_name=left.name,
        right_name=right.name,
        left_tables=left_tables,
        right_tables=right_tables,
        table_order=table_order,
        ignored_columns_by_table=effective_ignored_columns,
        semantic_foreign_keys=effective_semantic_foreign_keys,
        mismatch_sample_limit=mismatch_sample_limit,
    )


def _parse_backend_target(value: str) -> BackendParityTarget:
    first_part = value.split(",", 1)[0]
    name: str
    if "=" in first_part:
        name, _separator, raw_config = value.partition("=")
    else:
        raw_config = value
        name = first_part
    parts = [part for part in raw_config.split(",") if part]
    if not parts:
        raise ValueError("Backend target cannot be empty")
    cfg: dict[str, str] = {"backend": parts[0]}
    for part in parts[1:]:
        key, key_separator, parsed_value = part.partition("=")
        if not key_separator:
            raise ValueError(f"Invalid backend target option {part!r}")
        cfg[key] = parsed_value
    return BackendParityTarget(
        name=name,
        db_config=DbEngineConfig.from_mapping(cfg),
    )


def _split_qualified_column(value: str) -> tuple[str, str]:
    table, separator, column = value.rpartition(".")
    if not separator or "." not in table or not column:
        raise ValueError(f"Expected a fully-qualified column, got {value!r}")
    return table, column


def _parse_ignore_column(value: str) -> tuple[str, str]:
    return _split_qualified_column(value)


def _parse_semantic_foreign_key(value: str) -> SemanticForeignKey:
    source_column_ref, mapping_separator, target_ref = value.partition("=")
    if not mapping_separator:
        raise ValueError(f"Invalid semantic foreign key mapping {value!r}")

    target_column_ref, target_columns_separator, raw_target_columns = (
        target_ref.partition(":")
    )
    if not target_columns_separator:
        raise ValueError(
            f"Semantic foreign key mapping requires target columns: {value!r}"
        )

    source_table, source_column = _split_qualified_column(source_column_ref)
    target_table, target_column = _split_qualified_column(target_column_ref)
    target_columns = tuple(
        column.strip() for column in raw_target_columns.split(",") if column.strip()
    )
    if not target_columns:
        raise ValueError(
            f"Semantic foreign key mapping requires target columns: {value!r}"
        )
    return SemanticForeignKey(
        source_table=source_table,
        source_column=source_column,
        target_table=target_table,
        target_column=target_column,
        target_columns=target_columns,
    )


def _semantic_foreign_key_from_config(value: Any) -> SemanticForeignKey:
    source_table, source_column = _split_qualified_column(str(value.source))
    target_table, target_column = _split_qualified_column(str(value.target))
    return SemanticForeignKey(
        source_table=source_table,
        source_column=source_column,
        target_table=target_table,
        target_column=target_column,
        target_columns=tuple(value.columns),
    )


def _group_ignored_columns(
    ignored_columns: Sequence[tuple[str, str]],
) -> dict[str, frozenset[str]]:
    grouped: dict[str, set[str]] = {}
    for table, column in ignored_columns:
        grouped.setdefault(table, set()).add(column)
    return {table: frozenset(columns) for table, columns in grouped.items()}


def _merge_ignored_columns(
    *ignored_columns_by_table: Mapping[str, frozenset[str]],
) -> dict[str, frozenset[str]]:
    grouped: dict[str, set[str]] = {}
    for ignored_columns in ignored_columns_by_table:
        for table, columns in ignored_columns.items():
            grouped.setdefault(table, set()).update(columns)
    return {table: frozenset(columns) for table, columns in grouped.items()}


def _parity_ignore_columns_from_config(config: Any) -> list[tuple[str, str]]:
    parity_config = getattr(config, "parity", None)
    if parity_config is None:
        return []
    return [
        _parse_ignore_column(column)
        for column in getattr(parity_config, "ignore_columns", ())
    ]


def _parity_semantic_foreign_keys_from_config(
    config: Any,
) -> tuple[SemanticForeignKey, ...]:
    parity_config = getattr(config, "parity", None)
    if parity_config is None:
        return ()
    return tuple(
        _semantic_foreign_key_from_config(value)
        for value in getattr(parity_config, "semantic_foreign_keys", ())
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare two polars-hist-db backends")
    parser.add_argument(
        "--config", required=True, help="Path to polars-hist-db YAML config"
    )
    parser.add_argument("--dataset", help="Dataset name to ingest")
    parser.add_argument(
        "--table",
        action="append",
        dest="tables",
        help="Table to compare, as name or schema.name. Repeatable.",
    )
    parser.add_argument("--payload-file", type=Path, help="Payload file to inject")
    parser.add_argument(
        "--payload-time",
        help="Payload timestamp. Defaults to now only when --payload-file is used.",
    )
    parser.add_argument(
        "--left",
        default="mariadb",
        help="Left target as backend or name=backend. Default: mariadb",
    )
    parser.add_argument(
        "--right",
        default="xtdb",
        help="Right target as backend or name=backend. Default: xtdb",
    )
    parser.add_argument(
        "--ignore-column",
        action="append",
        default=[],
        type=_parse_ignore_column,
        help="Column to ignore as schema.table.column. Repeatable.",
    )
    parser.add_argument(
        "--semantic-fk",
        action="append",
        default=[],
        type=_parse_semantic_foreign_key,
        help=(
            "Resolve a backend-local FK before comparing, as "
            "source.schema_table.column=target.schema_table.column:semantic_col,..."
        ),
    )
    parser.add_argument(
        "--mismatch-sample-limit",
        type=int,
        default=5,
        help="Maximum differing rows to print per mismatched table. Default: 5",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    config = PolarsHistDbConfig.from_yaml(args.config)
    payload = args.payload_file.read_text() if args.payload_file else None
    payload_time = (
        datetime.fromisoformat(args.payload_time) if args.payload_time else None
    )
    result = asyncio.run(
        run_backend_parity(
            config,
            left=_parse_backend_target(args.left),
            right=_parse_backend_target(args.right),
            dataset_name=args.dataset,
            table_names=args.tables,
            payload=payload,
            payload_time=payload_time,
            ignored_columns_by_table=_group_ignored_columns(args.ignore_column),
            semantic_foreign_keys=tuple(args.semantic_fk),
            mismatch_sample_limit=args.mismatch_sample_limit,
        )
    )
    for line in format_backend_parity_result(result):
        print(line)
    if not result.is_match:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
