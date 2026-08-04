"""Benchmark XTDB delta, normalized foreign keys, and DSV partitioning.

Examples:
    uv run python benchmarks/xtdb_delta.py
    uv run python benchmarks/xtdb_delta.py --target-rows 50000,5000000
    XTDB_BENCHMARK_DSN=postgresql://... uv run python benchmarks/xtdb_delta.py \
        --remote-table example.records --remote-limit 50000
"""

import gc
import json
import os
import re
from argparse import ArgumentParser
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from statistics import median
from time import perf_counter
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import polars as pl
import pyarrow as pa

from polars_hist_db.backends.xtdb import (
    XtdbStagingOps,
    _filter_xtdb_unchanged_rows,
)
from polars_hist_db.config import DatasetConfig, TableColumnConfig, TableConfig
from polars_hist_db.config.dataset import TimePartition
from polars_hist_db.loaders.dsv_input_source import DsvCrawlerInputSource
from polars_hist_db.overrides import (
    InMemoryArrowOverrideOperationStore,
    arrow_override_operation_schema,
    decode_arrow_override_operations,
    encode_arrow_override_operations,
)


@dataclass
class CurrentRows:
    frame: pl.DataFrame

    @property
    def connection(self) -> "CurrentRows":
        return self

    def table_insert(self, df: pl.DataFrame, *_args: Any, **_kwargs: Any) -> int:
        return len(df)

    def execute(self, *_args: Any, **_kwargs: Any) -> None:
        pass

    def from_raw_sql(self, sql: str, *_args: Any) -> pl.DataFrame:
        if "__xtdb_minimum_id" in sql:
            return pl.DataFrame(
                {
                    "id": [None],
                    "__xtdb_minimum_id": [self.frame["id"].min()],
                },
                schema={"id": pl.Int64, "__xtdb_minimum_id": pl.Int64},
            )
        return self.frame

    def from_table(self, _schema: str, _table: str) -> pl.DataFrame:
        return self.frame

    def table_query(
        self,
        _schema: str,
        _table: str,
        query_df: pl.DataFrame,
        column_selection: list[str],
        **_kwargs: Any,
    ) -> pl.DataFrame:
        if query_df.is_empty():
            return self.frame.select(column_selection).head(0)
        frame = self.frame
        if "id" in column_selection and "id" not in frame.columns:
            frame = frame.with_columns(pl.col("_id").alias("id"))
        return frame.join(query_df, on=query_df.columns, how="inner").select(
            column_selection
        )


class ForeignKeyStaging(XtdbStagingOps):
    def __init__(self, parent: pl.DataFrame):
        super().__init__(object())
        self.rows = CurrentRows(parent)
        self.inserted_rows = 0

    def _dataframes(self) -> Any:
        return self.rows

    def _bulk_table_insert(self, df: pl.DataFrame, *args, **kwargs) -> int:
        self.inserted_rows += len(df)
        return len(df)


def network_floor_seconds(data_gb: float, bandwidth_gbps: float) -> float:
    return data_gb * 8 / bandwidth_gbps


def synthetic_frames(
    target_rows: int, upload_rows: int, value_columns: int
) -> tuple[pl.DataFrame, pl.DataFrame]:
    if upload_rows > target_rows:
        raise ValueError("upload rows cannot exceed target rows")
    current = pl.DataFrame({"_id": range(target_rows)})
    incoming = pl.DataFrame({"id": range(upload_rows)})
    for column in range(value_columns):
        name = f"value_{column}"
        current = current.with_columns((pl.col("_id") + column).alias(name))
        incoming = incoming.with_columns((pl.col("id") + column).alias(name))
    current = current.sample(fraction=1, shuffle=True, seed=42)
    if upload_rows:
        incoming = incoming.with_columns(
            pl.when(pl.col("id") == upload_rows - 1)
            .then(pl.col("value_0") + 1)
            .otherwise(pl.col("value_0"))
            .alias("value_0")
        )
    return current, incoming


def table_config(value_columns: int) -> TableConfig:
    return TableConfig(
        schema="benchmark",
        name="records",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            *[
                TableColumnConfig("records", f"value_{column}", "BIGINT")
                for column in range(value_columns)
            ],
        ],
    )


def foreign_key_config() -> tuple[DatasetConfig, TableConfig]:
    dataset = DatasetConfig(
        name="records",
        delta_table_schema="benchmark",
        input_config={"type": "dsv", "search_paths": []},  # type: ignore[arg-type]
        pipeline=[  # type: ignore[arg-type]
            {
                "schema": "benchmark",
                "table": "parents",
                "type": "primary",
                "columns": [
                    {
                        "source": "parent_id",
                        "target": "id",
                        "deduce_foreign_key": True,
                    },
                    {"source": "parent_key", "target": "natural_key"},
                ],
            }
        ],
    )
    config = TableConfig(
        schema="benchmark",
        name="parents",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("parents", "id", "BIGINT", nullable=False),
            TableColumnConfig("parents", "natural_key", "BIGINT"),
        ],
    )
    return dataset, config


def benchmark_delta(
    target_rows: int,
    upload_rows: int,
    value_columns: int,
    repeats: int,
) -> tuple[float, float, float, int]:
    current, incoming = synthetic_frames(target_rows, upload_rows, value_columns)
    ops = CurrentRows(current)
    config = table_config(value_columns)
    timings = []
    changed_rows = 0
    for _ in range(repeats):
        gc.collect()
        started = perf_counter()
        changed = _filter_xtdb_unchanged_rows(
            incoming, "benchmark", "records", config, ops, None
        )
        timings.append(perf_counter() - started)
        changed_rows = len(changed)
    return (
        median(timings),
        current.estimated_size("mb"),
        incoming.estimated_size("mb"),
        changed_rows,
    )


def benchmark_foreign_keys(
    parent_rows: int, upload_rows: int, match_fraction: float, repeats: int
) -> tuple[float, float, float, int, int, int, bool]:
    if upload_rows > parent_rows:
        raise ValueError("upload rows cannot exceed parent rows")
    if not 0 <= match_fraction <= 1:
        raise ValueError("match fraction must be between zero and one")
    matched_rows = round(upload_rows * match_fraction)
    unmatched_rows = upload_rows - matched_rows
    parent = pl.DataFrame(
        {"id": range(parent_rows), "natural_key": range(parent_rows)}
    ).sample(fraction=1, shuffle=True, seed=42)
    incoming_keys = [
        *range(matched_rows),
        *range(parent_rows, parent_rows + unmatched_rows),
    ]
    incoming = pl.DataFrame(
        {
            "parent_id": pl.Series([None] * upload_rows, dtype=pl.Int64),
            "parent_key": incoming_keys,
        }
    )
    dataset, config = foreign_key_config()
    timings = []
    inserted_rows = 0
    generated_id_collisions = 0
    stage_updated = False
    for _ in range(repeats):
        gc.collect()
        staging = ForeignKeyStaging(parent)
        staging._stage_run_cache["benchmark"] = incoming
        started = perf_counter()
        result = staging.prepare_pipeline_item_dataframe(
            "benchmark", dataset, 0, config, valid_time=None
        )
        timings.append(perf_counter() - started)
        assert len(result) == upload_rows
        inserted_rows = staging.inserted_rows
        staged = staging._stage_run_cache["benchmark"]
        stage_updated = staged["parent_id"].null_count() == 0
        generated_ids = staged.filter(pl.col("parent_key") >= parent_rows).get_column(
            "parent_id"
        )
        generated_id_collisions = len(generated_ids) - generated_ids.n_unique()
        assert inserted_rows == unmatched_rows
        assert stage_updated
    return (
        median(timings),
        parent.estimated_size("mb"),
        incoming.estimated_size("mb"),
        matched_rows,
        inserted_rows,
        generated_id_collisions,
        stage_updated,
    )


def benchmark_time_partitions(
    row_count: int, bucket_count: int, repeats: int
) -> tuple[float, float, int]:
    if row_count < bucket_count:
        raise ValueError("row count cannot be smaller than bucket count")
    if bucket_count < 1:
        raise ValueError("bucket count must be positive")

    start = datetime(2025, 1, 1, tzinfo=UTC)
    rows = (
        pl.DataFrame({"id": range(row_count)})
        .with_columns(
            event_time=pl.lit(start) + pl.duration(days=pl.col("id") % bucket_count)
        )
        .sample(fraction=1, shuffle=True, seed=42)
    )
    source: Any = object.__new__(DsvCrawlerInputSource)
    source.tables = {
        "events": TableConfig(
            schema="benchmark",
            name="events",
            columns=[],
            primary_keys=["id"],
        )
    }
    source.dataset = SimpleNamespace(
        pipeline=SimpleNamespace(
            get_main_table_name=lambda: ("benchmark", "events"),
            get_header_map=lambda _table: {"id": "id"},
        ),
        time_partition=TimePartition(
            column="event_time",
            bucket_interval="1d",
            bucket_strategy="round_down",
        ),
    )
    source.config = SimpleNamespace(filter_past_events=False)
    source.previous_payload_time = datetime.min.replace(tzinfo=UTC)

    timings = []
    partition_count = 0
    for _ in range(repeats):
        gc.collect()
        started = perf_counter()
        partitions = source._apply_time_partitioning(
            rows, start + timedelta(days=bucket_count - 1)
        )
        timings.append(perf_counter() - started)
        partition_count = len(partitions)
        assert partition_count == bucket_count
        assert sum(len(partition) for _, partition in partitions) == row_count

    return median(timings), rows.estimated_size("mb"), partition_count


def synthetic_arrow_override_operations(
    operation_rows: int, scope_count: int
) -> pa.Table:
    """Build a deterministic typed operation batch for repeatable CRDT benchmarks."""
    if operation_rows < 1:
        raise ValueError("operation rows must be positive")
    if not 1 <= scope_count <= operation_rows:
        raise ValueError("scope count must be between one and operation rows")
    value_fields: dict[str, object] = {
        field.name: None
        for field in arrow_override_operation_schema().field("value").type
    }
    value_fields["kind"] = "integer"
    rows = []
    for index in range(operation_rows):
        value = {**value_fields, "integer_value": index}
        rows.append(
            {
                "format_version": 1,
                "operation_id": UUID(int=index + 1).bytes,
                "change_set_id": UUID(int=operation_rows + index + 1).bytes,
                "layer_id": None,
                "generation": None,
                "layer_revision": None,
                "feed_id": "benchmark",
                "entity_id": f"record-{index % scope_count}",
                "field_path": "value",
                "operation_type": "set",
                "value": value,
                "unit": None,
                "supersedes_ids": [],
                "removes_ids": [],
                "valid_from": datetime(2026, 1, 1, tzinfo=UTC),
                "valid_to": None,
                "observed_value": None,
                "source_drift": False,
                "comment": None,
                "actor_subject": None,
                "actor_display_name": None,
                "recorded_at": None,
                "payload_hash": None,
            }
        )
    return pa.Table.from_pylist(rows, schema=arrow_override_operation_schema())


def benchmark_arrow_override_crdt(
    operation_rows: int, scope_count: int, repeats: int
) -> tuple[float, float, float, int, int]:
    """Measure typed IPC and authoritative sync/projection independently."""
    proposed = synthetic_arrow_override_operations(operation_rows, scope_count)
    layer_id = UUID(int=(1 << 128) - 1)
    ipc_timings: list[float] = []
    sync_timings: list[float] = []
    encoded_bytes = 0
    projection_rows = 0
    conflicts = 0
    for _ in range(repeats):
        gc.collect()
        started = perf_counter()
        encoded = encode_arrow_override_operations(proposed)
        decoded = decode_arrow_override_operations(encoded)
        ipc_timings.append(perf_counter() - started)
        assert decoded.equals(proposed)
        encoded_bytes = len(encoded)

        store = InMemoryArrowOverrideOperationStore()
        store.create_layer(layer_id)
        started = perf_counter()
        result = store.sync(
            layer_id=layer_id,
            generation=1,
            known_revision=0,
            pending=decoded,
            actor_subject="benchmark-subject",
            actor_display_name=None,
            recorded_at=datetime(2026, 1, 2, tzinfo=UTC),
        )
        sync_timings.append(perf_counter() - started)
        projection_rows = result.projection_delta.num_rows
        conflicts = (
            result.projection_delta["frontier_state"].to_pylist().count("conflict")
        )
        assert projection_rows == scope_count
    return (
        median(ipc_timings),
        median(sync_timings),
        encoded_bytes / (1024 * 1024),
        projection_rows,
        conflicts,
    )


def benchmark_remote_sample(dsn: str, table: str, limit: int) -> None:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*", table):
        raise ValueError("remote table must be an unquoted schema.table identifier")
    import psycopg

    with psycopg.connect(dsn) as connection:
        started = perf_counter()
        frame = pl.read_database(f"SELECT * FROM {table} LIMIT {limit}", connection)
        elapsed = perf_counter() - started
    print("remote_table,rows,decoded_mb,seconds,rows_per_second,decoded_mb_per_second")
    decoded_mb = frame.estimated_size("mb")
    print(
        f"{table},{len(frame)},{decoded_mb:.2f},{elapsed:.3f},"
        f"{len(frame) / elapsed:.0f},{decoded_mb / elapsed:.2f}"
    )


def main() -> None:
    parser = ArgumentParser()
    parser.add_argument("--target-rows", default="50000,500000,5000000,10000000")
    parser.add_argument("--upload-rows", type=int, default=50_000)
    parser.add_argument("--value-columns", type=int, default=8)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--target-gb", type=float, default=100)
    parser.add_argument("--bandwidth-gbps", default="1,10,100")
    parser.add_argument("--fk-match-fractions", default="0,0.5,1")
    parser.add_argument("--partition-rows", type=int, default=500_000)
    parser.add_argument("--partition-buckets", type=int, default=365)
    parser.add_argument(
        "--override-cases",
        default="1000:1000,10000:10000,10000:1000",
        help="comma-separated operation_rows:scope_count cases",
    )
    parser.add_argument("--remote-table")
    parser.add_argument("--remote-limit", type=int, default=50_000)
    parser.add_argument("--json-output")
    args = parser.parse_args()
    results = []

    print(
        "target_rows,upload_rows,target_mb,upload_mb,"
        "synthetic_selective_compare_seconds,changed_rows"
    )
    for target_rows in (int(value) for value in args.target_rows.split(",")):
        elapsed, target_mb, upload_mb, changed = benchmark_delta(
            target_rows,
            args.upload_rows,
            args.value_columns,
            args.repeats,
        )
        print(
            f"{target_rows},{args.upload_rows},{target_mb:.2f},{upload_mb:.2f},"
            f"{elapsed:.3f},{changed}"
        )
        results.append(
            {
                "name": f"delta {target_rows} stored / {args.upload_rows} uploaded",
                "unit": "seconds",
                "value": elapsed,
            }
        )

    print("target_gb,bandwidth_gbps,ideal_transfer_floor_seconds")
    for bandwidth in (float(value) for value in args.bandwidth_gbps.split(",")):
        print(
            f"{args.target_gb:g},{bandwidth:g},"
            f"{network_floor_seconds(args.target_gb, bandwidth):.1f}"
        )

    print(
        "parent_rows,upload_rows,match_fraction,matched_rows,created_rows,"
        "generated_id_collisions,stage_updated,parent_mb,upload_fk_mb,"
        "synthetic_selective_fk_seconds_excluding_network_and_insert"
    )
    for parent_rows in (int(value) for value in args.target_rows.split(",")):
        for match_fraction in (
            float(value) for value in args.fk_match_fractions.split(",")
        ):
            elapsed, parent_mb, upload_mb, matched, created, collisions, updated = (
                benchmark_foreign_keys(
                    parent_rows, args.upload_rows, match_fraction, args.repeats
                )
            )
            print(
                f"{parent_rows},{args.upload_rows},{match_fraction:g},{matched},"
                f"{created},{collisions},{str(updated).lower()},{parent_mb:.2f},"
                f"{upload_mb:.2f},{elapsed:.3f}"
            )
            results.append(
                {
                    "name": (
                        f"foreign keys {parent_rows} stored / "
                        f"{args.upload_rows} uploaded / {match_fraction:g} matched"
                    ),
                    "unit": "seconds",
                    "value": elapsed,
                }
            )

    partition_elapsed, partition_mb, partition_count = benchmark_time_partitions(
        args.partition_rows,
        args.partition_buckets,
        args.repeats,
    )
    print("partition_rows,partition_buckets,input_mb,unordered_partition_seconds")
    print(
        f"{args.partition_rows},{partition_count},{partition_mb:.2f},"
        f"{partition_elapsed:.3f}"
    )
    results.append(
        {
            "name": (
                f"time partitioning {args.partition_rows} rows / "
                f"{partition_count} buckets"
            ),
            "unit": "seconds",
            "value": partition_elapsed,
        }
    )

    print(
        "override_rows,scope_count,ipc_mb,ipc_roundtrip_seconds,"
        "sync_projection_seconds,projection_rows,conflicts"
    )
    for case in args.override_cases.split(","):
        operation_rows, scope_count = (int(value) for value in case.split(":"))
        ipc_elapsed, sync_elapsed, ipc_mb, projected, conflicts = (
            benchmark_arrow_override_crdt(operation_rows, scope_count, args.repeats)
        )
        print(
            f"{operation_rows},{scope_count},{ipc_mb:.2f},{ipc_elapsed:.3f},"
            f"{sync_elapsed:.3f},{projected},{conflicts}"
        )
        results.extend(
            [
                {
                    "name": (
                        f"Arrow override IPC {operation_rows} operations / "
                        f"{scope_count} scopes"
                    ),
                    "unit": "seconds",
                    "value": ipc_elapsed,
                },
                {
                    "name": (
                        f"Arrow override sync {operation_rows} operations / "
                        f"{scope_count} scopes"
                    ),
                    "unit": "seconds",
                    "value": sync_elapsed,
                },
            ]
        )

    if args.remote_table:
        dsn = os.environ.get("XTDB_BENCHMARK_DSN")
        if not dsn:
            parser.error("XTDB_BENCHMARK_DSN is required with --remote-table")
        benchmark_remote_sample(dsn, args.remote_table, args.remote_limit)

    if args.json_output:
        Path(args.json_output).write_text(json.dumps(results, indent=2) + "\n")


if __name__ == "__main__":
    main()
