import asyncio
import sys
from datetime import datetime
from types import SimpleNamespace

import polars as pl

from polars_hist_db.backends.config import DbEngineConfig
from polars_hist_db.config import TableColumnConfig, TableConfig
from polars_hist_db.parity import (
    BackendParityResult,
    BackendParityTarget,
    SemanticForeignKey,
    TableParityMismatch,
    _parse_args,
    _parse_backend_target,
    _parse_ignore_column,
    _parse_semantic_foreign_key,
    _should_prepare_backend_tables,
    _table_read_sql,
    compare_backend_tables,
    format_backend_parity_result,
    main,
    prepare_parity_config,
    run_backend_parity,
)


class _InputConfig:
    def __init__(self):
        self.payload = None
        self.payload_time = None

    def set_payload(self, payload, payload_time):
        self.payload = payload
        self.payload_time = payload_time


class _Config:
    def __init__(self):
        self.db_config = DbEngineConfig(backend="mariadb")
        self.datasets = {"sample_dataset": SimpleNamespace(input_config=_InputConfig())}


def test_prepare_parity_config_overrides_backend_without_mutating_source_config():
    source = _Config()
    target = BackendParityTarget(
        name="xtdb-local",
        db_config=DbEngineConfig(backend="xtdb", hostname="127.0.0.1", port=5432),
    )
    payload_time = datetime.fromisoformat("2030-01-01T12:00:00")

    prepared = prepare_parity_config(
        source,
        target,
        dataset_name="sample_dataset",
        payload="id,name\n1,Alpha\n",
        payload_time=payload_time,
    )

    assert prepared is not source
    assert prepared.db_config.backend == "xtdb"
    assert source.db_config.backend == "mariadb"
    assert (
        prepared.datasets["sample_dataset"].input_config.payload == "id,name\n1,Alpha\n"
    )
    assert prepared.datasets["sample_dataset"].input_config.payload_time == payload_time
    assert source.datasets["sample_dataset"].input_config.payload is None


def test_compare_backend_tables_reports_row_count_and_value_mismatches():
    left_tables = {
        "sample.trades": pl.DataFrame(
            {"id": [1, 2], "trade_status": ["Delivered", "Loaded"]}
        ),
        "sample.entity_info": pl.DataFrame({"id": [501], "name": ["A"]}),
    }
    right_tables = {
        "sample.trades": pl.DataFrame(
            {"id": [1, 2], "trade_status": ["Delivered", "Ballast"]}
        ),
        "sample.entity_info": pl.DataFrame({"id": [501], "name": ["A"]}),
        "sample.location_info": pl.DataFrame({"id": [101]}),
    }

    result = compare_backend_tables(
        left_name="mariadb",
        right_name="xtdb",
        left_tables=left_tables,
        right_tables=right_tables,
        table_order=["sample.trades", "sample.entity_info", "sample.location_info"],
    )

    assert not result.is_match
    assert result.matches == ["sample.entity_info"]
    assert result.mismatches == [
        TableParityMismatch(
            table="sample.trades",
            reason="data mismatch",
            left_rows=2,
            right_rows=2,
            details=(
                "differing_columns=trade_status",
                (
                    "sample[0] left={'id': 2, 'trade_status': 'Loaded'} "
                    "right={'id': 2, 'trade_status': 'Ballast'}"
                ),
            ),
        ),
        TableParityMismatch(
            table="sample.location_info",
            reason="missing table result",
            left_rows=None,
            right_rows=1,
        ),
    ]


def test_compare_backend_tables_caps_value_mismatch_samples():
    result = compare_backend_tables(
        left_name="mariadb",
        right_name="xtdb",
        left_tables={
            "sample.trades": pl.DataFrame(
                {"id": [1, 2, 3], "trade_status": ["A", "B", "C"]}
            )
        },
        right_tables={
            "sample.trades": pl.DataFrame(
                {"id": [1, 2, 3], "trade_status": ["X", "Y", "Z"]}
            )
        },
        table_order=["sample.trades"],
        mismatch_sample_limit=2,
    )

    assert result.mismatches[0].details == (
        "differing_columns=trade_status",
        (
            "sample[0] left={'id': 1, 'trade_status': 'A'} "
            "right={'id': 1, 'trade_status': 'X'}"
        ),
        (
            "sample[1] left={'id': 2, 'trade_status': 'B'} "
            "right={'id': 2, 'trade_status': 'Y'}"
        ),
        "1 additional differing rows omitted",
    )


def test_compare_backend_tables_reports_unresolved_semantic_foreign_keys():
    result = compare_backend_tables(
        left_name="mariadb",
        right_name="xtdb",
        left_tables={
            "sample.location_info": pl.DataFrame({"id": [1], "name": ["Alpha Site"]}),
            "sample.trades": pl.DataFrame(
                {"id": [10, 11], "origin_location_id": [1, 999]}
            ),
        },
        right_tables={
            "sample.location_info": pl.DataFrame({"id": [1], "name": ["Alpha Site"]}),
            "sample.trades": pl.DataFrame(
                {"id": [10, 11], "origin_location_id": [1, 999]}
            ),
        },
        table_order=["sample.trades"],
        semantic_foreign_keys=(
            SemanticForeignKey(
                source_table="sample.trades",
                source_column="origin_location_id",
                target_table="sample.location_info",
                target_column="id",
                target_columns=("name",),
            ),
        ),
    )

    assert result.semantic_foreign_key_diagnostics == (
        (
            "mariadb unresolved semantic_fk "
            "sample.trades.origin_location_id -> sample.location_info.id "
            "using name: 1/2 source rows"
        ),
        (
            "xtdb unresolved semantic_fk "
            "sample.trades.origin_location_id -> sample.location_info.id "
            "using name: 1/2 source rows"
        ),
    )


def test_format_backend_parity_result_includes_details_and_diagnostics():
    result = BackendParityResult(
        left_name="mariadb",
        right_name="xtdb",
        matches=["sample.entity_info"],
        mismatches=[
            TableParityMismatch(
                table="sample.trades",
                reason="data mismatch",
                left_rows=1,
                right_rows=1,
                details=("differing_columns=trade_status",),
            )
        ],
        semantic_foreign_key_diagnostics=(
            (
                "xtdb unresolved semantic_fk "
                "sample.trades.origin_location_id -> sample.location_info.id "
                "using name: 1/2 source rows"
            ),
        ),
    )

    assert list(format_backend_parity_result(result)) == [
        "PASS sample.entity_info",
        (
            "WARN xtdb unresolved semantic_fk "
            "sample.trades.origin_location_id -> sample.location_info.id "
            "using name: 1/2 source rows"
        ),
        "FAIL sample.trades: data mismatch (mariadb=1, xtdb=1)",
        "  differing_columns=trade_status",
    ]


def test_compare_backend_tables_can_resolve_backend_local_surrogate_keys():
    left_tables = {
        "sample.location_info": pl.DataFrame(
            {
                "id": [1, 2],
                "name": ["Alpha Site", "#Unspecified"],
                "country": ["Nigeria", "#Unspecified"],
            }
        ),
        "sample.trades": pl.DataFrame(
            {
                "id": [308025, 308327],
                "origin_location_id": [1, 2],
                "destination_location_id": [2, 2],
            }
        ),
    }
    right_tables = {
        "sample.location_info": pl.DataFrame(
            {
                "id": [1701, -1408044658],
                "name": ["Alpha Site", "#Unspecified"],
                "country": ["Nigeria", "#Unspecified"],
            }
        ),
        "sample.trades": pl.DataFrame(
            {
                "id": [308025, 308327],
                "origin_location_id": [1701, -1408044658],
                "destination_location_id": [-1408044658, -1408044658],
            }
        ),
    }

    result = compare_backend_tables(
        left_name="mariadb",
        right_name="xtdb",
        left_tables=left_tables,
        right_tables=right_tables,
        table_order=["sample.location_info", "sample.trades"],
        ignored_columns_by_table={"sample.location_info": frozenset({"id"})},
        semantic_foreign_keys=(
            SemanticForeignKey(
                source_table="sample.trades",
                source_column="origin_location_id",
                target_table="sample.location_info",
                target_column="id",
                target_columns=("name", "country"),
            ),
            SemanticForeignKey(
                source_table="sample.trades",
                source_column="destination_location_id",
                target_table="sample.location_info",
                target_column="id",
                target_columns=("name", "country"),
            ),
        ),
    )

    assert result.is_match
    assert result.matches == ["sample.location_info", "sample.trades"]
    assert result.mismatches == []


def test_parse_semantic_foreign_key_accepts_cli_mapping():
    semantic_fk = _parse_semantic_foreign_key(
        "sample.trades.origin_location_id=sample.location_info.id:name,country"
    )

    assert semantic_fk == SemanticForeignKey(
        source_table="sample.trades",
        source_column="origin_location_id",
        target_table="sample.location_info",
        target_column="id",
        target_columns=("name", "country"),
    )


def test_parse_ignore_column_accepts_qualified_column():
    assert _parse_ignore_column("sample.location_info.id") == (
        "sample.location_info",
        "id",
    )


def test_parse_args_leaves_payload_time_unset_for_file_based_runs(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        ["polars-hist-db-parity", "--config", "configs/sample.yaml"],
    )

    args = _parse_args()

    assert args.payload_time is None


def test_main_passes_cli_parity_rules_to_backend_parity(monkeypatch):
    captured = {}

    class _Config:
        parity = SimpleNamespace(
            ignore_columns=("sample.location_info.id",),
            semantic_foreign_keys=(
                SimpleNamespace(
                    source="sample.trades.origin_location_id",
                    target="sample.location_info.id",
                    columns=("name", "country"),
                ),
            ),
        )

    async def fake_run_backend_parity(*args, **kwargs):
        captured.update(kwargs)
        return BackendParityResult(
            left_name="mariadb",
            right_name="xtdb",
            matches=[],
            mismatches=[],
        )

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "polars-hist-db-parity",
            "--config",
            "configs/sample.yaml",
            "--ignore-column",
            "sample.location_info.created_at",
            "--semantic-fk",
            "sample.trades.origin_location_id=sample.location_info.id:name,country",
        ],
    )
    monkeypatch.setattr(
        "polars_hist_db.parity.PolarsHistDbConfig.from_yaml",
        lambda path: _Config(),
    )
    monkeypatch.setattr(
        "polars_hist_db.parity.run_backend_parity",
        fake_run_backend_parity,
    )

    main()

    assert captured["ignored_columns_by_table"] == {
        "sample.location_info": frozenset({"created_at"})
    }
    assert captured["semantic_foreign_keys"] == (
        SemanticForeignKey(
            source_table="sample.trades",
            source_column="origin_location_id",
            target_table="sample.location_info",
            target_column="id",
            target_columns=("name", "country"),
        ),
    )


def test_run_backend_parity_uses_parity_rules_from_config(monkeypatch):
    class _Engine:
        def __init__(self, backend_name):
            self.backend_name = backend_name
            self.disposed = False

        def dispose(self):
            self.disposed = True

    class _Backend:
        def __init__(self, backend_name):
            self.name = backend_name

        def create_engine(self, db_config):
            return _Engine(db_config.backend)

    source_config = SimpleNamespace(
        db_config=DbEngineConfig(backend="mariadb"),
        datasets={"trades_dataset": SimpleNamespace(input_config=_InputConfig())},
        tables=SimpleNamespace(
            items=[
                TableConfig(
                    schema="sample",
                    name="location_info",
                    is_temporal=False,
                    primary_keys=["id"],
                    columns=[
                        TableColumnConfig("location_info", "id", "INT"),
                        TableColumnConfig("location_info", "name", "VARCHAR"),
                        TableColumnConfig("location_info", "country", "VARCHAR"),
                    ],
                ),
                TableConfig(
                    schema="sample",
                    name="trades",
                    is_temporal=True,
                    primary_keys=["id"],
                    columns=[
                        TableColumnConfig("trades", "id", "INT"),
                        TableColumnConfig("trades", "origin_location_id", "INT"),
                        TableColumnConfig(
                            "trades",
                            "destination_location_id",
                            "INT",
                        ),
                    ],
                ),
            ]
        ),
        parity=SimpleNamespace(
            ignore_columns=("sample.location_info.id",),
            semantic_foreign_keys=(
                SimpleNamespace(
                    source="sample.trades.origin_location_id",
                    target="sample.location_info.id",
                    columns=("name", "country"),
                ),
                SimpleNamespace(
                    source="sample.trades.destination_location_id",
                    target="sample.location_info.id",
                    columns=("name", "country"),
                ),
            ),
        ),
    )
    tables_by_backend = {
        "mariadb": {
            "sample.location_info": pl.DataFrame(
                {
                    "id": [1, 2],
                    "name": ["Alpha Site", "#Unspecified"],
                    "country": ["Nigeria", "#Unspecified"],
                }
            ),
            "sample.trades": pl.DataFrame(
                {
                    "id": [308025, 308327],
                    "origin_location_id": [1, 2],
                    "destination_location_id": [2, 2],
                }
            ),
        },
        "xtdb": {
            "sample.location_info": pl.DataFrame(
                {
                    "id": [1701, -1408044658],
                    "name": ["Alpha Site", "#Unspecified"],
                    "country": ["Nigeria", "#Unspecified"],
                }
            ),
            "sample.trades": pl.DataFrame(
                {
                    "id": [308025, 308327],
                    "origin_location_id": [1701, -1408044658],
                    "destination_location_id": [-1408044658, -1408044658],
                }
            ),
        },
    }

    async def fake_run_datasets(*args, **kwargs):
        return None

    monkeypatch.setattr(
        "polars_hist_db.parity.backend_from_config",
        lambda db_config: _Backend(db_config.backend),
    )
    monkeypatch.setattr(
        "polars_hist_db.parity._should_prepare_backend_tables",
        lambda backend_name: False,
    )
    monkeypatch.setattr("polars_hist_db.parity.run_datasets", fake_run_datasets)
    monkeypatch.setattr(
        "polars_hist_db.parity.read_parity_tables",
        lambda engine, *args, **kwargs: tables_by_backend[engine.backend_name],
    )

    result = asyncio.run(
        run_backend_parity(
            source_config,
            left=BackendParityTarget(
                name="mariadb",
                db_config=DbEngineConfig(backend="mariadb"),
            ),
            right=BackendParityTarget(
                name="xtdb",
                db_config=DbEngineConfig(backend="xtdb"),
            ),
            dataset_name="trades_dataset",
        )
    )

    assert result.is_match
    assert result.matches == ["sample.location_info", "sample.trades"]


def test_parse_backend_target_accepts_named_connection_options():
    target = _parse_backend_target(
        "left=mariadb,hostname=127.0.0.1,port=13307,username=root,password=admin"
    )

    assert target.name == "left"
    assert target.db_config.backend == "mariadb"
    assert target.db_config.hostname == "127.0.0.1"
    assert target.db_config.port == 13307
    assert target.db_config.username == "root"
    assert target.db_config.password == "admin"


def test_parse_backend_target_accepts_unnamed_connection_options():
    target = _parse_backend_target("xtdb,hostname=127.0.0.1,port=33126")

    assert target.name == "xtdb"
    assert target.db_config.backend == "xtdb"
    assert target.db_config.hostname == "127.0.0.1"
    assert target.db_config.port == 33126


def test_table_read_sql_uses_valid_time_all_for_xtdb_tables():
    table_config = TableConfig(
        schema="sample",
        name="trades",
        is_temporal=True,
        primary_keys=["id"],
        columns=[TableColumnConfig("trades", "id", "INT")],
    )

    system_time = datetime.fromisoformat("2035-01-01T00:00:01")

    assert _table_read_sql("xtdb", table_config, system_time=system_time) == (
        "SELECT * FROM sample.trades FOR VALID_TIME ALL "
        "FOR SYSTEM_TIME AS OF TIMESTAMP '2035-01-01T00:00:01'"
    )
    assert _table_read_sql("mariadb", table_config, system_time=system_time) is None


def test_table_read_sql_uses_valid_time_asof_for_xtdb_non_temporal_tables():
    table_config = TableConfig(
        schema="sample",
        name="entity_info",
        is_temporal=False,
        primary_keys=["id"],
        columns=[TableColumnConfig("entity_info", "id", "INT")],
    )
    system_time = datetime.fromisoformat("2035-01-01T00:00:01")

    assert _table_read_sql("xtdb", table_config, system_time=system_time) == (
        "SELECT * FROM sample.entity_info "
        "FOR VALID_TIME AS OF TIMESTAMP '2035-01-01T00:00:01' "
        "FOR SYSTEM_TIME AS OF TIMESTAMP '2035-01-01T00:00:01'"
    )


def test_backend_table_prepare_is_skipped_for_xtdb():
    assert not _should_prepare_backend_tables("xtdb")
    assert _should_prepare_backend_tables("mariadb")
