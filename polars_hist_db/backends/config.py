from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal

BackendName = Literal["mariadb", "xtdb", "mssql"]


def _default_port(backend: str) -> int:
    if backend == "xtdb":
        return 5432
    if backend == "mssql":
        return 1433
    return 3306


def _default_adbc_port(backend: str) -> int | None:
    if backend == "xtdb":
        return 9832
    return None


def _default_max_rows_per_insert(backend: str) -> int | None:
    if backend == "xtdb":
        return 10_000
    return None


def _parse_port(value: Any, backend: str) -> int:
    if value is None:
        return _default_port(backend)
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return _default_port(backend)


def _parse_optional_port(value: Any, backend: str) -> int | None:
    if value is None:
        return _default_adbc_port(backend)
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return _default_adbc_port(backend)


def _parse_optional_positive_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str) and value.isdigit():
        parsed = int(value)
    else:
        raise ValueError(f"Expected a positive integer or null, got {value!r}")

    if parsed < 1:
        raise ValueError(f"Expected a positive integer or null, got {value!r}")
    return parsed


def _parse_max_rows_per_insert(value: Any, backend: str) -> int | None:
    if value is None:
        return _default_max_rows_per_insert(backend)
    return _parse_optional_positive_int(value)


@dataclass(frozen=True)
class DbEngineConfig:
    backend: BackendName = "mariadb"
    hostname: str = "127.0.0.1"
    port: int = 3306
    adbc_port: int | None = None
    database: str | None = None
    username: str | None = None
    password: str | None = None
    ssl_config: Mapping[str, Any] | None = None
    pool_size: int = 3
    max_overflow: int = 2
    max_rows_per_insert: int | None = None

    @classmethod
    def from_mapping(cls, cfg: Mapping[str, Any]) -> "DbEngineConfig":
        backend = str(cfg.get("backend", "mariadb")).lower()
        return cls(
            backend=_validate_backend(backend),
            hostname=str(cfg.get("hostname", "127.0.0.1")),
            port=_parse_port(cfg.get("port"), backend),
            adbc_port=_parse_optional_port(cfg.get("adbc_port"), backend),
            database=cfg.get("database"),
            username=cfg.get("username"),
            password=cfg.get("password"),
            ssl_config=cfg.get("ssl_config"),
            pool_size=int(cfg.get("pool_size", 3)),
            max_overflow=int(cfg.get("max_overflow", 2)),
            max_rows_per_insert=_parse_max_rows_per_insert(
                cfg.get("max_rows_per_insert"), backend
            ),
        )


def _validate_backend(backend: str) -> BackendName:
    if backend in {"mariadb", "xtdb", "mssql"}:
        return backend  # type: ignore[return-value]
    raise ValueError(f"Unsupported database backend '{backend}'")
