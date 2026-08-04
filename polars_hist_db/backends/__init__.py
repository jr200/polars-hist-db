from .base import HistoricalDbBackend, TableHealthResult
from .config import BackendName, DbEngineConfig

__all__ = [
    "BackendName",
    "DbEngineConfig",
    "HistoricalDbBackend",
    "MariaDbBackend",
    "TableHealthResult",
    "XtdbBackend",
    "backend_from_config",
    "get_backend",
]


def __getattr__(name: str):
    if name == "MariaDbBackend":
        from .mariadb import MariaDbBackend

        return MariaDbBackend
    if name == "XtdbBackend":
        from .xtdb import XtdbBackend

        return XtdbBackend
    if name == "backend_from_config":
        from .registry import backend_from_config

        return backend_from_config
    if name == "get_backend":
        from .registry import get_backend

        return get_backend
    raise AttributeError(name)
