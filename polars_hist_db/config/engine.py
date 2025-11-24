from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional

from sqlalchemy import Engine
from sqlalchemy import create_engine


@dataclass
class SslConfig:
    ssl_ca: str
    ssl_cert: Optional[str] = None
    ssl_key: Optional[str] = None


@dataclass
class DbEngineConfig:
    hostname: str
    backend: str = "mariadb"
    port: int = 3306
    username: Optional[str] = None
    password: Optional[str] = None
    ssl_config: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        if isinstance(self.ssl_config, dict):
            self.ssl_config = SslConfig(**self.ssl_config)

    def get_engine(self) -> Engine:
        return _make_engine(**asdict(self))


def _make_engine(**kwargs) -> Engine:
    backend = kwargs.pop("backend", None)
    if backend == "mariadb":
        return _mariadb_engine(**kwargs)

    raise ValueError(f"unsupported database: {backend}")


def _mariadb_engine(
    hostname: str,
    port: int,
    username: Optional[str],
    password: Optional[str],
    ssl_config: Optional[Dict[str, Any]],
    use_insertmanyvalues=True,
    **kwargs,
) -> Engine:
    if username is None and password is None:
        url = f"mariadb+pymysql://{hostname}:{port}"
    else:
        url = f"mariadb+pymysql://{username}:{password}@{hostname}:{port}"

    # https://github.com/sqlalchemy/sqlalchemy/issues/3146
    # updates should return the number of rows affected, rather than the number of rows that matched
    # the where clause
    connect_args: Dict[str, Any] = {"client_flag": 0}

    if ssl_config is not None:
        ssl_params = {k: v for k, v in ssl_config.items() if v is not None}
        ssl_params["__fake_param"] = "_unused_"
        connect_args["ssl"] = ssl_params

    engine = create_engine(
        url,
        pool_recycle=3600,
        use_insertmanyvalues=use_insertmanyvalues,
        connect_args=connect_args,
        **kwargs,
    )

    return engine
