from dataclasses import dataclass
from typing import Any, Dict, Optional, Union

from sqlalchemy import Engine
from sqlalchemy import create_engine


@dataclass
class SslConfig:
    ssl_ca: str
    ssl_cert: Optional[str] = None
    ssl_key: Optional[str] = None


_engine_cache: Dict[int, Engine] = {}


@dataclass
class DbEngineConfig:
    hostname: str
    backend: str = "mariadb"
    port: int = 3306
    username: Optional[str] = None
    password: Optional[str] = None
    ssl_config: Union[Dict[str, Any], SslConfig, None] = None
    pool_size: int = 3
    max_overflow: int = 2

    def __post_init__(self):
        self.port = int(self.port)
        if isinstance(self.ssl_config, dict):
            ssl_dict = self.ssl_config
            self.ssl_config = SslConfig(
                ssl_ca=ssl_dict["ssl_ca"],
                ssl_cert=ssl_dict.get("ssl_cert"),
                ssl_key=ssl_dict.get("ssl_key"),
            )

    def get_engine(self) -> Engine:
        key = id(self)
        if key not in _engine_cache:
            kwargs = {
                "backend": self.backend,
                "hostname": self.hostname,
                "port": self.port,
                "username": self.username,
                "password": self.password,
                "ssl_config": self.ssl_config,
                "pool_size": self.pool_size,
                "max_overflow": self.max_overflow,
            }
            # Convert SslConfig back to dict for the engine builder
            if isinstance(kwargs["ssl_config"], SslConfig):
                sc = kwargs["ssl_config"]
                kwargs["ssl_config"] = {
                    "ssl_ca": sc.ssl_ca,
                    "ssl_cert": sc.ssl_cert,
                    "ssl_key": sc.ssl_key,
                }
            _engine_cache[key] = _make_engine(**kwargs)
        return _engine_cache[key]

    def dispose(self) -> None:
        key = id(self)
        if key in _engine_cache:
            _engine_cache[key].dispose()
            del _engine_cache[key]


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
