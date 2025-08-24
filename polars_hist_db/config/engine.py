from dataclasses import dataclass, asdict
from typing import Optional

from sqlalchemy import Engine
from sqlalchemy import create_engine


@dataclass
class DbEngineConfig:
    hostname: str
    backend: str = "mariadb"
    port: int = 3306
    username: Optional[str] = None
    password: Optional[str] = None

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
    connect_args={"client_flag": 0}

    engine = create_engine(
        url,
        pool_recycle=3600,
        use_insertmanyvalues=use_insertmanyvalues,
        connect_args=connect_args,
        **kwargs,
    )

    return engine
