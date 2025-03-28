from typing import Optional
from sqlalchemy import create_engine, Engine


def make_engine(**kwargs) -> Engine:
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

    engine = create_engine(
        url, pool_recycle=3600, use_insertmanyvalues=use_insertmanyvalues, **kwargs
    )

    return engine
