from typing import Union
import pyarrow as pa
import polars as pl
import base64


def to_ipc_b64(df: Union[pl.DataFrame, pa.Table]) -> str:
    buffer = df.write_ipc_stream(None, compression=None)
    base64_str = base64.b64encode(buffer.getvalue()).decode("utf-8")
    return base64_str


def from_ipc_b64(payload: str) -> pl.DataFrame:
    decoded = base64.b64decode(payload)
    df = pl.read_ipc_stream(decoded)
    return df
