from typing import Union
import polars as pl
import base64


def to_ipc_b64(df: pl.DataFrame) -> bytes:
    buffer = df.write_ipc_stream(None, compression="uncompressed")
    base64_bytes = base64.b64encode(buffer.getvalue())
    return base64_bytes


def from_ipc_b64(payload: Union[str, bytes]) -> pl.DataFrame:
    decoded = base64.b64decode(payload)
    df = pl.read_ipc_stream(decoded)
    return df
