from typing import Union, Optional
import polars as pl
import base64
from polars._typing import IpcCompression


def to_ipc_b64(df: pl.DataFrame, compression: Optional[IpcCompression] = None) -> bytes:
    if compression is None:
        compression = "uncompressed"

    buffer = df.write_ipc_stream(None, compression=compression)
    base64_bytes = base64.b64encode(buffer.getvalue())
    return base64_bytes


def from_ipc_b64(payload: Union[str, bytes]) -> pl.DataFrame:
    decoded = base64.b64decode(payload)
    df = pl.read_ipc_stream(decoded)
    return df
