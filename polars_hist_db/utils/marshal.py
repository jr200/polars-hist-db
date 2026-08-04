import base64
import zlib
from typing import Literal

import polars as pl

type IpcCompression = Literal["uncompressed", "lz4", "zstd", "zlib"]


def to_ipc_b64(df: pl.DataFrame, compression: IpcCompression | None = None) -> bytes:
    if compression is None:
        compression = "uncompressed"

    if compression == "zlib":
        uncompressed_buffer = df.write_ipc_stream(None, compression="uncompressed")
        buffer = zlib.compress(uncompressed_buffer.getvalue())
    else:
        compressed_buffer = df.write_ipc_stream(None, compression=compression)
        buffer = compressed_buffer.getvalue()

    base64_bytes = base64.b64encode(buffer)
    return base64_bytes


def from_ipc_b64(payload: str | bytes, use_zlib: bool = False) -> pl.DataFrame:
    decoded = base64.b64decode(payload)
    if use_zlib:
        decoded = zlib.decompress(decoded)
    df = pl.read_ipc_stream(decoded)
    return df
