import json
from datetime import datetime
from nats.aio.msg import Msg

import polars as pl

from ..config.input.ingest_fn_registry import IngestFnRegistry
from ..config.input.jetstream_config import JetstreamIngestConfig


def load_df_from_msg(
    msg: Msg, ts: datetime, ingest_config: JetstreamIngestConfig
) -> pl.DataFrame:
    data = json.loads(msg.data.decode())

    fn_name = data.get("fn_loader_name", ingest_config.fn_name)
    fn_args = data.get("fn_loader_args", ingest_config.fn_args)

    fn_reg = IngestFnRegistry()
    df = fn_reg.call_function(data, ts, fn_name, fn_args)

    return df
