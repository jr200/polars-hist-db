import nats
from typing import Any, Dict, List
import logging

LOGGER = logging.getLogger(__name__)


async def make_nats_client(
    nats_servers: List[str], options: Dict[str, Any]
) -> nats.NATS:
    nc = nats.NATS()

    async def error_cb(e):
        LOGGER.error("Error:", e)

    async def closed_cb():
        LOGGER.info("Connection to NATS is closed.")

    async def reconnected_cb():
        LOGGER.warning(f"ReConnected to NATS at {nc.connected_url.netloc}...")

    # async def discovered_server_cb():
    #     LOGGER.info(f"Discovered server at {nc.connected_url.netloc}...")

    async def disconnected_cb():
        LOGGER.info(f"Disconnected from NATS. Stats: {nc.stats}")

    options["error_cb"] = error_cb
    options["closed_cb"] = closed_cb
    options["reconnected_cb"] = reconnected_cb
    options["disconnected_cb"] = disconnected_cb
    # options["discovered_server_cb"] = discovered_server_cb

    LOGGER.info(f"Connecting to NATS servers: {nats_servers}")
    await nc.connect(nats_servers, **options)
    return nc
