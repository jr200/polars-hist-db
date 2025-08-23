from datetime import datetime
import logging
from typing import Dict, Any, Callable, Awaitable, Optional, List
import pytz
from sqlalchemy.engine import Connection

from .audit import AuditOps

LOGGER = logging.getLogger(__name__)

TableUpdateCallback = Callable[[str, str, datetime], Awaitable[None]]

class AuditLogTracker:
    _borg: Dict[str, Any] = {}

    last_known_updates: Dict[str, datetime]
    table_update_callback: Optional[TableUpdateCallback]

    def __init__(self):
        self.__dict__ = self._borg
        if "last_known_updates" not in self._borg:
            self._borg["last_known_updates"] = dict()
        if "table_update_callback" not in self._borg:
            self._borg["table_update_callback"] = None

    def update_last_known_update(self, table_key: str, timestamp: datetime):
        self.last_known_updates[table_key] = timestamp

    def set_table_update_callback(self, cb: TableUpdateCallback):
        self.table_update_callback = cb

    def clear_updates(self):
        self.last_known_updates.clear()

    async def check_for_updates(
        self, epoch_ms: int, schemas: List[str], connection: Connection
    ):
        # this is implemented as a polling task to allow for historic-replays
        # options were:
        # 1. listen to actual db update events
        # 2. preload the audit table with known trigger times
        # 3. poll the audit table for new entries
        #
        # (1) would tie this service to actual db updates/writes, making more work for historic playbacks
        # (2) works for historic data, but if new data comes along, the preloaded audit would be out of date
        # (3) ugly, but covers both cases with minimal code

        if self.table_update_callback is None:
            raise ValueError("Developer Error: table_update_callback is not set")

        audit_ops = [AuditOps(schema) for schema in schemas]
        asof_timestamp = datetime.fromtimestamp(epoch_ms / 1000, tz=pytz.utc)

        for aops in audit_ops:
            last_updated_map = aops.get_latest_entry(
                connection, asof_timestamp=asof_timestamp
            ).select("table_name", "data_source_ts")

            # Check for updates and invoke callback
            if not last_updated_map.is_empty():
                for row in last_updated_map.iter_rows():
                    table_name = row[0]
                    new_timestamp = row[1]

                    # Check if this table has been updated
                    table_key = f"{aops.schema}.{table_name}"
                    last_known = self.last_known_updates.get(table_key)

                    if last_known is None or new_timestamp > last_known:
                        # Table has been updated, invoke callback
                        try:
                            await self.table_update_callback(aops.schema, table_name, new_timestamp)
                            LOGGER.debug(
                                f"Callback invoked for table {aops.schema}.{table_name} at {new_timestamp}"
                            )
                        except Exception as e:
                            LOGGER.error(
                                f"Error in table update callback for {aops.schema}.{table_name}: {e}"
                            )

                        # Update the last known timestamp
                        self.update_last_known_update(table_key, new_timestamp)
