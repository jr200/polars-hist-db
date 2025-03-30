from datetime import datetime
import logging

from sqlalchemy import Connection

from ..config import TableConfigs, DatasetConfig
from ..core import TableConfigOps, DeltaTableOps, TableOps

LOGGER = logging.getLogger(__name__)


def scrape_primary_item(
    dataset: DatasetConfig,
    tables: TableConfigs,
    upload_time: datetime,
    connection: Connection,
):
    pipeline = dataset.pipeline
    delta_table_schema = dataset.delta_table_schema
    delta_table_name = dataset.name
    main_table_config = tables[pipeline.get_main_table_name()]
    assert main_table_config.delta_config is not None
    LOGGER.debug("(item %d) scraping item %s", -1, main_table_config.name)

    selected_columns = (
        pipeline.extract_items(main_table_config.name).get_column("source").to_list()
    )

    TableConfigOps(connection).create(main_table_config)
    tbo = TableOps(delta_table_schema, delta_table_name, connection)
    common_columns = [c.name for c in tbo.get_column_intersection(selected_columns)]

    if selected_columns is not None:
        if len(common_columns) != len(selected_columns):
            cols_not_configured = set(common_columns).symmetric_difference(
                selected_columns
            )
            raise ValueError(f"column mismatch on {cols_not_configured}")

    ni, nu, nd = DeltaTableOps(
        delta_table_schema, delta_table_name, main_table_config.delta_config, connection
    ).upsert(
        main_table_config.name,
        upload_time,
        common_columns,
    )

    LOGGER.debug("(item %d) upserted %d rows", -1, ni + nu)

    # TODO: trigger table mod notification
