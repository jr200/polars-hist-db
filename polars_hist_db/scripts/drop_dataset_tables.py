import asyncio
import logging

from .init_helpers import initialise_logging, parse_args
from ..core import AuditOps, make_engine, TableConfigOps
from ..config import Config
from ..utils.clock import Clock

LOGGER = logging.getLogger(__name__)


async def start_drop_dataset_tables(config: Config, dataset_name: str):
    tables_to_drop = set()
    for dataset in config.datasets.datasets:
        if dataset_name is None or dataset.name == dataset_name:
            tables_to_drop.update(dataset.pipeline.get_table_names())

    LOGGER.warning(f"Dropping tables: [{', '.join(tables_to_drop)}]")

    engine = make_engine(**config.db_config.to_dict())
    with engine.begin() as connection:
        TableConfigOps(connection).drop_all(config.tables)
        AuditOps(config.tables.schemas()[0]).drop(connection)

    _timings = Clock()
    _timings._df.write_clipboard()


def main():
    args = parse_args()
    initialise_logging(args.LOG_CONFIG_FILE)
    config = Config.from_yaml(args.CONFIG_FILE)

    asyncio.run(start_drop_dataset_tables(config, args.dataset))


if __name__ == "__main__":
    main()
