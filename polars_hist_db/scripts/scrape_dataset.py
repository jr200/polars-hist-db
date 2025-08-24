import asyncio
import logging

from .init_helpers import initialise_logging, parse_args
from ..config import PolarsHistDbConfig
from ..dataset import run_datasets

LOGGER = logging.getLogger(__name__)


async def start_scrape_dataset(config: PolarsHistDbConfig, dataset_name: str):
    engine = config.db_config.get_engine()
    await run_datasets(config, engine, dataset_name)


def main():
    args = parse_args()
    initialise_logging(args.LOG_CONFIG_FILE)
    config = PolarsHistDbConfig.from_yaml(args.CONFIG_FILE)

    asyncio.run(start_scrape_dataset(config, args.dataset))


if __name__ == "__main__":
    main()
