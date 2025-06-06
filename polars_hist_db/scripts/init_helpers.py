import argparse
import logging
import logging.config
import os
import sys

import yaml


def parse_args() -> argparse.Namespace:
    argv = sys.argv[1:]
    parser = argparse.ArgumentParser(add_help=True)

    parser.add_argument(
        "dataset",
        help="Dataset to run",
    )

    parser.add_argument(
        "-f",
        "--config",
        help="Path to config file",
        default=None,
        dest="CONFIG_FILE",
        required=True,
    )

    default_log_config_file = os.path.normpath(
        os.path.join(os.path.dirname(__file__), "..", "logging.yaml")
    )
    parser.add_argument(
        "-l",
        "--log-config",
        help="Path to logger config",
        default=str(default_log_config_file),
        action="store",
        dest="LOG_CONFIG_FILE",
    )

    args = parser.parse_args(argv)

    return args


def initialise_logging(filename: str) -> None:
    try:
        with open(filename, "r") as file:
            logging_dict = yaml.safe_load(file)
        logging.config.dictConfig(logging_dict)
    except FileNotFoundError:
        print(f"FATAL: Logger config file not found at {filename}")
    except Exception as e:
        logger = logging.getLogger("UNCAUGHT_EXCEPTION")
        logger.fatal("", exc_info=e)

    # Set up exception hook
    def handle_exception(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return

        logger = logging.getLogger("UNCAUGHT_EXCEPTION")
        logger.fatal("", exc_info=(exc_type, exc_value, exc_traceback))

    sys.excepthook = handle_exception
