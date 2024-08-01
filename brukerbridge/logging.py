import logging
import logging.config
import logging.handlers
from queue import Queue
from typing import Callable

logger = logging.getLogger(__name__)


def configure_logging(log_dir: str):
    """Three file handlers rotating once a day: INFO and above, DEBUG and above and ERROR

    DEBUG level messages emitted by PIL are filtered out as it is extremely chatty (emits MB of logs in short order)
    """
    logging_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "class": "logging.Formatter",
                "format": "%(asctime)s  %(levelname)-8s %(name)-40s %(processName)-16s %(message)s",
            }
        },
        "filters": {"filter_pil_debug": {"()": FilterDebug, "name": "PIL"}},
        "handlers": {
            "rh_info": {
                "class": "logging.handlers.TimedRotatingFileHandler",
                "filename": f"{log_dir}/bridge.log",
                "when": "D",
                "interval": 1,
                "level": "INFO",
                "formatter": "default",
            },
            "rh_debug": {
                "class": "logging.handlers.TimedRotatingFileHandler",
                "filename": f"{log_dir}/bridge_debug.log",
                "when": "D",
                "interval": 1,
                "level": "DEBUG",
                "formatter": "default",
                "filters": ["filter_pil_debug"],
            },
            "rh_error": {
                "class": "logging.handlers.TimedRotatingFileHandler",
                "filename": f"{log_dir}/bridge_error.log",
                "when": "D",
                "interval": 1,
                "level": "ERROR",
                "formatter": "default",
            },
            "sh": {
                "class": "logging.StreamHandler",
                "level": "INFO",
                "formatter": "default",
            },
        },
        "root": {
            "level": "DEBUG",
            "handlers": ["rh_info", "rh_debug", "rh_error", "sh"],
        },
    }

    logging.config.dictConfig(logging_config)


class FilterDebug(logging.Filter):
    """Filter out all records at the DEBUG level for the 'name' logger and its descendants"""

    def __init__(self, name):
        self.name = name

    def filter(self, record):
        return not (
            record.name.startswith(self.name) and record.levelno == logging.DEBUG
        )


def logger_thread(log_queue: Queue):
    """Processes logs enqueued by QueueHandlers, presumably running in other processes"""
    logger.debug("Started logging thread")
    while True:
        record = log_queue.get()
        # catch sentinel
        if record is None:
            break
        record_logger = logging.getLogger(record.name)
        record_logger.handle(record)


def worker_process(fn: Callable, log_queue: Queue, *args):
    """Logging initialization for concurrent workers"""
    qh = logging.handlers.QueueHandler(log_queue)
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.addHandler(qh)
    logger.debug("Executing %s with args %s", fn.__name__, args)
    fn(*args)
