import logging
import logging.config
import logging.handlers

logger = logging.getLogger(__name__)


def configure_logging(log_dir):
    logging_config = {
        "version": 1,
        "formatters": {
            "default": {
                "class": "logging.Formatter",
                "format": "%(asctime)s %(name)-15s %(levelname)-8s %(processName)-15s %(message)s",
            }
        },
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


def logger_thread(log_queue):
    """Processes logs enqueued by QueueHandlers, presumably running in other processes"""
    logger.debug("Started logging thread")
    while True:
        record = log_queue.get()
        # catch sentinel
        if record is None:
            break
        record_logger = logging.getLogger(record.name)
        record_logger.handle(record)


def worker_process(fn, log_queue, *args):
    """Logging initialization for concurrent workers"""
    qh = logging.handlers.QueueHandler(log_queue)
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.addHandler(qh)
    logger.debug("Executing %s with args %s", fn.__name__, args)
    fn(*args)
