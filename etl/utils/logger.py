import logging
import logging.config

LOGGING = {
    "version": 1,
    "disable_existing_loggers": True,
    "formatters": {
        "verbose": {"format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"}
    },
    "handlers": {
        "console": {
            "level": "INFO",
            "class": "logging.StreamHandler",
            "formatter": "verbose",
        },
        # Add slack handler.
    },
    "loggers": {
        "logger": {"handlers": ["console"], "level": "DEBUG", "propagate": False}
    },
}

logging.config.dictConfig(LOGGING)
logger = logging.getLogger("logger")
