import logging
import logging.config

from config.config import SLACK_WEBHOOK_URL
from webhook_logger.slack import SlackHandler
from webhook_logger.slack import SlackFormatter

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,  # What is this?
    "formatters": {
        "verbose": {"format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"},
        "slack_format": {"()": "webhook_logger.slack.SlackFormatter"},
    },
    "handlers": {
        "console": {
            "level": "INFO",
            "class": "logging.StreamHandler",
            "formatter": "verbose",
        },
        "slack": {
            "level": "INFO",
            "class": "webhook_logger.slack.SlackHandler",
            "hook_url": SLACK_WEBHOOK_URL,
            "formatter": "slack_format",
        },
    },
    "loggers": {"logger": {"handlers": ["console", "slack"], "level": "DEBUG"}},
}

logging.config.dictConfig(LOGGING)
logger = logging.getLogger("logger")
