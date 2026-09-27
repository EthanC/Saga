"""Configure Saga's Loguru sinks."""

import copy
import logging
import re
from logging import LogRecord
from sys import stderr, stdout

from loguru import logger
from loguru_discord import DiscordSink

from core.config import LoggingSettings, Settings

DISCORD_WEBHOOK_PATTERN = re.compile(
    r"(?:https://(?:www\.)?discord\.com)?"
    r"/api/webhooks/\d+/[A-Za-z0-9._-]+",
    re.IGNORECASE,
)
DISCORD_MESSAGE_LIMIT = 1800


class _SafeDiscordSink(logging.Handler):
    """Redact and bound records while isolating webhook transport failures."""

    def __init__(self, webhook_url: str) -> None:
        super().__init__()
        self.sink = DiscordSink(webhook_url)

    def emit(self, record: LogRecord) -> None:
        safe_record = copy.copy(record)
        safe_record.msg = redact_discord_webhooks(record.getMessage())[
            :DISCORD_MESSAGE_LIMIT
        ]
        safe_record.args = ()
        try:
            self.sink.emit(safe_record)
        except Exception as error:
            stderr.write(
                "Discord log forwarding failed "
                f"({type(error).__name__}); see console logs\n"
            )


def configure_logging(settings: Settings | LoggingSettings) -> None:
    """Configure console, Discord, and standard-library logging."""
    logger.level(settings.log_level)
    logger.level(settings.log_discord_level)
    logger.remove()
    logger.add(stdout, level=settings.log_level, backtrace=False, diagnose=False)

    if settings.log_discord_webhook_url is not None:
        logger.add(
            _SafeDiscordSink(settings.log_discord_webhook_url),
            level=settings.log_discord_level,
            backtrace=False,
            diagnose=False,
            enqueue=True,
        )
        logger.info(
            "Discord log forwarding enabled at {} and above", settings.log_discord_level
        )
    else:
        logger.debug("Discord log forwarding is disabled")
    logging.basicConfig(level=logging.WARNING, force=True)


def redact_discord_webhooks(value: str) -> str:
    """Remove complete or path-only Discord webhook credentials from text."""
    return DISCORD_WEBHOOK_PATTERN.sub("<discord webhook>", value)
