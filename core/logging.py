"""Configure Saga's Loguru sinks."""

import copy
import logging
import re
import time
from collections import OrderedDict
from logging import LogRecord
from sys import stderr, stdout
from threading import Lock
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from loguru import logger
from loguru_discord import DiscordSink

from core.config import LoggingSettings, Settings
from core.shutdown import StopController

DISCORD_WEBHOOK_PATTERN = re.compile(
    r"(?:https://(?:(?:www|canary|ptb)\.)?discord\.com)?"
    r"/api(?:/v\d+)?/webhooks/\d+/[A-Za-z0-9._-]+(?:[?#][^\s\"'<>]*)?",
    re.IGNORECASE,
)
DISCORD_MESSAGE_LIMIT = 1800
URL_PATTERN = re.compile(r"https?://[^\s\"'<>]+", re.IGNORECASE)
HEADER_PATTERN = re.compile(
    r"\b(?:authorization|proxy-authorization|cookie|set-cookie)\s*:\s*[^\r\n]+",
    re.IGNORECASE,
)
SECRET_NAMES = (
    r"po[ _-]?token|integrity[_-]?token|websafeFallbackToken|"
    r"access[_-]?token|refresh[_-]?token|token|password|s3accesskey|s3secretkey|"
    r"authorization|proxy[_-]?authorization|cookies?|set[_-]?cookie"
)
SECRET_PREFIX = rf"[\"']?\b(?:{SECRET_NAMES})[\"']?\s*[:=]\s*"
TOKEN_PATTERN = re.compile(
    rf"({SECRET_PREFIX})(?:\"[^\"]*\"|'[^']*'|[^\s,;\r\n{{}}\[\]]+)", re.IGNORECASE
)
STRUCTURED_TOKEN_PATTERN = re.compile(rf"({SECRET_PREFIX})(?=[{{\[])", re.IGNORECASE)
_DIAGNOSTICS: OrderedDict[str, float] = OrderedDict()
_DIAGNOSTICS_LOCK = Lock()
_STOP_CONTROLLER: StopController | None = None


class _SafeDiscordSink(logging.Handler):
    """Redact and bound records while isolating webhook transport failures."""

    def __init__(self, webhook_url: str) -> None:
        super().__init__()
        self.sink = DiscordSink(webhook_url)

    def emit(self, record: LogRecord) -> None:
        safe_record = copy.copy(record)
        safe_record.msg = redact_diagnostics(record.getMessage())[
            :DISCORD_MESSAGE_LIMIT
        ]
        safe_record.args = ()
        try:
            if _STOP_CONTROLLER is None:
                self.sink.emit(safe_record)
            else:
                from core.workers import run_managed

                run_managed(
                    "log",
                    "emit",
                    {
                        "webhook_url": self.sink.webhook_url,
                        "message": safe_record.getMessage(),
                    },
                    (),
                    {},
                    _STOP_CONTROLLER,
                )
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
    logger.add(_safe_console, level=settings.log_level, backtrace=False, diagnose=False)

    if settings.log_discord_webhook_url is not None:
        logger.add(
            _SafeDiscordSink(settings.log_discord_webhook_url),
            level=settings.log_discord_level,
            backtrace=False,
            diagnose=False,
            enqueue=True,
            filter=lambda record: not record["extra"].get("downloader_diagnostic"),
        )
        logger.info(
            "Discord log forwarding enabled at {} and above", settings.log_discord_level
        )
    else:
        logger.debug("Discord log forwarding is disabled")
    logging.basicConfig(level=logging.WARNING, force=True)


def bind_stop_controller(controller: StopController) -> None:
    """Bound queued log forwarding with the same owned-process stop budget."""
    global _STOP_CONTROLLER
    _STOP_CONTROLLER = controller


def redact_discord_webhooks(value: str) -> str:
    """Remove complete or path-only Discord webhook credentials from text."""
    return DISCORD_WEBHOOK_PATTERN.sub("<discord webhook>", value)


def redact_diagnostics(value: str) -> str:
    """Remove credentials, signed query values, cookies, and generated tokens."""

    def safe_url(match: re.Match[str]) -> str:
        try:
            parsed = urlsplit(match[0])
            authority = parsed.netloc.rsplit("@", 1)[-1]
            query = urlencode(
                [
                    (key, "<redacted>")
                    for key, _ in parse_qsl(parsed.query, keep_blank_values=True)
                ]
            )
            return urlunsplit((parsed.scheme, authority, parsed.path, query, ""))
        except ValueError:
            return "<redacted url>"

    value = redact_discord_webhooks(value)
    value = URL_PATTERN.sub(safe_url, value)
    value = _redact_structured_tokens(value)
    value = HEADER_PATTERN.sub("<redacted header>", value)
    return TOKEN_PATTERN.sub(r"\1<redacted>", value)


def _redact_structured_tokens(value: str) -> str:
    """Redact complete credential containers without eating prose alert counts."""
    pieces: list[str] = []
    cursor = copied = 0
    while match := STRUCTURED_TOKEN_PATTERN.search(value, cursor):
        start = match.end()
        end = _container_end(value, start)
        prefix = value[: match.start()].rstrip()
        # An unquoted prose label with a numeric diagnostic count is not a
        # credential field. Quoted keys, assignments, and headers remain private.
        count_in_prose = (
            match[1].strip().casefold() == "cookies:"
            and bool(prefix)
            and prefix[-1].isalnum()
            and re.fullmatch(r"\{\s*[\"']count[\"']\s*:\s*\d+\s*\}", value[start:end])
        )
        if not count_in_prose:
            pieces.extend((value[copied:start], "<redacted>"))
            copied = end
        cursor = end
    return "".join(pieces) + value[copied:]


def _container_end(value: str, start: int) -> int:
    stack: list[str] = []
    quote: str | None = None
    escaped = False
    for position in range(start, len(value)):
        character = value[position]
        if quote:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == quote:
                quote = None
        elif character in "\"'":
            quote = character
        elif character in "{[":
            stack.append("}" if character == "{" else "]")
        elif character in "}]":
            if character != stack.pop():
                return len(value)
            if not stack:
                return position + 1
    # An incomplete secret container must not leave its trailing values exposed.
    return len(value)


def _safe_console(message: Any) -> None:
    stdout.write(redact_diagnostics(str(message)))


class DownloaderLogger:
    """Keep extractor diagnostics readable without duplicate Discord failures."""

    def __init__(self) -> None:
        self.seen: set[str] = set()

    def debug(self, message: str) -> None:
        logger.bind(downloader_diagnostic=True).debug(
            "yt-dlp: {}", redact_diagnostics(message)
        )

    def warning(self, message: str) -> None:
        safe = redact_diagnostics(message)
        if safe in self.seen:
            return
        if len(self.seen) < 128:
            self.seen.add(safe)
        now = time.monotonic()
        with _DIAGNOSTICS_LOCK:
            previous = _DIAGNOSTICS.get(safe)
            if previous is not None and now - previous < 60:
                return
            _DIAGNOSTICS[safe] = now
            _DIAGNOSTICS.move_to_end(safe)
            while len(_DIAGNOSTICS) > 256:
                _DIAGNOSTICS.popitem(last=False)
        logger.bind(downloader_diagnostic=True).warning("yt-dlp: {}", safe)

    def error(self, message: str) -> None:
        # The owning processor emits the one actionable warning with retry context.
        self.debug(message)
