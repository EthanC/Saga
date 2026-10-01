"""Load and validate Saga's TOML and environment configuration."""

import math
import re
import tomllib
from dataclasses import dataclass, field
from datetime import UTC, datetime, tzinfo
from ipaddress import ip_address
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from cronsim import CronSim, CronSimError
from environs import Env
from tzlocal import get_localzone

INSTANCE_NAME_PATTERN = re.compile(r"[a-z0-9](?:[a-z0-9_-]{0,62}[a-z0-9])?")
CHANNEL_ID_PATTERN = re.compile(r"UC[A-Za-z0-9_-]{22}")
HANDLE_PATTERN = re.compile(r"@[A-Za-z0-9._-]{3,30}")
WEBHOOK_PATH_PATTERN = re.compile(r"/api/webhooks/\d+/[^/]+")
HTTP_HOST_PATTERN = re.compile(
    r"[A-Za-z0-9_](?:[A-Za-z0-9_-]{0,61}[A-Za-z0-9_])?"
    r"(?:\.[A-Za-z0-9_](?:[A-Za-z0-9_-]{0,61}[A-Za-z0-9_])?)*"
)
DISCORD_WEBHOOK_HOSTS = frozenset(
    {"discord.com", "www.discord.com", "canary.discord.com", "ptb.discord.com"}
)
ContentType = Literal["video", "short", "livestream"]
ALL_CONTENT_TYPES: tuple[ContentType, ...] = ("video", "short", "livestream")
LOG_LEVELS = frozenset(
    {"TRACE", "DEBUG", "INFO", "SUCCESS", "WARNING", "ERROR", "CRITICAL"}
)
_CRON_FIELD_COUNT = 5


@dataclass(frozen=True, slots=True)
class YouTubeChannelConfig:
    """Contain notification settings for one configured YouTube channel."""

    reference: str
    discord_webhook_urls: tuple[str, ...]
    videos: bool = True
    shorts: bool = True
    livestreams: bool = True

    @property
    def content_types(self) -> frozenset[ContentType]:
        """Return the enabled YouTube content types."""
        return frozenset(
            content_type
            for content_type, enabled in (
                ("video", self.videos),
                ("short", self.shorts),
                ("livestream", self.livestreams),
            )
            if enabled
        )


@dataclass(frozen=True, slots=True)
class YouTubeConfig:
    """Contain one validated YouTube monitoring configuration."""

    name: str
    channels: tuple[YouTubeChannelConfig, ...]
    cooldown: float = 60.0
    retries: int = 3
    retry_delay: float = 5.0


@dataclass(frozen=True, slots=True)
class BackupSettings:
    """Contain startup-only scheduled backup settings."""

    enabled: bool = True
    schedule: str = "0 7 * * *"
    directory: Path = Path("backups")
    retention_count: int = 7
    timezone: ZoneInfo = field(default_factory=get_localzone)


@dataclass(frozen=True, slots=True)
class LoggingSettings:
    """Contain settings needed before full startup validation."""

    log_level: str = "INFO"
    log_discord_webhook_url: str | None = None
    log_discord_level: str = "WARNING"


@dataclass(frozen=True, slots=True)
class Settings:
    """Contain process-wide paths, credentials, and runtime limits."""

    config_path: Path
    data_path: Path
    healthcheck_file: Path | None
    youtube_cookies_path: Path | None
    internet_archive_email: str | None
    internet_archive_password: str | None
    ziggy_host: str | None
    ziggy_identifier: str | None
    log_level: str
    youtube_pot_provider_url: str = "http://127.0.0.1:4416"
    backup: BackupSettings = field(default_factory=BackupSettings)
    log_discord_webhook_url: str | None = None
    log_discord_level: str = "WARNING"
    notification_timeout: float = 30.0
    archive_workers: int = 1
    upload_workers: int = 1
    download_workers: int = 1
    upload_ready_buffer: int = 2
    unknown_download_bytes: int = 10_000_000_000
    validation_enabled: bool = False
    background_max_attempts: int = 12
    background_deadline: float = 86_400.0
    transfer_stall_timeout: float = 900.0
    archive_retry_delay: float = 300.0
    archive_request_timeout: float = 60.0
    archive_poll_timeout: float = 900.0
    archive_poll_interval: float = 5.0
    minimum_free_bytes: int = 5_000_000_000
    download_bytes_per_second: int = 0
    upload_bytes_per_second: int = 0
    report_discord_webhook_url: str | None = None
    report_schedule: str = "0 0 * * *"
    report_finalization_grace: float = 300.0

    @property
    def archival_enabled(self) -> bool:
        """Return whether either preservation provider is configured."""
        return self.internet_archive_email is not None or self.ziggy_host is not None

    @property
    def media_archival_enabled(self) -> bool:
        """Return whether Internet Archive media preservation is configured."""
        return self.internet_archive_email is not None


def load_youtube_configs(path: Path) -> tuple[YouTubeConfig, ...]:
    """Load all YouTube instances from a TOML configuration file."""
    with path.open("rb") as file:
        document: Any = tomllib.load(file)

    if not isinstance(document, dict):
        raise ValueError("config.toml must contain a TOML table")

    unknown_root = set(document) - {"instances"}
    if unknown_root:
        raise ValueError(f"Unknown top-level configuration keys: {_keys(unknown_root)}")

    instances: Any = document.get("instances")
    if not isinstance(instances, dict):
        raise ValueError("config.toml must define an [instances] table")

    unknown_instances = set(instances) - {"youtube"}
    if unknown_instances:
        raise ValueError(
            f"Unknown instance configuration keys: {_keys(unknown_instances)}"
        )

    values: Any = instances.get("youtube")
    if not isinstance(values, list) or not values:
        raise ValueError(
            "config.toml must define at least one [[instances.youtube]] table"
        )

    configs = tuple(
        _parse_youtube_config(value, index) for index, value in enumerate(values)
    )
    names = [config.name.casefold() for config in configs]
    if len(names) != len(set(names)):
        raise ValueError("config.toml contains duplicate YouTube instance names")

    return configs


def load_logging_settings(project_root: Path) -> LoggingSettings:
    """Load logging settings before validating the rest of the environment."""
    env = Env()
    env.read_env(project_root / ".env", recurse=False)
    return _logging_settings(env)


def load_settings(project_root: Path) -> Settings:
    """Load process-wide settings after reading an optional project .env file."""
    env = Env()
    env.read_env(project_root / ".env", recurse=False)

    config_path = _path(env.str("SAGA_CONFIG_PATH", "config/config.toml"), project_root)
    data_path = _path(env.str("SAGA_DATA_PATH", "data"), project_root)
    health_value = env.str("SAGA_HEALTHCHECK_FILE", None)
    cookies_value = env.str("YOUTUBE_COOKIES_PATH", None)
    pot_provider_value = env.str("YOUTUBE_POT_PROVIDER_URL", "http://127.0.0.1:4416")
    archive_email = env.str("INTERNET_ARCHIVE_EMAIL", None)
    archive_password = env.str("INTERNET_ARCHIVE_PASSWORD", None)
    ziggy_host = env.str("ZIGGY_HOST", None)
    ziggy_identifier = env.str("ZIGGY_IDENTIFIER", None)
    report_discord_webhook_url = env.str("REPORT_DISCORD_WEBHOOK_URL", None)
    logging_settings = _logging_settings(env)

    try:
        backup_enabled = env.bool("SAGA_BACKUP_ENABLED", True)
        backup_retention_count = env.int("SAGA_BACKUP_RETENTION_COUNT", 7)
    except ValueError as error:
        raise ValueError(f"Invalid backup environment setting: {error}") from error
    backup_schedule = env.str("SAGA_BACKUP_SCHEDULE", "0 7 * * *")
    backup_timezone_name = env.str("SAGA_BACKUP_TIMEZONE", None) or None
    try:
        backup_timezone = (
            get_localzone()
            if backup_timezone_name is None
            else ZoneInfo(backup_timezone_name)
        )
    except (OSError, ZoneInfoNotFoundError) as error:
        raise ValueError(
            "SAGA_BACKUP_TIMEZONE must be a valid IANA timezone"
        ) from error
    _validate_cron_schedule(
        backup_schedule, "SAGA_BACKUP_SCHEDULE", datetime.now(UTC), backup_timezone
    )
    backup_directory_value = env.str("SAGA_BACKUP_DIRECTORY", "backups")
    if (
        not backup_directory_value
        or backup_directory_value != backup_directory_value.strip()
    ):
        raise ValueError("SAGA_BACKUP_DIRECTORY must be a non-empty path")
    if backup_retention_count < 0:
        raise ValueError("SAGA_BACKUP_RETENTION_COUNT must be non-negative")

    if (archive_email is None) != (archive_password is None):
        raise ValueError(
            "INTERNET_ARCHIVE_EMAIL and INTERNET_ARCHIVE_PASSWORD must be set together"
        )
    if (ziggy_host is None) != (ziggy_identifier is None):
        raise ValueError("ZIGGY_HOST and ZIGGY_IDENTIFIER must be set together")
    if ziggy_host is not None:
        ziggy_host = _ziggy_url(ziggy_host)
        if not 1 <= len(ziggy_identifier or "") <= 128:
            raise ValueError("ZIGGY_IDENTIFIER must contain 1 to 128 characters")
    if report_discord_webhook_url is not None and not _valid_webhook_url(
        report_discord_webhook_url
    ):
        raise ValueError(
            "REPORT_DISCORD_WEBHOOK_URL must be a valid Discord webhook URL"
        )
    if cookies_value is not None:
        cookies_path = _path(cookies_value, project_root)
        if not cookies_path.is_file():
            raise ValueError(f"YOUTUBE_COOKIES_PATH is not a file: {cookies_path}")
    else:
        default_cookies_path = config_path.parent / "cookies.txt"
        cookies_path = default_cookies_path if default_cookies_path.is_file() else None
    pot_provider_url = _http_origin(pot_provider_value, "YOUTUBE_POT_PROVIDER_URL")

    notification_timeout = env.float("DISCORD_TIMEOUT", 30.0)
    if not math.isfinite(notification_timeout) or notification_timeout <= 0:
        raise ValueError("DISCORD_TIMEOUT must be a positive finite number")

    archive_workers = env.int("SAGA_ARCHIVE_WORKERS", 1)
    if not 1 <= archive_workers <= 4:
        raise ValueError("SAGA_ARCHIVE_WORKERS must be between 1 and 4")
    upload_workers = _worker_count(env, "SAGA_UPLOAD_WORKERS", 1)
    download_workers = _worker_count(env, "SAGA_DOWNLOAD_WORKERS", 1)
    upload_ready_buffer = _nonnegative_env_int(env, "SAGA_UPLOAD_READY_BUFFER", 2)
    unknown_download_bytes = _nonnegative_env_int(
        env, "SAGA_UNKNOWN_DOWNLOAD_BYTES", 10_000_000_000
    )
    background_max_attempts = _nonnegative_env_int(
        env, "SAGA_BACKGROUND_MAX_ATTEMPTS", 12
    )
    if min(upload_ready_buffer, unknown_download_bytes, background_max_attempts) < 1:
        raise ValueError(
            "buffer, download reservation, and retry budget must be positive"
        )
    archive_retry_delay = _positive_env_float(env, "SAGA_ARCHIVE_RETRY_DELAY", 300.0)
    archive_request_timeout = _positive_env_float(
        env, "SAGA_ARCHIVE_REQUEST_TIMEOUT", 60.0
    )
    archive_poll_timeout = _positive_env_float(env, "SAGA_ARCHIVE_POLL_TIMEOUT", 900.0)
    archive_poll_interval = _positive_env_float(env, "SAGA_ARCHIVE_POLL_INTERVAL", 5.0)
    minimum_free_bytes = env.int("SAGA_MINIMUM_FREE_BYTES", 5_000_000_000)
    if minimum_free_bytes < 0:
        raise ValueError("SAGA_MINIMUM_FREE_BYTES must be non-negative")
    download_bytes_per_second = _nonnegative_env_int(
        env, "SAGA_DOWNLOAD_BYTES_PER_SECOND", 0
    )
    upload_bytes_per_second = _nonnegative_env_int(
        env, "SAGA_UPLOAD_BYTES_PER_SECOND", 0
    )
    report_schedule = env.str("SAGA_REPORT_SCHEDULE", "0 0 * * *")
    _validate_cron_schedule(
        report_schedule, "SAGA_REPORT_SCHEDULE", datetime.now(UTC), UTC
    )
    report_finalization_grace = _positive_env_float(
        env, "SAGA_REPORT_FINALIZATION_GRACE", 300.0
    )

    return Settings(
        config_path=config_path,
        data_path=data_path,
        healthcheck_file=(
            _path(health_value, project_root)
            if health_value is not None
            else data_path / "healthy"
        ),
        youtube_cookies_path=cookies_path,
        youtube_pot_provider_url=pot_provider_url,
        internet_archive_email=archive_email,
        internet_archive_password=archive_password,
        ziggy_host=ziggy_host,
        ziggy_identifier=ziggy_identifier,
        log_level=logging_settings.log_level,
        backup=BackupSettings(
            enabled=backup_enabled,
            schedule=backup_schedule,
            directory=_path(backup_directory_value, data_path),
            retention_count=backup_retention_count,
            timezone=backup_timezone,
        ),
        log_discord_webhook_url=logging_settings.log_discord_webhook_url,
        log_discord_level=logging_settings.log_discord_level,
        notification_timeout=notification_timeout,
        archive_workers=archive_workers,
        upload_workers=upload_workers,
        download_workers=download_workers,
        upload_ready_buffer=upload_ready_buffer,
        unknown_download_bytes=unknown_download_bytes,
        validation_enabled=env.bool("SAGA_VALIDATION_ENABLED", False),
        background_max_attempts=background_max_attempts,
        background_deadline=_positive_env_float(
            env, "SAGA_BACKGROUND_DEADLINE", 86_400
        ),
        transfer_stall_timeout=_positive_env_float(
            env, "SAGA_TRANSFER_STALL_TIMEOUT", 900
        ),
        archive_retry_delay=archive_retry_delay,
        archive_request_timeout=archive_request_timeout,
        archive_poll_timeout=archive_poll_timeout,
        archive_poll_interval=archive_poll_interval,
        minimum_free_bytes=minimum_free_bytes,
        download_bytes_per_second=download_bytes_per_second,
        upload_bytes_per_second=upload_bytes_per_second,
        report_discord_webhook_url=report_discord_webhook_url,
        report_schedule=report_schedule,
        report_finalization_grace=report_finalization_grace,
    )


def _parse_youtube_config(value: Any, index: int) -> YouTubeConfig:
    label = f"instances.youtube[{index}]"
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be a table")

    allowed = {"name", "channels", "cooldown", "retries", "retry_delay"}
    unknown = set(value) - allowed
    if unknown:
        raise ValueError(f"Unknown keys in {label}: {_keys(unknown)}")

    name = _nonempty_string(value.get("name"), f"{label}.name")
    if not INSTANCE_NAME_PATTERN.fullmatch(name):
        raise ValueError(
            f"{label}.name must contain lowercase letters, digits, hyphens, "
            "or underscores"
        )

    channel_values = value.get("channels")
    if not isinstance(channel_values, list) or not channel_values:
        raise ValueError(f"{label}.channels must be a non-empty array of tables")
    channels = tuple(
        _parse_youtube_channel_config(channel, index, label)
        for index, channel in enumerate(channel_values)
    )
    references = [channel.reference.casefold() for channel in channels]
    if len(references) != len(set(references)):
        raise ValueError(f"{label}.channels must not contain duplicate references")

    cooldown = _finite_number(value.get("cooldown", 60.0), f"{label}.cooldown")
    if cooldown <= 0:
        raise ValueError(f"{label}.cooldown must be greater than zero")

    retries = value.get("retries", 3)
    if isinstance(retries, bool) or not isinstance(retries, int) or retries < 0:
        raise ValueError(f"{label}.retries must be a non-negative integer")

    retry_delay = _finite_number(value.get("retry_delay", 5.0), f"{label}.retry_delay")
    if retry_delay < 0:
        raise ValueError(f"{label}.retry_delay must be non-negative")

    return YouTubeConfig(
        name=name,
        channels=channels,
        cooldown=cooldown,
        retries=retries,
        retry_delay=retry_delay,
    )


def _parse_youtube_channel_config(
    value: Any, index: int, parent_label: str
) -> YouTubeChannelConfig:
    label = f"{parent_label}.channels[{index}]"
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be a table")

    unknown = set(value) - {
        "reference",
        "discord_webhook_urls",
        "videos",
        "shorts",
        "livestreams",
    }
    if unknown:
        raise ValueError(f"Unknown keys in {label}: {_keys(unknown)}")

    reference = _nonempty_string(value.get("reference"), f"{label}.reference")
    if not _valid_channel_reference(reference):
        raise ValueError(f"{label}.reference is invalid: {reference!r}")

    webhooks = _string_list(
        value.get("discord_webhook_urls"), f"{label}.discord_webhook_urls"
    )
    if any(not _valid_webhook_url(url) for url in webhooks):
        raise ValueError(
            f"{label}.discord_webhook_urls must contain only Discord webhook URLs"
        )

    return YouTubeChannelConfig(
        reference=reference,
        discord_webhook_urls=webhooks,
        videos=_boolean(value.get("videos", True), f"{label}.videos"),
        shorts=_boolean(value.get("shorts", True), f"{label}.shorts"),
        livestreams=_boolean(value.get("livestreams", True), f"{label}.livestreams"),
    )


def _valid_channel_reference(value: str) -> bool:
    if CHANNEL_ID_PATTERN.fullmatch(value) or HANDLE_PATTERN.fullmatch(value):
        return True

    try:
        parsed = urlsplit(value)
    except ValueError:
        return False

    return (
        parsed.scheme == "https"
        and parsed.hostname in {"youtube.com", "www.youtube.com"}
        and not parsed.username
        and not parsed.password
        and not parsed.query
        and not parsed.fragment
        and parsed.path.startswith(("/channel/", "/@", "/c/", "/user/"))
    )


def _valid_webhook_url(value: str) -> bool:
    try:
        parsed = urlsplit(value)
    except ValueError:
        return False

    return (
        parsed.scheme == "https"
        and parsed.hostname in DISCORD_WEBHOOK_HOSTS
        and parsed.username is None
        and parsed.password is None
        and parsed.port is None
        and parsed.query == ""
        and parsed.fragment == ""
        and WEBHOOK_PATH_PATTERN.fullmatch(parsed.path) is not None
    )


def _string_list(value: Any, label: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{label} must be a non-empty array of strings")
    if any(not isinstance(item, str) or not item.strip() for item in value):
        raise ValueError(f"{label} must contain only non-empty strings")

    normalized = tuple(item.strip() for item in value)
    folded = [item.casefold() for item in normalized]
    if len(folded) != len(set(folded)):
        raise ValueError(f"{label} must not contain duplicate values")
    return normalized


def _nonempty_string(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a non-empty string")
    return value.strip()


def _finite_number(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"{label} must be a finite number")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{label} must be a finite number")
    return result


def _boolean(value: Any, label: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{label} must be a boolean")
    return value


def _path(value: str, project_root: Path) -> Path:
    path = Path(value).expanduser()
    return path if path.is_absolute() else project_root / path


def _positive_env_float(env: Env, name: str, default: float) -> float:
    value = env.float(name, default)
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be a positive finite number")
    return value


def _validate_cron_schedule(
    schedule: str, name: str, now: datetime, timezone: tzinfo
) -> None:
    if len(schedule.split()) != _CRON_FIELD_COUNT:
        raise ValueError(f"{name} must have five fields")
    try:
        next(CronSim(schedule, now.astimezone(timezone)))
    except CronSimError as error:
        raise ValueError(f"{name} is invalid: {error}") from error
    except StopIteration as error:
        raise ValueError(f"{name} has no future occurrence") from error


def _nonnegative_env_int(env: Env, name: str, default: int) -> int:
    value = env.str(name, str(default))
    if not value or re.fullmatch(r"[0-9]+", value) is None:
        raise ValueError(f"{name} must be a non-negative integer")
    return int(value)


def _worker_count(env: Env, name: str, default: int) -> int:
    value = _nonnegative_env_int(env, name, default)
    if not 1 <= value <= 4:
        raise ValueError(f"{name} must be between 1 and 4")
    return value


def _logging_settings(env: Env) -> LoggingSettings:
    webhook_url = env.str("LOG_DISCORD_WEBHOOK_URL", None)
    if webhook_url is not None and not _valid_webhook_url(webhook_url):
        raise ValueError("LOG_DISCORD_WEBHOOK_URL must be a valid Discord webhook URL")
    return LoggingSettings(
        log_level=_log_level(env, "LOG_LEVEL", "INFO"),
        log_discord_webhook_url=webhook_url,
        log_discord_level=_log_level(env, "LOG_DISCORD_LEVEL", "WARNING"),
    )


def _log_level(env: Env, name: str, default: str) -> str:
    value = env.str(name, default).strip().upper()
    if value not in LOG_LEVELS:
        raise ValueError(f"{name} must be a valid Loguru level")
    return value


def _ziggy_url(value: str) -> str:
    return _http_origin(value, "ZIGGY_HOST")


def _http_origin(value: str, name: str) -> str:
    candidate = value.strip()
    if "://" not in candidate:
        candidate = f"http://{candidate}"
    try:
        parsed = urlsplit(candidate)
        port = parsed.port
    except ValueError as error:
        raise ValueError(f"{name} must be a valid HTTP origin") from error
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or not _valid_http_host(parsed.hostname)
        or parsed.username is not None
        or parsed.password is not None
        or parsed.netloc.endswith(":")
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
        or port is not None
        and not 1 <= port <= 65535
    ):
        raise ValueError(f"{name} must be a valid HTTP origin")
    return candidate.rstrip("/")


def _valid_http_host(value: str) -> bool:
    try:
        ip_address(value)
    except ValueError:
        value = value.removesuffix(".")
        return len(value) <= 253 and HTTP_HOST_PATTERN.fullmatch(value) is not None
    return True


def _keys(values: set[str]) -> str:
    return ", ".join(sorted(repr(value) for value in values))
