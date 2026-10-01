from pathlib import Path
from typing import Any

import pytest
from dotenv import dotenv_values

from core import config

VALID_CHANNEL = {
    "reference": "@valid",
    "discord_webhook_urls": ["https://discord.com/api/webhooks/1/token"],
}
VALID_INSTANCE = {"name": "primary", "channels": [VALID_CHANNEL]}
SETTING_NAMES = (
    "SAGA_UPLOAD_WORKERS",
    "SAGA_DOWNLOAD_WORKERS",
    "SAGA_UPLOAD_READY_BUFFER",
    "SAGA_UNKNOWN_DOWNLOAD_BYTES",
    "SAGA_VALIDATION_ENABLED",
    "SAGA_BACKGROUND_MAX_ATTEMPTS",
    "SAGA_BACKGROUND_DEADLINE",
    "SAGA_TRANSFER_STALL_TIMEOUT",
    "SAGA_CONFIG_PATH",
    "SAGA_DATA_PATH",
    "SAGA_HEALTHCHECK_FILE",
    "SAGA_BACKUP_ENABLED",
    "SAGA_BACKUP_SCHEDULE",
    "SAGA_BACKUP_DIRECTORY",
    "SAGA_BACKUP_RETENTION_COUNT",
    "SAGA_BACKUP_TIMEZONE",
    "YOUTUBE_COOKIES_PATH",
    "YOUTUBE_POT_PROVIDER_URL",
    "INTERNET_ARCHIVE_EMAIL",
    "INTERNET_ARCHIVE_PASSWORD",
    "ZIGGY_HOST",
    "ZIGGY_IDENTIFIER",
    "LOG_LEVEL",
    "LOG_DISCORD_WEBHOOK_URL",
    "LOG_DISCORD_LEVEL",
    "DISCORD_TIMEOUT",
    "SAGA_ARCHIVE_WORKERS",
    "SAGA_ARCHIVE_RETRY_DELAY",
    "SAGA_ARCHIVE_REQUEST_TIMEOUT",
    "SAGA_ARCHIVE_POLL_TIMEOUT",
    "SAGA_ARCHIVE_POLL_INTERVAL",
    "SAGA_MINIMUM_FREE_BYTES",
    "SAGA_DOWNLOAD_BYTES_PER_SECOND",
    "SAGA_UPLOAD_BYTES_PER_SECOND",
    "REPORT_DISCORD_WEBHOOK_URL",
    "SAGA_REPORT_SCHEDULE",
    "SAGA_REPORT_FINALIZATION_GRACE",
)


@pytest.fixture
def clean_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in SETTING_NAMES:
        monkeypatch.delenv(name, raising=False)


@pytest.mark.parametrize(
    "name,value",
    [
        ("SAGA_UPLOAD_WORKERS", "0"),
        ("SAGA_DOWNLOAD_WORKERS", "5"),
        ("SAGA_UPLOAD_READY_BUFFER", "0"),
        ("SAGA_UNKNOWN_DOWNLOAD_BYTES", "0"),
        ("SAGA_BACKGROUND_MAX_ATTEMPTS", "0"),
    ],
)
def test_upload_first_configuration_rejects_zero_capacity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    clean_settings: None,
    name: str,
    value: str,
) -> None:
    monkeypatch.setenv(name, value)
    with pytest.raises(ValueError):
        config.load_settings(tmp_path)


@pytest.mark.parametrize(
    ("document", "message"),
    [
        ([], "must contain a TOML table"),
        ({"extra": True}, "Unknown top-level"),
        ({}, "must define an \\[instances\\] table"),
        ({"instances": {"other": True}}, "Unknown instance"),
        ({"instances": {}}, "at least one"),
        ({"instances": {"youtube": [1]}}, "must be a table"),
        (
            {"instances": {"youtube": [VALID_INSTANCE, VALID_INSTANCE]}},
            "duplicate YouTube instance names",
        ),
    ],
)
def test_load_youtube_configs_rejects_document_shapes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, document: Any, message: str
) -> None:
    path = tmp_path / "config.toml"
    path.write_text("", encoding="utf-8")
    monkeypatch.setattr(config.tomllib, "load", lambda _: document)

    with pytest.raises(ValueError, match=message):
        config.load_youtube_configs(path)


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ({"channels": [VALID_CHANNEL]}, "name must be a non-empty string"),
        ({"name": "primary"}, "channels must be a non-empty array"),
        ({"name": "primary", "channels": [1]}, "channels\\[0\\] must be a table"),
        (
            {"name": "primary", "channels": [VALID_CHANNEL, VALID_CHANNEL]},
            "duplicate references",
        ),
        ({**VALID_INSTANCE, "cooldown": True}, "cooldown must be a finite number"),
        ({**VALID_INSTANCE, "cooldown": float("inf")}, "finite number"),
        ({**VALID_INSTANCE, "cooldown": 0}, "greater than zero"),
        ({**VALID_INSTANCE, "retries": True}, "non-negative integer"),
        ({**VALID_INSTANCE, "retries": -1}, "non-negative integer"),
        ({**VALID_INSTANCE, "retry_delay": -1}, "must be non-negative"),
    ],
)
def test_parse_youtube_config_rejects_invalid_values(value: Any, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        config._parse_youtube_config(value, 0)


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ({"reference": "https://[::1", "discord_webhook_urls": ["x"]}, "invalid"),
        ({"reference": "@valid"}, "non-empty array"),
        (
            {"reference": "@valid", "discord_webhook_urls": [" "]},
            "only non-empty strings",
        ),
        (
            {"reference": "@valid", "discord_webhook_urls": ["x", "X"]},
            "duplicate values",
        ),
        (
            {"reference": "@valid", "discord_webhook_urls": ["https://[::1"]},
            "Discord webhook URLs",
        ),
    ],
)
def test_parse_channel_rejects_invalid_values(value: Any, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        config._parse_youtube_channel_config(value, 0, "instance")


def test_channel_reference_accepts_youtube_url() -> None:
    value = {**VALID_CHANNEL, "reference": "https://youtube.com/@valid"}

    parsed = config._parse_youtube_channel_config(value, 0, "instance")

    assert parsed.reference == "https://youtube.com/@valid"


def test_load_settings_accepts_all_optional_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clean_settings: None
) -> None:
    cookies = tmp_path / "cookies.txt"
    cookies.write_text("cookies", encoding="utf-8")
    values = {
        "INTERNET_ARCHIVE_EMAIL": "user@example.com",
        "INTERNET_ARCHIVE_PASSWORD": "password",
        "SAGA_DATA_PATH": "store",
        "SAGA_HEALTHCHECK_FILE": "health",
        "YOUTUBE_COOKIES_PATH": "cookies.txt",
        "YOUTUBE_POT_PROVIDER_URL": "http://pot-provider:4416/",
        "SAGA_BACKUP_ENABLED": "false",
        "SAGA_BACKUP_SCHEDULE": "30 2 * * 1",
        "SAGA_BACKUP_DIRECTORY": "snapshots",
        "SAGA_BACKUP_RETENTION_COUNT": "0",
        "SAGA_BACKUP_TIMEZONE": "America/New_York",
        "LOG_LEVEL": "DEBUG",
        "LOG_DISCORD_WEBHOOK_URL": "https://discord.com/api/webhooks/1/log-token",
        "LOG_DISCORD_LEVEL": "ERROR",
        "DISCORD_TIMEOUT": "2.5",
        "SAGA_ARCHIVE_WORKERS": "4",
        "SAGA_ARCHIVE_RETRY_DELAY": "1",
        "SAGA_ARCHIVE_REQUEST_TIMEOUT": "2",
        "SAGA_ARCHIVE_POLL_TIMEOUT": "3",
        "SAGA_ARCHIVE_POLL_INTERVAL": "4",
        "SAGA_MINIMUM_FREE_BYTES": "0",
        "SAGA_DOWNLOAD_BYTES_PER_SECOND": "6250000",
        "SAGA_UPLOAD_BYTES_PER_SECOND": "1250000",
        "REPORT_DISCORD_WEBHOOK_URL": "https://discord.com/api/webhooks/2/report",
        "SAGA_REPORT_SCHEDULE": "0 * * * *",
        "SAGA_REPORT_FINALIZATION_GRACE": "30",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)

    result = config.load_settings(tmp_path)

    assert result.config_path == tmp_path / "config" / "config.toml"
    assert result.data_path == tmp_path / "store"
    assert result.healthcheck_file == tmp_path / "health"
    assert result.youtube_cookies_path == cookies
    assert result.youtube_pot_provider_url == "http://pot-provider:4416"
    assert result.backup.enabled is False
    assert result.backup.schedule == "30 2 * * 1"
    assert result.backup.directory == tmp_path / "store" / "snapshots"
    assert result.backup.retention_count == 0
    assert result.backup.timezone.key == "America/New_York"
    assert result.ziggy_host is None
    assert result.log_level == "DEBUG"
    assert result.log_discord_webhook_url == values["LOG_DISCORD_WEBHOOK_URL"]
    assert result.log_discord_level == "ERROR"
    assert result.notification_timeout == 2.5
    assert result.archive_workers == 4
    assert result.minimum_free_bytes == 0
    assert result.download_bytes_per_second == 6_250_000
    assert result.upload_bytes_per_second == 1_250_000
    assert result.report_discord_webhook_url == values["REPORT_DISCORD_WEBHOOK_URL"]
    assert result.report_schedule == "0 * * * *"
    assert result.report_finalization_grace == 30


def test_load_settings_discovers_default_cookies(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clean_settings: None
) -> None:
    config_directory = tmp_path / "config"
    config_directory.mkdir()
    cookies = config_directory / "cookies.txt"
    cookies.write_text("cookies", encoding="utf-8")
    monkeypatch.setenv("INTERNET_ARCHIVE_EMAIL", "user@example.com")
    monkeypatch.setenv("INTERNET_ARCHIVE_PASSWORD", "password")

    result = config.load_settings(tmp_path)

    assert result.config_path == config_directory / "config.toml"
    assert result.youtube_cookies_path == cookies


def test_example_environment_preserves_container_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clean_settings: None
) -> None:
    paths = {
        "SAGA_CONFIG_PATH": str(tmp_path / "config.toml"),
        "SAGA_DATA_PATH": str(tmp_path / "data"),
        "SAGA_HEALTHCHECK_FILE": str(tmp_path / "data" / "healthy"),
    }
    for name, value in paths.items():
        monkeypatch.setenv(name, value)
    example = Path(__file__).resolve().parents[1] / ".env.example"
    for name, value in dotenv_values(example).items():
        assert value is not None
        monkeypatch.setenv(name, value)

    result = config.load_settings(tmp_path)

    assert result.config_path == Path(paths["SAGA_CONFIG_PATH"])
    assert result.data_path == Path(paths["SAGA_DATA_PATH"])
    assert result.healthcheck_file == Path(paths["SAGA_HEALTHCHECK_FILE"])


def test_backup_schedule_requires_a_future_occurrence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clean_settings: None
) -> None:
    monkeypatch.setenv("INTERNET_ARCHIVE_EMAIL", "user@example.com")
    monkeypatch.setenv("INTERNET_ARCHIVE_PASSWORD", "password")
    monkeypatch.setattr(config, "CronSim", lambda *_: iter(()))

    with pytest.raises(ValueError, match="no future occurrence"):
        config.load_settings(tmp_path)


@pytest.mark.parametrize(
    ("values", "message"),
    [
        ({"INTERNET_ARCHIVE_EMAIL": "user@example.com"}, "must be set together"),
        ({"ZIGGY_HOST": "localhost"}, "ZIGGY_HOST and ZIGGY_IDENTIFIER"),
        ({"ZIGGY_HOST": "localhost", "ZIGGY_IDENTIFIER": "x" * 129}, "1 to 128"),
        ({"YOUTUBE_COOKIES_PATH": "missing"}, "is not a file"),
        ({"YOUTUBE_POT_PROVIDER_URL": "ftp://localhost"}, "valid HTTP origin"),
        ({"YOUTUBE_POT_PROVIDER_URL": "http://local host:4416"}, "valid HTTP origin"),
        ({"YOUTUBE_POT_PROVIDER_URL": "http://localhost:"}, "valid HTTP origin"),
        ({"DISCORD_TIMEOUT": "0"}, "positive finite number"),
        ({"DISCORD_TIMEOUT": "nan"}, "invalid"),
        ({"SAGA_ARCHIVE_WORKERS": "0"}, "between 1 and 4"),
        ({"SAGA_BACKUP_ENABLED": "maybe"}, "Invalid backup environment setting"),
        ({"SAGA_BACKUP_RETENTION_COUNT": "nope"}, "Invalid backup environment setting"),
        ({"SAGA_BACKUP_SCHEDULE": "0 7 * *"}, "must have five fields"),
        ({"SAGA_BACKUP_SCHEDULE": "0 99 * * *"}, "is invalid"),
        ({"SAGA_BACKUP_TIMEZONE": "Not/A_Zone"}, "valid IANA timezone"),
        ({"SAGA_BACKUP_DIRECTORY": " "}, "non-empty path"),
        ({"SAGA_BACKUP_RETENTION_COUNT": "-1"}, "must be non-negative"),
        ({"LOG_DISCORD_WEBHOOK_URL": "https://example.com/hook"}, "valid Discord"),
        ({"LOG_LEVEL": "verbose"}, "valid Loguru level"),
        ({"LOG_DISCORD_LEVEL": "alert"}, "valid Loguru level"),
        ({"SAGA_MINIMUM_FREE_BYTES": "-1"}, "must be non-negative"),
        ({"SAGA_DOWNLOAD_BYTES_PER_SECOND": "-1"}, "SAGA_DOWNLOAD_BYTES_PER_SECOND"),
        ({"SAGA_UPLOAD_BYTES_PER_SECOND": "1.5"}, "SAGA_UPLOAD_BYTES_PER_SECOND"),
        ({"SAGA_UPLOAD_BYTES_PER_SECOND": ""}, "SAGA_UPLOAD_BYTES_PER_SECOND"),
        ({"SAGA_UPLOAD_BYTES_PER_SECOND": "fast"}, "SAGA_UPLOAD_BYTES_PER_SECOND"),
        ({"SAGA_ARCHIVE_RETRY_DELAY": "0"}, "positive finite number"),
        ({"REPORT_DISCORD_WEBHOOK_URL": "https://example.com/hook"}, "valid Discord"),
        ({"SAGA_REPORT_SCHEDULE": "0 * * *"}, "must have five fields"),
        ({"SAGA_REPORT_SCHEDULE": "0 99 * * *"}, "is invalid"),
        ({"SAGA_REPORT_FINALIZATION_GRACE": "inf"}, "invalid"),
        (
            {"ZIGGY_HOST": "http://localhost:notaport", "ZIGGY_IDENTIFIER": "x"},
            "valid HTTP origin",
        ),
        (
            {"ZIGGY_HOST": "ftp://localhost", "ZIGGY_IDENTIFIER": "x"},
            "valid HTTP origin",
        ),
    ],
)
def test_load_settings_rejects_invalid_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    clean_settings: None,
    values: dict[str, str],
    message: str,
) -> None:
    monkeypatch.setenv("INTERNET_ARCHIVE_EMAIL", "user@example.com")
    monkeypatch.setenv("INTERNET_ARCHIVE_PASSWORD", "password")
    for name, value in values.items():
        monkeypatch.setenv(name, value)
    if values == {"INTERNET_ARCHIVE_EMAIL": "user@example.com"}:
        monkeypatch.delenv("INTERNET_ARCHIVE_PASSWORD")

    with pytest.raises(ValueError, match=message):
        config.load_settings(tmp_path)


@pytest.mark.parametrize(
    "value",
    [
        "http://user@localhost",
        "http://user:password@localhost",
        "http://localhost/path",
        "http://localhost?query",
        "http://localhost#fragment",
    ],
)
def test_ziggy_url_rejects_non_origin_components(value: str) -> None:
    with pytest.raises(ValueError, match="valid HTTP origin"):
        config._ziggy_url(value)


def test_bandwidth_defaults_and_process_environment_precedence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clean_settings: None
) -> None:
    (tmp_path / ".env").write_text(
        "INTERNET_ARCHIVE_EMAIL=file@example.com\n"
        "INTERNET_ARCHIVE_PASSWORD=file-password\n"
        "SAGA_DOWNLOAD_BYTES_PER_SECOND=10\n"
        "SAGA_UPLOAD_BYTES_PER_SECOND=20\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("INTERNET_ARCHIVE_EMAIL", "process@example.com")
    monkeypatch.setenv("SAGA_DOWNLOAD_BYTES_PER_SECOND", "30")

    result = config.load_settings(tmp_path)

    assert result.internet_archive_email == "process@example.com"
    assert result.youtube_pot_provider_url == "http://127.0.0.1:4416"
    assert result.download_bytes_per_second == 30
    assert result.upload_bytes_per_second == 20

    (tmp_path / ".env").unlink()
    monkeypatch.setenv("INTERNET_ARCHIVE_PASSWORD", "password")
    result = config.load_settings(tmp_path)
    assert result.download_bytes_per_second == 30
    assert result.upload_bytes_per_second == 0
