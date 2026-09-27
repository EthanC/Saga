import asyncio
import logging
from io import StringIO
from pathlib import Path
from typing import Any

import pytest

import saga
from core import logging as logging_config
from core.config import LoggingSettings, Settings, load_logging_settings


def settings(webhook_url: str | None = None) -> Settings:
    return Settings(
        config_path=Path("config.toml"),
        data_path=Path("data"),
        healthcheck_file=None,
        youtube_cookies_path=None,
        internet_archive_email="user@example.com",
        internet_archive_password="password",
        ziggy_host=None,
        ziggy_identifier=None,
        log_level="INFO",
        log_discord_webhook_url=webhook_url,
    )


class FakeLogger:
    def __init__(self) -> None:
        self.sinks: list[tuple[Any, dict[str, Any]]] = []
        self.messages: list[tuple[str, tuple[Any, ...]]] = []

    def remove(self) -> None:
        self.sinks.clear()

    def add(self, sink: Any, **options: Any) -> None:
        self.sinks.append((sink, options))

    def level(self, name: str) -> None:
        pass

    def info(self, message: str, *values: Any) -> None:
        self.messages.append((message, values))

    def debug(self, message: str, *values: Any) -> None:
        self.messages.append((message, values))


def test_configure_logging_adds_console_only_without_webhook(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = FakeLogger()
    basic_calls: list[dict[str, Any]] = []
    monkeypatch.setattr(logging_config, "logger", fake)
    monkeypatch.setattr(
        logging_config.logging,
        "basicConfig",
        lambda **options: basic_calls.append(options),
    )

    logging_config.configure_logging(settings())

    assert len(fake.sinks) == 1
    assert fake.sinks[0][1]["level"] == "INFO"
    assert basic_calls == [{"level": logging.WARNING, "force": True}]


def test_configure_logging_adds_warning_discord_sink(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = FakeLogger()
    sink_calls: list[tuple[str, bool]] = []

    class FakeSink:
        def __init__(self, url: str) -> None:
            sink_calls.append((url, False))

        def emit(self, record: logging.LogRecord) -> None:
            pass

    monkeypatch.setattr(logging_config, "logger", fake)
    monkeypatch.setattr(logging_config, "DiscordSink", FakeSink)

    url = "https://discord.com/api/webhooks/1/log-token"
    logging_config.configure_logging(settings(url))

    assert sink_calls == [(url, False)]
    assert len(fake.sinks) == 2
    assert fake.sinks[1][1]["level"] == "WARNING"


def test_load_logging_settings_ignores_unrelated_invalid_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    webhook_url = "https://discord.com/api/webhooks/1/log-token"
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("LOG_DISCORD_WEBHOOK_URL", webhook_url)
    monkeypatch.setenv("LOG_DISCORD_LEVEL", "ERROR")
    monkeypatch.setenv("SAGA_BACKUP_ENABLED", "invalid")

    result = load_logging_settings(tmp_path)

    assert result == LoggingSettings("DEBUG", webhook_url, "ERROR")


def test_start_configures_logging_before_full_settings_validation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logging_settings = LoggingSettings()
    calls: list[str] = []

    monkeypatch.setattr(saga, "load_logging_settings", lambda _: logging_settings)

    def configure(settings: Settings | LoggingSettings) -> None:
        assert settings is logging_settings
        calls.append("configure")

    def fail_settings(_: Path) -> None:
        calls.append("settings")
        raise ValueError("invalid environment")

    monkeypatch.setattr(saga, "configure_logging", configure)
    monkeypatch.setattr(saga, "load_settings", fail_settings)

    with pytest.raises(ValueError, match="invalid environment"):
        asyncio.run(saga.start())

    assert calls == ["configure", "settings"]


def test_redact_discord_webhooks_handles_full_and_path_only_urls() -> None:
    token = "secret.token-value"
    text = f"https://discord.com/api/webhooks/123/{token} and /api/webhooks/456/{token}"

    redacted = logging_config.redact_discord_webhooks(text)

    assert token not in redacted
    assert redacted == "<discord webhook> and <discord webhook>"


def test_safe_discord_sink_redacts_and_truncates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    messages: list[str] = []

    class FakeSink:
        def __init__(self, url: str) -> None:
            pass

        def emit(self, record: logging.LogRecord) -> None:
            messages.append(record.getMessage())

    monkeypatch.setattr(logging_config, "DiscordSink", FakeSink)
    sink = logging_config._SafeDiscordSink("https://discord.com/api/webhooks/1/sink")
    record = logging.LogRecord(
        "test",
        logging.ERROR,
        __file__,
        1,
        "failure at /api/webhooks/2/secret-token " + "x" * 2000,
        (),
        None,
    )

    sink.emit(record)

    assert "secret-token" not in messages[0]
    assert len(messages[0]) == logging_config.DISCORD_MESSAGE_LIMIT


def test_safe_discord_sink_hides_transport_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class BrokenSink:
        def __init__(self, url: str) -> None:
            pass

        def emit(self, record: logging.LogRecord) -> None:
            raise RuntimeError("/api/webhooks/1/secret-token")

    output = StringIO()
    monkeypatch.setattr(logging_config, "DiscordSink", BrokenSink)
    monkeypatch.setattr(logging_config, "stderr", output)
    sink = logging_config._SafeDiscordSink("https://discord.com/api/webhooks/1/sink")

    sink.emit(
        logging.LogRecord("test", logging.ERROR, __file__, 1, "failure", (), None)
    )

    assert output.getvalue() == (
        "Discord log forwarding failed (RuntimeError); see console logs\n"
    )
