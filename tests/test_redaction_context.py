import logging
from io import StringIO

import pytest

from core import logging as logging_config


def test_cookie_alert_count_survives_console_and_discord_redaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    text = "YouTube access failures persist; refresh account cookies: {'count': 201}"
    assert logging_config.redact_diagnostics(text) == text
    console = StringIO()
    discord: list[str] = []

    class Sink:
        def __init__(self, url: str) -> None:
            pass

        def emit(self, record: logging.LogRecord) -> None:
            discord.append(record.getMessage())

    monkeypatch.setattr(logging_config, "stdout", console)
    monkeypatch.setattr(logging_config, "DiscordSink", Sink)
    logging_config._safe_console(text)
    logging_config._SafeDiscordSink("webhook").emit(
        logging.LogRecord("test", logging.ERROR, "", 0, text, (), None)
    )
    assert console.getvalue() == text and discord == [text]


@pytest.mark.parametrize(
    "text",
    [
        "cookies: {'count': 201, 'SID': 'secret'}",
        "cookies={'count': 201}",
        "{'cookies': {'SID': 'secret', 'nested': [{'token': 'secret'}]}}",
        '{"cookies": ["secret", {"key": "secret"}]}',
        "Cookie: {'SID': 'secret'}",
        "token: {'key': 'secret'}",
        'password={"key":"escaped\\"secret"}',
        "cookies: {'SID': 'secret'",
        "cookies: {'SID': 'secret']",
    ],
)
def test_real_or_malformed_credential_containers_are_fully_redacted(text: str) -> None:
    safe = logging_config.redact_diagnostics(text)
    assert "secret" not in safe and "201" not in safe
    assert "<redacted" in safe


def test_prose_count_and_adjacent_credentials_do_not_interfere() -> None:
    text = "Refresh account cookies: {'count': 201}; token={'secret': 'secret'}"
    safe = logging_config.redact_diagnostics(text)
    assert "{'count': 201}" in safe and "secret" not in safe
    assert logging_config.redact_diagnostics(safe) == safe
