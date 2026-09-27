from pathlib import Path

import pytest

from core.config import load_settings, load_youtube_configs


def write_config(path: Path, body: str) -> Path:
    path.write_text(body, encoding="utf-8")
    return path


def test_load_youtube_configs(tmp_path: Path) -> None:
    path = write_config(
        tmp_path / "config.toml",
        """
[instances]
[[instances.youtube]]
name = "primary"
cooldown = 90
retries = 4
retry_delay = 10

[[instances.youtube.channels]]
reference = "@ExampleHandle"
discord_webhook_urls = ["https://discord.com/api/webhooks/123/token"]

[[instances.youtube.channels]]
reference = "UCaaaaaaaaaaaaaaaaaaaaaa"
discord_webhook_urls = ["https://discord.com/api/webhooks/456/other-token"]
videos = false
shorts = true
livestreams = false
""",
    )

    configs = load_youtube_configs(path)

    assert len(configs) == 1
    assert configs[0].name == "primary"
    assert configs[0].cooldown == 90.0
    assert configs[0].retries == 4
    assert configs[0].retry_delay == 10.0
    assert [channel.reference for channel in configs[0].channels] == [
        "@ExampleHandle",
        "UCaaaaaaaaaaaaaaaaaaaaaa",
    ]
    assert configs[0].channels[0].discord_webhook_urls == (
        "https://discord.com/api/webhooks/123/token",
    )
    assert configs[0].channels[1].discord_webhook_urls == (
        "https://discord.com/api/webhooks/456/other-token",
    )
    assert configs[0].channels[0].videos is True
    assert configs[0].channels[0].shorts is True
    assert configs[0].channels[0].livestreams is True
    assert configs[0].channels[1].content_types == {"short"}


@pytest.mark.parametrize(
    ("body", "message"),
    [
        (
            "[instances]\n[[instances.youtube]]\nname='x'\nchannels=[]\nextra=true",
            "Unknown keys",
        ),
        (
            "[instances]\n[[instances.youtube]]\nname='x'\n"
            "[[instances.youtube.channels]]\nreference='@valid'\n"
            "discord_webhook_urls=['https://example.com/api/webhooks/1/t']",
            "Discord webhook URLs",
        ),
        (
            "[instances]\n[[instances.youtube]]\nname='UPPER'\n"
            "[[instances.youtube.channels]]\nreference='@valid'\n"
            "discord_webhook_urls=['https://discord.com/api/webhooks/1/t']",
            "lowercase letters",
        ),
        (
            "[instances]\n[[instances.youtube]]\nname='x'\n"
            "[[instances.youtube.channels]]\nreference='@valid'\n"
            "discord_webhook_urls=['https://discord.com/api/webhooks/1/t']\nextra=true",
            "Unknown keys in instances.youtube\\[0\\].channels\\[0\\]",
        ),
        (
            "[instances]\n[[instances.youtube]]\nname='x'\nchannels=['@valid']\n"
            "discord_webhook_urls=['https://discord.com/api/webhooks/1/t']",
            "Unknown keys",
        ),
        (
            "[instances]\n[[instances.youtube]]\nname='x'\n"
            "[[instances.youtube.channels]]\nreference='@valid'\n"
            "discord_webhook_urls=['https://discord.com/api/webhooks/1/t']\n"
            "shorts='yes'",
            "shorts must be a boolean",
        ),
    ],
)
def test_reject_invalid_config(tmp_path: Path, body: str, message: str) -> None:
    path = write_config(tmp_path / "config.toml", body)

    with pytest.raises(ValueError, match=message):
        load_youtube_configs(path)


def test_settings_require_archive_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("INTERNET_ARCHIVE_EMAIL", raising=False)
    monkeypatch.delenv("INTERNET_ARCHIVE_PASSWORD", raising=False)

    with pytest.raises(ValueError, match="are required"):
        load_settings(tmp_path)


def test_settings_normalize_ziggy_origin(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("INTERNET_ARCHIVE_EMAIL", "user@example.com")
    monkeypatch.setenv("INTERNET_ARCHIVE_PASSWORD", "password")
    monkeypatch.setenv("ZIGGY_HOST", "ziggy:9449")
    monkeypatch.setenv("ZIGGY_IDENTIFIER", "saga")

    result = load_settings(tmp_path)

    assert result.ziggy_host == "http://ziggy:9449"
