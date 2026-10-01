import os
import runpy
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from core import healthcheck
from core.config import YouTubeConfig


@pytest.mark.parametrize(
    ("cooldown", "age", "expected"),
    [
        (60, None, False),
        (60, -1, False),
        (60, 0, True),
        (60, 300, True),
        (60, 301, False),
        (600, 1500, True),
        (600, 1800, True),
        (600, 1801, False),
    ],
)
def test_healthcheck_respects_heartbeat_age_and_polling_schedule(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    cooldown: float,
    age: float | None,
    expected: bool,
) -> None:
    now = 1_800_000_000.0
    path = tmp_path / "custom-heartbeat"
    if age is not None:
        path.touch()
        os.utime(path, (now - age, now - age))
    monkeypatch.setattr(healthcheck.time, "time", lambda: now)
    monkeypatch.setattr(
        healthcheck,
        "load_settings",
        lambda _: SimpleNamespace(
            healthcheck_file=path,
            config_path=tmp_path,
            data_path=tmp_path,
            archival_enabled=False,
        ),
    )
    monkeypatch.setattr(
        healthcheck,
        "load_youtube_configs",
        lambda _: (
            YouTubeConfig("fast", (), cooldown=1),
            YouTubeConfig("slow", (), cooldown=cooldown),
        ),
    )

    assert healthcheck.is_healthy(tmp_path) is expected


@pytest.mark.parametrize("error", [ValueError("invalid config"), OSError("unreadable")])
def test_healthcheck_rejects_unreadable_or_invalid_settings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, error: Exception
) -> None:
    def load(_: Path) -> None:
        raise error

    monkeypatch.setattr(healthcheck, "load_settings", load)

    assert not healthcheck.is_healthy(tmp_path)


def test_healthcheck_without_a_heartbeat_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        healthcheck, "load_settings", lambda _: SimpleNamespace(healthcheck_file=None)
    )

    assert not healthcheck.is_healthy(tmp_path)


def test_probe_entrypoint_fails_without_a_heartbeat(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "core.config.load_settings",
        lambda _: SimpleNamespace(healthcheck_file=tmp_path / "missing"),
    )

    with pytest.raises(SystemExit) as caught:
        runpy.run_path(healthcheck.__file__, run_name="__main__")

    assert caught.value.code == 1


@pytest.mark.parametrize("age", [0, 3600])
def test_container_probe_exit_status(tmp_path: Path, age: int) -> None:
    config = tmp_path / "config.toml"
    config.write_text(
        '[[instances.youtube]]\nname = "primary"\n'
        '[[instances.youtube.channels]]\nreference = "@channel"\n'
        'discord_webhook_urls = ["https://discord.com/api/webhooks/1/token"]\n',
        encoding="utf-8",
    )
    heartbeat = tmp_path / "custom-heartbeat"
    heartbeat.touch()
    (tmp_path / "transfers.json").write_text('{"healthy": true}', encoding="utf-8")
    timestamp = time.time() - age
    os.utime(heartbeat, (timestamp, timestamp))
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("SAGA_", "INTERNET_ARCHIVE_", "ZIGGY_"))
        and key
        not in {"YOUTUBE_COOKIES_PATH", "DISCORD_TIMEOUT", "REPORT_DISCORD_WEBHOOK_URL"}
    }
    environment.update(
        SAGA_CONFIG_PATH=str(config),
        SAGA_DATA_PATH=str(tmp_path),
        SAGA_HEALTHCHECK_FILE=str(heartbeat),
        INTERNET_ARCHIVE_EMAIL="user@example.com",
        INTERNET_ARCHIVE_PASSWORD="password",
    )

    result = subprocess.run(
        [sys.executable, "-B", "-m", "core.healthcheck"],
        cwd=Path(__file__).resolve().parents[1],
        env=environment,
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )

    assert result.returncode == (0 if age == 0 else 1), result.stderr
