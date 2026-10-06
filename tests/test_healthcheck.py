import json
import os
import runpy
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import pytest

from core import healthcheck


@pytest.mark.parametrize(
    ("age", "expected"),
    [(None, False), (-1, False), (0, True), (120, True), (121, False)],
)
def test_healthcheck_requires_a_fresh_verdict(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, age: float | None, expected: bool
) -> None:
    now = 1_800_000_000.0
    path = tmp_path / "custom-heartbeat"
    if age is not None:
        path.write_text(
            json.dumps(
                {
                    "version": 1,
                    "healthy": True,
                    "generated_at": datetime.fromtimestamp(now - age, UTC).isoformat(),
                }
            )
        )
        os.utime(path, (now, now))
    monkeypatch.setattr(healthcheck.time, "time", lambda: now)
    monkeypatch.setenv("SAGA_HEALTHCHECK_FILE", str(path))
    assert healthcheck.is_healthy(tmp_path) is expected


@pytest.mark.parametrize("payload", ["", "bad", "[]", "{}", '{"version":2}'])
def test_healthcheck_rejects_malformed_and_legacy_heartbeats(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, payload: str
) -> None:
    path = tmp_path / "healthy"
    path.write_text(payload)
    monkeypatch.setenv("SAGA_HEALTHCHECK_FILE", str(path))
    assert not healthcheck.is_healthy(tmp_path)


@pytest.mark.parametrize("healthy", [True, False])
def test_probe_entrypoint_returns_the_service_verdict(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, healthy: bool
) -> None:
    path = tmp_path / "healthy"
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "healthy": healthy,
                "generated_at": datetime.now(UTC).isoformat(),
            }
        )
    )
    monkeypatch.setenv("SAGA_HEALTHCHECK_FILE", str(path))
    with pytest.raises(SystemExit) as caught:
        runpy.run_path(healthcheck.__file__, run_name="__main__")
    assert caught.value.code == (0 if healthy else 1)


def test_probe_honors_native_dotenv_and_process_path_overrides(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("SAGA_HEALTHCHECK_FILE", raising=False)
    monkeypatch.delenv("SAGA_DATA_PATH", raising=False)
    assert healthcheck.heartbeat_path(tmp_path) == tmp_path / "data" / "healthy"
    dotenv = tmp_path / ".env"
    dotenv.write_text("SAGA_DATA_PATH=storage\n")
    assert healthcheck.heartbeat_path(tmp_path) == tmp_path / "storage" / "healthy"
    monkeypatch.setenv("SAGA_DATA_PATH", "override")
    assert healthcheck.heartbeat_path(tmp_path) == tmp_path / "override" / "healthy"
    dotenv.write_text("SAGA_HEALTHCHECK_FILE='health snapshot'\n")
    assert healthcheck.heartbeat_path(tmp_path) == tmp_path / "health snapshot"
    monkeypatch.setenv("SAGA_HEALTHCHECK_FILE", str(tmp_path / "process"))
    assert healthcheck.heartbeat_path(tmp_path) == tmp_path / "process"


@pytest.mark.parametrize("age", [0, 3600])
def test_container_probe_needs_only_the_standard_library(
    tmp_path: Path, age: int
) -> None:
    heartbeat = tmp_path / "custom-heartbeat"
    heartbeat.write_text(
        json.dumps(
            {
                "version": 1,
                "healthy": True,
                "generated_at": datetime.fromtimestamp(
                    time.time() - age, UTC
                ).isoformat(),
            }
        )
    )
    result = subprocess.run(
        [sys.executable, "-S", "-B", "-m", "core.healthcheck"],
        cwd=Path(__file__).resolve().parents[1],
        env={**os.environ, "SAGA_HEALTHCHECK_FILE": str(heartbeat)},
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    assert result.returncode == (0 if age == 0 else 1), result.stderr
