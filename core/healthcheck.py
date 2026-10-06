"""Read the service's health verdict without loading application dependencies."""

import json
import os
import time
from datetime import datetime
from pathlib import Path

MAXIMUM_AGE = 120


def heartbeat_path(project_root: Path) -> Path:
    """Honor process overrides and native installations' dotenv paths."""
    value = os.environ.get("SAGA_HEALTHCHECK_FILE")
    if value is None:
        from dotenv import dotenv_values

        values = {**dotenv_values(project_root / ".env"), **os.environ}
        value = values.get("SAGA_HEALTHCHECK_FILE")
        if value is None:
            value = str(Path(values.get("SAGA_DATA_PATH") or "data") / "healthy")
    return project_root / value


def is_healthy(project_root: Path) -> bool:
    """Reject missing, malformed, stale, or unhealthy service snapshots."""
    try:
        path = heartbeat_path(project_root)
        document = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(document, dict) or document.get("version") != 1:
            return False
        stamp = datetime.fromisoformat(document["generated_at"])
        now = time.time()
        return (
            document.get("healthy") is True
            and stamp.tzinfo is not None
            and 0 <= now - stamp.timestamp() <= MAXIMUM_AGE
            and 0 <= now - path.stat().st_mtime <= MAXIMUM_AGE
        )
    except OSError, ValueError, KeyError, TypeError:
        return False


if __name__ == "__main__":
    raise SystemExit(0 if is_healthy(Path(__file__).resolve().parent.parent) else 1)
