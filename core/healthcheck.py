"""Check that channel polling has produced a recent heartbeat."""

import json
import time
from pathlib import Path

from core.config import load_settings, load_youtube_configs


def is_healthy(project_root: Path) -> bool:
    """Allow three polling intervals, with at least five minutes for slow scans."""
    try:
        settings = load_settings(project_root)
        path = settings.healthcheck_file
        if path is None or not path.is_file():
            return False
        configurations = load_youtube_configs(settings.config_path)
        maximum_age = max(300.0, 3 * max(item.cooldown for item in configurations))
        age = time.time() - path.stat().st_mtime
        if not 0 <= age <= maximum_age:
            return False
        transfer_path = settings.data_path / "transfers.json"
        if settings.archival_enabled:
            if (
                not transfer_path.is_file()
                or time.time() - transfer_path.stat().st_mtime > 120
            ):
                return False
            document = json.loads(transfer_path.read_text(encoding="utf-8"))
            return isinstance(document, dict) and document.get("healthy") is True
        return True
    except OSError, ValueError:
        return False


if __name__ == "__main__":
    raise SystemExit(0 if is_healthy(Path(__file__).resolve().parent.parent) else 1)
