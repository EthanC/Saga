"""Check that channel polling has produced a recent heartbeat."""

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
        return 0 <= age <= maximum_age
    except OSError, ValueError:
        return False


if __name__ == "__main__":
    raise SystemExit(0 if is_healthy(Path(__file__).resolve().parent.parent) else 1)
