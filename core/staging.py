"""Shared staging filesystem and reservation rules."""

from pathlib import Path


def ensure_download_root(root: Path) -> Path:
    """Create staging before measuring the filesystem that holds downloads."""
    try:
        root.mkdir(parents=True, exist_ok=True)
        if root.is_symlink():
            raise OSError("staging root must not be a symlink")
    except OSError as error:
        raise RuntimeError(f"staging path is inaccessible: {root}: {error}") from error
    return root


def download_reservation(expected: int, reusable: int = 0) -> int:
    """Reserve remaining input writes plus an entire merge output."""
    expected = max(expected, 0)
    reusable = min(max(reusable, 0), expected)
    return max(2 * expected - reusable, expected)
