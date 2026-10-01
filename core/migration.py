"""Rehearse the upload-first migration on a consistent SQLite snapshot."""

import argparse
import sqlite3
from contextlib import closing
from pathlib import Path

from core.backups import _create_backup_sync
from core.state import StateStore


def rehearse_migration(database: Path, directory: Path) -> Path:
    """Migrate a verified backup twice and leave the source database untouched."""
    snapshot = _create_backup_sync(database, directory, retention_count=0)
    for _ in range(2):
        state = StateStore(snapshot)
        state.close()
    with closing(sqlite3.connect(snapshot)) as connection:
        if connection.execute("PRAGMA integrity_check").fetchone() != ("ok",):
            raise sqlite3.DatabaseError("migrated snapshot failed integrity_check")
        if connection.execute("PRAGMA foreign_key_check").fetchall():
            raise sqlite3.DatabaseError("migrated snapshot failed foreign_key_check")
    return snapshot


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", type=Path)
    parser.add_argument("--directory", type=Path, required=True)
    arguments = parser.parse_args()
    print(rehearse_migration(arguments.database, arguments.directory))


if __name__ == "__main__":
    main()
