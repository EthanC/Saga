"""Shared staging filesystem and reservation rules."""

import json
import re
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from core.logging import redact_diagnostics

INPUT_PATTERN = re.compile(
    r"(?:media|merging)\.f([\w-]+)\.([a-z0-9]+)(?:\.part(?:-Frag\d+)?)?(?:\.ytdl)?$"
)


@dataclass(frozen=True)
class PartialInventory:
    """Describe recognized inputs without treating output media as reusable."""

    reusable_bytes: int = 0
    partial_bytes: int = 0
    files: tuple[Path, ...] = ()
    newest_write: float = 0
    finding: str | None = None


class StagingIdentityError(RuntimeError):
    """Keep conflicting or unowned inputs intact for inspection."""


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


def _document(path: Path) -> dict[str, Any]:
    if path.is_symlink():
        raise ValueError("symlink manifest")
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("manifest is not an object")
    return value


def _valid_sidecar(path: Path) -> bool:
    downloader = _document(path).get("downloader")
    fragment = (
        downloader.get("current_fragment") if isinstance(downloader, dict) else None
    )
    index = fragment.get("index") if isinstance(fragment, dict) else None
    return isinstance(index, int) and not isinstance(index, bool) and index >= 0


def completed_download_size(
    directory: Path, operation_id: str, video_id: str
) -> int | None:
    """Recognize an atomic native receipt without granting unreceipted MKV credit."""
    if directory.name != operation_id or directory.is_symlink():
        return None
    media = directory / "media.mkv"
    try:
        if media.is_symlink() or not media.is_file():
            return None
        size = media.stat().st_size
        receipt = _document(directory / "download.json")
        return (
            size
            if size > 0
            and type(receipt.get("size")) is int
            and receipt == {"video_id": video_id, "size": size}
            else None
        )
    except OSError, ValueError:
        return None


def inventory_inputs(
    directory: Path, operation_id: str, video_id: str
) -> PartialInventory:
    """Credit only the operation's recorded selected formats and regular inputs."""
    if directory.name != operation_id or directory.is_symlink():
        return PartialInventory(finding="unknown operation or symlink directory")
    if not directory.exists():
        return PartialInventory()
    unvalidated_bytes = 0
    try:
        unvalidated_bytes = sum(
            path.stat().st_size
            for path in directory.iterdir()
            if INPUT_PATTERN.fullmatch(path.name)
            and path.is_file()
            and not path.is_symlink()
        )
        manifest = _document(directory / "inputs.json")
        if (
            manifest.get("operation_id") != operation_id
            or manifest.get("video_id") != video_id
        ):
            raise ValueError("operation identity mismatch")
        formats = manifest["formats"]
        if not isinstance(formats, dict) or not formats:
            raise ValueError("missing selected formats")
        files: list[Path] = []
        reusable = partial = 0
        newest = 0.0
        for path in directory.iterdir():
            match = INPUT_PATTERN.fullmatch(path.name)
            if not match:
                if ".part" in path.name or path.name.endswith(".ytdl"):
                    raise ValueError("unknown partial input")
                continue
            if path.is_symlink() or not path.is_file():
                raise ValueError("symlink or nonregular input")
            if match[1] not in formats or formats[match[1]].get("ext") != match[2]:
                raise ValueError("selected format mismatch")
            if path.name.endswith(".ytdl") and not _valid_sidecar(path):
                raise ValueError("malformed downloader sidecar")
            stat = path.stat()
            files.append(path)
            partial += stat.st_size
            newest = max(newest, stat.st_mtime)
            # Legacy names and fragment scratch files need adoption/completion first.
            if (
                path.name.startswith("merging.")
                and not path.name.endswith(".ytdl")
                and "-Frag" not in path.name
            ):
                reusable += stat.st_size
        return PartialInventory(reusable, partial, tuple(files), newest)
    except FileNotFoundError:
        return PartialInventory(
            partial_bytes=unvalidated_bytes,
            finding="input inventory changed or manifest absent",
        )
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as error:
        return PartialInventory(
            partial_bytes=unvalidated_bytes, finding=f"invalid input identity: {error}"
        )


def prepare_inputs(
    directory: Path, operation_id: str, video_id: str, selected: list[dict[str, Any]]
) -> PartialInventory:
    """Record format identity and adopt legacy input/sidecar sets idempotently."""
    if directory.name != operation_id or directory.is_symlink():
        raise StagingIdentityError("unknown operation or symlink directory")
    formats = {
        str(item["format_id"]): {
            key: item.get(key)
            for key in ("ext", "protocol", "vcodec", "acodec", "width", "height")
        }
        for item in selected
    }
    if not formats or any(
        not re.fullmatch(r"[\w-]+", key)
        or not re.fullmatch(r"[a-z0-9]+", str(value["ext"]))
        for key, value in formats.items()
    ):
        raise StagingIdentityError("invalid selected formats")
    manifest_path = directory / "inputs.json"
    manifest = {"operation_id": operation_id, "video_id": video_id, "formats": formats}
    if manifest_path.exists():
        try:
            existing = _document(manifest_path)
        except (OSError, ValueError) as error:
            raise StagingIdentityError("invalid input manifest") from error
        if existing != manifest and (
            existing.get("operation_id") != operation_id
            or existing.get("video_id") != video_id
            or any(INPUT_PATTERN.fullmatch(path.name) for path in directory.iterdir())
        ):
            raise StagingIdentityError(
                "selected formats changed; retained previous inputs"
            )
    moves: list[tuple[Path, Path]] = []
    for path in directory.iterdir():
        match = INPUT_PATTERN.fullmatch(path.name)
        if not match:
            continue
        if path.is_symlink() or not path.is_file():
            raise StagingIdentityError("symlink or nonregular input")
        if match[1] not in formats or formats[match[1]]["ext"] != match[2]:
            raise StagingIdentityError("legacy selected format mismatch")
        if path.name.endswith(".ytdl"):
            try:
                if not _valid_sidecar(path):
                    raise ValueError("missing downloader state")
            except (OSError, ValueError) as error:
                raise StagingIdentityError("malformed downloader sidecar") from error
        if path.name.startswith("media."):
            destination = path.with_name("merging." + path.name[len("media.") :])
            if destination.exists() or destination.is_symlink():
                raise StagingIdentityError("legacy destination conflict")
            moves.append((path, destination))
    # Publish identity before moves. A crash leaves a retryable mixed-name set.
    temporary = manifest_path.with_suffix(".tmp")
    if temporary.is_symlink():
        raise StagingIdentityError("symlink manifest temporary")
    temporary.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    temporary.replace(manifest_path)
    for source, destination in moves:
        source.rename(destination)
    return inventory_inputs(directory, operation_id, video_id)


def maintain_partials(
    state: Any,
    settings: Any,
    *,
    now: float | None = None,
    youtube: Any = None,
    controller: Any = None,
) -> dict[str, Any]:
    """Observe or reclaim only owned inactive retryable inputs above the target."""
    current = time.time() if now is None else now
    rows = state.staging_rows()
    root = settings.download_root.resolve()
    known: set[str] = set()
    candidates: list[tuple[Any, PartialInventory, float]] = []
    findings: list[dict[str, Any]] = []
    inactive = 0
    for row in rows:
        if controller:
            controller.check()
        directory = Path(row.working_directory)
        if directory.parent.resolve() != root or not row.operation_id:
            findings.append(
                {"video_id": row.video_id, "reason": "unknown operation path"}
            )
            continue
        known.add(directory.name)
        if (
            completed_download_size(directory, row.operation_id, row.video_id)
            is not None
        ):
            continue
        inventory = inventory_inputs(directory, row.operation_id, row.video_id)
        if (
            youtube
            and inventory.partial_bytes
            and inventory.finding == "input inventory changed or manifest absent"
            and row.status == "download"
            and not row.claimed_at
            and not row.download_completed_at
            and (
                not row.retry_at
                or row.retry_at.replace(tzinfo=UTC).timestamp() <= current
            )
        ):
            token = state.claim_partial_maintenance(row.video_id, row.operation_id)
            if token:
                outcome = "partial inputs validated; resumption eligible"
                try:
                    contract = json.loads(row.frozen_metadata or "{}")
                    if not isinstance(contract, dict) or any(
                        contract.get(key) != value
                        for key, value in (
                            ("operation_id", row.operation_id),
                            ("identifier", row.ia_identifier),
                            ("media_name", row.media_name),
                            (
                                "source_url",
                                f"https://www.youtube.com/watch?v={row.video_id}",
                            ),
                        )
                    ):
                        raise StagingIdentityError("unknown partial operation contract")
                    selected, expected = youtube.selected_formats(row.video_id)
                    if controller:
                        controller.check()
                    inventory = prepare_inputs(
                        directory, row.operation_id, row.video_id, selected
                    )
                    state.update_archive(
                        row.video_id,
                        download_estimated_bytes=max(
                            row.download_estimated_bytes or 0,
                            expected,
                            contract.get("estimated_bytes") or 0,
                        ),
                    )
                    # Fresh validation protects just-adopted inputs from eviction.
                    row.updated_at = datetime.fromtimestamp(current, UTC)
                except (
                    OSError,
                    RuntimeError,
                    ValueError,
                    KeyError,
                    TypeError,
                ) as error:
                    outcome = redact_diagnostics(
                        f"partial adoption failed: {type(error).__name__}: {error}"
                    )
                    findings.append({"video_id": row.video_id, "reason": outcome})
                finally:
                    state.release_partial_maintenance(
                        row.video_id, token, error=outcome
                    )
        if inventory.finding:
            findings.append({"video_id": row.video_id, "reason": inventory.finding})
        if (
            row.status != "download"
            or row.claimed_at
            or row.media_uploaded_at
            or row.download_completed_at
        ):
            continue
        inactive += inventory.partial_bytes
        age = current - max(
            inventory.newest_write, row.updated_at.replace(tzinfo=UTC).timestamp()
        )
        if (
            not inventory.finding
            and inventory.files
            and age >= settings.partial_min_age
        ):
            candidates.append((row, inventory, age))
    for directory in settings.download_root.iterdir():
        if directory.name not in known:
            findings.append(
                {"operation_id": directory.name, "reason": "unknown staging entry"}
            )
    candidates.sort(key=lambda item: (-item[2], item[0].video_id))
    decisions: list[dict[str, Any]] = []
    remaining = inactive
    for row, inventory, age in candidates:
        if remaining <= settings.partial_max_bytes:
            break
        decision = {
            "operation_id": row.operation_id,
            "video_id": row.video_id,
            "bytes": inventory.partial_bytes,
            "age_seconds": age,
            "reason": "inactive partial target exceeded",
            "action": "observe",
        }
        decisions.append(decision)
        if settings.partial_retention_mode == "observe":
            remaining -= inventory.partial_bytes
            continue
        token = state.claim_partial_maintenance(row.video_id, row.operation_id)
        if token is None:
            decision["action"] = "claim conflict"
            continue
        error = "partial maintenance skipped; identity or age changed"
        try:
            refreshed = inventory_inputs(
                Path(row.working_directory), row.operation_id, row.video_id
            )
            if (
                refreshed.finding
                or current - refreshed.newest_write < settings.partial_min_age
            ):
                decision["action"] = "identity or age changed"
                continue
            for path in refreshed.files:
                path.unlink(missing_ok=True)
            remaining -= refreshed.partial_bytes
            error = (
                "partial inputs evicted by retention policy; fresh download eligible"
            )
            decision["action"] = "evicted"
        except OSError as exception:
            error = f"partial eviction failed: {type(exception).__name__}"
            decision["action"] = error
        finally:
            state.release_partial_maintenance(row.video_id, token, error=error)
    return {
        "mode": settings.partial_retention_mode,
        "inactive_bytes": inactive,
        "target_bytes": settings.partial_max_bytes,
        "decisions": decisions,
        "findings": findings,
        "waiting_reason": "protected partials exceed retention target"
        if remaining > settings.partial_max_bytes
        else None,
    }
