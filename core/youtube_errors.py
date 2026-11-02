"""Recognize access failures without importing the downloader or database."""

from hashlib import sha256


class VideoAccessError(RuntimeError):
    """YouTube requires authentication or has challenged the current session."""

    cookie_generation: str | None = None


def cookie_fingerprint(contents: str | None) -> str:
    """Identify an attempted session without storing its cookie values."""
    return (
        "anonymous"
        if contents is None
        else sha256(contents.encode("utf-8")).hexdigest()
    )


def is_access_error(detail: str) -> bool:
    """Check access requirements before generic video-unavailable wording."""
    detail = detail.casefold().replace("’", "'")
    return any(
        marker in detail
        for marker in (
            "sign in",
            "confirm your age",
            "age-restricted",
            "not a bot",
            "login required",
            "members-only",
            "cookies are no longer valid",
        )
    )


UNAVAILABLE_MARKERS = (
    "private video",
    "video unavailable",
    "this video is unavailable",
    "has been removed",
)
PERMANENT_MARKERS = (
    *UNAVAILABLE_MARKERS,
    "members-only",
    "available to this channel's members",
    "join this channel",
    "account associated with this video has been terminated",
    "no longer available",
)
TRANSIENT_MARKERS = ("try again later", "rate-limit", "rate limit", "too many requests")


def is_permanent_video_error(detail: str) -> bool:
    """Check whether YouTube reported a video as gone rather than not yet ready."""
    detail = detail.casefold().replace("’", "'")
    if any(marker in detail for marker in TRANSIENT_MARKERS):
        return False
    return any(marker in detail for marker in PERMANENT_MARKERS)
