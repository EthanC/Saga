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
