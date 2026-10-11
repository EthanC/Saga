"""Recognize access failures without importing the downloader or database."""


class VideoAccessError(RuntimeError):
    """YouTube requires authentication or has challenged the current session."""


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
