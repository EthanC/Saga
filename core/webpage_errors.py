"""Evidence that a Ziggy submission never reached its HTTP endpoint."""

import re
from socket import gaierror
from urllib.error import URLError

from archivist import NetworkError


class SubmissionNotSentError(NetworkError):
    """Connection establishment failed before the POST could be sent."""


def known_connect_failure(error: OSError) -> bool:
    reason = error.reason if isinstance(error, URLError) else error
    return isinstance(reason, (ConnectionRefusedError, gaierror))


def legacy_ziggy_not_sent(detail: str | None) -> bool:
    return bool(
        re.fullmatch(
            r"SubmissionUncertainError: Ziggy response was lost: "
            r"<urlopen error \[Errno (?:111|61|10061)\] Connection refused>",
            detail or "",
        )
    )
