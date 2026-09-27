import shutil
import subprocess
from pathlib import Path

import pytest

from core.youtube import verify_media


@pytest.mark.skipif(
    shutil.which("ffmpeg") is None or shutil.which("ffprobe") is None,
    reason="ffmpeg and ffprobe are not installed",
)
def test_ffmpeg_merge_and_ffprobe_inspection(tmp_path: Path) -> None:
    output = tmp_path / "smoke.mkv"
    subprocess.run(
        [
            "ffmpeg",
            "-v",
            "error",
            "-f",
            "lavfi",
            "-i",
            "color=size=16x16:rate=1:duration=1",
            "-f",
            "lavfi",
            "-i",
            "anullsrc=sample_rate=8000:channel_layout=mono",
            "-shortest",
            "-c:v",
            "mpeg4",
            "-c:a",
            "aac",
            str(output),
        ],
        check=True,
        capture_output=True,
        timeout=30,
    )

    media = verify_media(output)

    assert media.path == output
    assert media.size == output.stat().st_size
    assert len(media.md5) == 32
    assert len(media.sha1) == 40
