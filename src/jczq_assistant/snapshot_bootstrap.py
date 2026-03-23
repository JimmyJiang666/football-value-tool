"""只读部署时的 SQLite 快照下载辅助。"""

from __future__ import annotations

import gzip
import os
from pathlib import Path
import shutil
import tempfile

import requests

from jczq_assistant.config import REQUEST_TIMEOUT_SECONDS, REQUEST_USER_AGENT


def _download_to_tempfile(snapshot_url: str) -> Path:
    headers = {"User-Agent": REQUEST_USER_AGENT}
    response = requests.get(snapshot_url, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS * 4, stream=True)
    response.raise_for_status()

    suffix = "".join(Path(snapshot_url).suffixes) or ".sqlite3"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp_file:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                tmp_file.write(chunk)
        return Path(tmp_file.name)


def ensure_sqlite_snapshot(
    *,
    target_path: Path,
    snapshot_url: str | None,
) -> bool:
    """确保只读部署所需的 SQLite 快照存在。

    返回值表示这次是否实际下载了新文件。
    """

    if target_path.exists():
        return False
    if not snapshot_url:
        return False

    target_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = _download_to_tempfile(snapshot_url)
    temp_output_path = target_path.with_name(f"{target_path.name}.downloading")

    try:
        if snapshot_url.lower().endswith(".gz"):
            with gzip.open(temp_path, "rb") as source, open(temp_output_path, "wb") as destination:
                shutil.copyfileobj(source, destination)
        else:
            shutil.move(str(temp_path), temp_output_path)

        os.replace(temp_output_path, target_path)
        return True
    finally:
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)
        temp_output_path.unlink(missing_ok=True)
