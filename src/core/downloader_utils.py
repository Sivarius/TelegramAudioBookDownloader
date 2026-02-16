import hashlib
from pathlib import Path
from typing import Optional

from telethon.tl.custom.message import Message

from core.db import AppDatabase
from core.ftps_client import FTPSSync
from core.models import Settings
from core.sftp_client import SFTPSync

def _expected_size(message: Message) -> int:
    if message.file and getattr(message.file, "size", None):
        try:
            return int(message.file.size)
        except Exception:
            return 0
    return 0


def _is_local_file_valid(file_path: Path, expected_size: int) -> bool:
    if not file_path.exists() or not file_path.is_file():
        return False
    if expected_size > 0:
        return file_path.stat().st_size == expected_size
    return file_path.stat().st_size > 0


def _build_target_path(download_dir: Path, message: Message, existing_path: str) -> Path:
    if existing_path:
        return Path(existing_path)

    original_name = ""
    if message.file and getattr(message.file, "name", None):
        original_name = str(message.file.name)
    original_name = Path(original_name).name
    if not original_name:
        ext = ""
        if message.file and getattr(message.file, "ext", None):
            ext = str(message.file.ext or "")
        original_name = f"message_{message.id}{ext}"
    return download_dir / original_name


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_obj:
        while True:
            chunk = file_obj.read(1024 * 512)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _get_or_build_local_hash(db: AppDatabase, channel_id: int, message_id: int, file_path: Path) -> str:
    stat = file_path.stat()
    file_size = int(stat.st_size)
    file_mtime = float(stat.st_mtime)
    existing = db.get_local_file_meta_by_path(channel_id, str(file_path))
    if (
        existing
        and int(existing.get("file_size", 0)) == file_size
        and abs(float(existing.get("file_mtime", 0.0)) - file_mtime) < 0.0001
        and str(existing.get("file_sha256", "")).strip()
    ):
        return str(existing.get("file_sha256")).strip()
    file_sha256 = _sha256_file(file_path)
    db.upsert_local_file_meta(
        channel_id=channel_id,
        message_id=message_id,
        file_path=str(file_path),
        file_size=file_size,
        file_mtime=file_mtime,
        file_sha256=file_sha256,
    )
    return file_sha256


def _build_remote_sync(settings: Settings) -> Optional[object]:
    if settings.use_sftp:
        return SFTPSync(settings)
    if settings.use_ftps:
        return FTPSSync(settings)
    return None

