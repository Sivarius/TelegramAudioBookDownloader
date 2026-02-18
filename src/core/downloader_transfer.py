import asyncio
import logging
from pathlib import Path
from typing import Callable, Optional

from telethon.errors import FloodWaitError
from telethon.tl.custom.message import Message

from core.db import AppDatabase
from core.telegram_client import is_audio_message
from core.telegram_client import message_channel_id_variants
from core.downloader_utils import (
    _build_target_path,
    _expected_size,
    _is_local_file_valid,
)

async def download_if_needed(
    message: Message,
    db: AppDatabase,
    download_dir: Path,
    status_hook: Optional[Callable[[dict], None]] = None,
    channel_ref: str = "",
    channel_title: str = "",
    remote_sync: Optional[object] = None,
    cleanup_local_after_remote: bool = False,
) -> None:
    channel_ids = message_channel_id_variants(message)
    if not channel_ids:
        return
    # Prefer marked ID first for stable channel_state keys.
    channel_id = sorted(channel_ids, key=lambda cid: 0 if cid < 0 else 1)[0]

    if not is_audio_message(message):
        if status_hook:
            status_hook({"event": "skipped", "message_id": message.id, "reason": "not_audio"})
        return

    download_dir.mkdir(parents=True, exist_ok=True)
    expected_size = _expected_size(message)
    existing_path = db.get_downloaded_file_path(channel_id, message.id)

    if db.already_downloaded(channel_id, message.id):
        file_path = Path(existing_path) if existing_path else Path()
        if existing_path and _is_local_file_valid(file_path, expected_size):
            if status_hook:
                status_hook({"event": "skipped", "message_id": message.id, "reason": "already_downloaded"})
            return
        if status_hook:
            status_hook({"event": "retry_incomplete", "message_id": message.id})
        db.unmark_downloaded(channel_id, message.id)
        if existing_path and file_path.exists():
            try:
                file_path.unlink()
            except Exception:
                logging.warning("Failed to remove incomplete file %s", file_path)

    try:
        if status_hook:
            status_hook({"event": "downloading", "message_id": message.id})
        target_path = _build_target_path(download_dir, message, existing_path)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = target_path.with_name(f"{target_path.name}.part")
        if temp_path.exists():
            temp_path.unlink()
        last_percent = {"value": -1}

        def _progress_callback(received: int, total: int) -> None:
            if not status_hook or total <= 0:
                return
            percent = int((received * 100) / total)
            if percent == last_percent["value"]:
                return
            last_percent["value"] = percent
            status_hook(
                {
                    "event": "progress",
                    "message_id": message.id,
                    "received": int(received),
                    "total": int(total),
                    "percent": percent,
                }
            )

        downloaded_path = await message.download_media(
            file=str(temp_path), progress_callback=_progress_callback
        )
        if downloaded_path is None:
            logging.warning("Audio detected but no file downloaded for message_id=%s", message.id)
            if status_hook:
                status_hook({"event": "failed", "message_id": message.id, "reason": "empty_file_path"})
            return
        downloaded_temp = Path(str(downloaded_path))
        if not _is_local_file_valid(downloaded_temp, expected_size):
            raise RuntimeError(
                f"Incomplete download for message_id={message.id} "
                f"(expected_size={expected_size}, got={downloaded_temp.stat().st_size if downloaded_temp.exists() else 0})"
            )
        downloaded_temp.replace(target_path)
        file_path = target_path

        db.mark_downloaded(channel_id, message.id, str(file_path))
        db.update_channel_state(
            channel_id=channel_id,
            channel_ref=channel_ref,
            channel_title=channel_title,
            download_folder=str(download_dir),
            last_message_id=message.id,
            last_file_path=str(file_path),
        )
        logging.info("Downloaded message_id=%s to %s", message.id, file_path)
        if remote_sync and getattr(remote_sync, "enabled", False):
            try:
                if status_hook:
                    status_hook(
                        {
                            "event": "uploading",
                            "message_id": message.id,
                            "file_path": str(file_path),
                            "transport": str(getattr(remote_sync, "name", "REMOTE")),
                        }
                    )

                def _upload_progress(received: int, total: int) -> None:
                    if not status_hook:
                        return
                    percent = int((received * 100) / total) if total > 0 else 0
                    status_hook(
                        {
                            "event": "upload_progress",
                            "message_id": message.id,
                            "received": int(received),
                            "total": int(total),
                            "percent": int(percent),
                            "transport": str(getattr(remote_sync, "name", "REMOTE")),
                        }
                    )

                uploaded, sftp_info = await asyncio.to_thread(
                    remote_sync.upload_file_if_needed,
                    str(file_path),
                    _upload_progress,
                    (
                        cleanup_local_after_remote
                        and str(getattr(remote_sync, "name", "")).upper() != "FTPS"
                    ),
                )
                transport = str(getattr(remote_sync, "name", "REMOTE"))
                remote_path = ""
                marker = "remote="
                if marker in sftp_info:
                    remote_path = sftp_info.split(marker, 1)[1].split(";", 1)[0].strip()
                db.mark_remote_uploaded(channel_id, message.id, transport, remote_path)
                if status_hook:
                    status_hook(
                        {
                            "event": "sftp_uploaded" if uploaded else "sftp_skipped",
                            "message_id": message.id,
                            "file_path": str(file_path),
                            "sftp_info": sftp_info,
                            "transport": transport,
                        }
                    )
                if cleanup_local_after_remote and "verified=True" in sftp_info:
                    local_path = Path(str(file_path))
                    if local_path.exists():
                        local_path.unlink()
                        if status_hook:
                            status_hook(
                                {
                                    "event": "local_cleaned",
                                    "message_id": message.id,
                                    "file_path": str(file_path),
                                    "transport": transport,
                                }
                            )
            except Exception as sftp_exc:
                logging.exception("Remote upload failed for message_id=%s", message.id)
                if status_hook:
                    status_hook(
                        {
                            "event": "sftp_failed",
                            "message_id": message.id,
                            "file_path": str(file_path),
                            "reason": str(sftp_exc),
                            "transport": str(getattr(remote_sync, "name", "REMOTE")),
                        }
                    )
        if status_hook:
            status_hook({"event": "downloaded", "message_id": message.id, "file_path": str(file_path)})
    except FloodWaitError:
        raise
    except asyncio.CancelledError:
        target_path = _build_target_path(download_dir, message, existing_path)
        temp_path = target_path.with_name(f"{target_path.name}.part")
        if temp_path.exists():
            try:
                temp_path.unlink()
            except Exception:
                logging.warning("Failed to remove temp file after cancel: %s", temp_path)
        if status_hook:
            status_hook({"event": "failed", "message_id": message.id, "reason": "cancelled"})
        raise
    except Exception as exc:
        logging.exception("Failed to download message_id=%s", message.id)
        target_path = _build_target_path(download_dir, message, existing_path)
        temp_path = target_path.with_name(f"{target_path.name}.part")
        if temp_path.exists():
            try:
                temp_path.unlink()
            except Exception:
                logging.warning("Failed to remove temp file: %s", temp_path)
        if status_hook:
            status_hook({"event": "failed", "message_id": message.id, "reason": str(exc)})


