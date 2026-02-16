import asyncio
import hashlib
import logging
import posixpath
from pathlib import Path
from typing import Callable

from telethon import utils

from core.config import sanitize_folder_name
from core.db import AppDatabase
from core.ftps_client import FTPSSync
from core.models import Settings
from core.telegram_client import create_telegram_client, resolve_channel_entity


def _sha256_local_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_obj:
        while True:
            chunk = file_obj.read(1024 * 512)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


async def fetch_ftps_preview(
    settings: Settings,
    db_path: Path,
    limit: int = 300,
) -> tuple[bool, str, list[dict]]:
    if not settings.use_ftps:
        return False, "FTPS не используется.", []

    channel_ref = (settings.channel or "").strip()
    db = AppDatabase(db_path)
    ftps_sync = FTPSSync(settings)
    try:
        base_remote = (settings.ftps_remote_dir or "/").strip() or "/"
        state = db.get_channel_state_by_ref(channel_ref) if channel_ref else {}
        channel_title = str(state.get("channel_title", "") or channel_ref or "").strip()
        folder_raw = str(state.get("download_folder", "") or "").strip()
        if folder_raw:
            channel_folder = Path(folder_raw).name
        else:
            channel_folder = sanitize_folder_name(channel_title or channel_ref or "_")

        await asyncio.to_thread(ftps_sync.connect)

        base_items = await asyncio.to_thread(ftps_sync.list_remote_entries, base_remote, max(20, limit))
        channel_remote = (
            posixpath.join(base_remote.rstrip("/"), channel_folder) if base_remote != "/" else f"/{channel_folder}"
        )
        channel_items = await asyncio.to_thread(ftps_sync.list_remote_entries, channel_remote, limit)

        rows: list[dict] = []
        for item in channel_items:
            rows.append(
                {
                    "scope": "channel",
                    "name": item.get("name", ""),
                    "type": item.get("type", ""),
                    "size": item.get("size", ""),
                    "modify": item.get("modify", ""),
                    "path": item.get("path", ""),
                    "list_source": item.get("list_source", ""),
                }
            )

        if not rows:
            for item in base_items:
                rows.append(
                    {
                        "scope": "base",
                        "name": item.get("name", ""),
                        "type": item.get("type", ""),
                        "size": item.get("size", ""),
                        "modify": item.get("modify", ""),
                        "path": item.get("path", ""),
                        "list_source": item.get("list_source", ""),
                    }
                )
            if not rows:
                root_items = await asyncio.to_thread(ftps_sync.list_remote_entries, ".", 50)
                rows = [
                    {
                        "scope": "info",
                        "name": "channel dir empty/missing",
                        "type": "info",
                        "size": "",
                        "modify": "",
                        "path": channel_remote,
                        "list_source": "",
                    },
                    {
                        "scope": "info",
                        "name": "base dir entries: 0",
                        "type": "info",
                        "size": "",
                        "modify": "",
                        "path": base_remote,
                        "list_source": "",
                    },
                    {
                        "scope": "info",
                        "name": f"cwd entries: {len(root_items)}",
                        "type": "info",
                        "size": "",
                        "modify": "",
                        "path": ".",
                        "list_source": ".",
                    },
                ]
            return (
                True,
                "FTPS preview: "
                f"channel dir empty/missing ({channel_remote}), "
                f"base dir ({base_remote}) entries={len(base_items)}.",
                rows,
            )

        return True, f"FTPS preview: {len(rows)} entries from {channel_remote}", rows
    except Exception as exc:
        return False, f"FTPS preview error: {exc}", []
    finally:
        try:
            await asyncio.to_thread(ftps_sync.close)
        except Exception:
            pass
        db.close()


async def ftps_audit_selected_channel(
    settings: Settings,
    db_path: Path,
    set_status: Callable[..., None],
    worker_status: dict,
) -> tuple[bool, str]:
    if not settings.use_ftps:
        return True, "FTPS не используется."
    channel_ref = (settings.channel or "").strip()
    if not channel_ref or channel_ref == "_":
        return False, "Укажите CHANNEL_ID для проверки FTPS."

    client = create_telegram_client(settings)
    await client.connect()
    db = AppDatabase(db_path)
    ftps_sync = FTPSSync(settings)

    async def _check_ftps_with_retry(
        file_path: Path,
        local_hash: str,
        channel_folder_name: str,
        remote_path_hint: str = "",
        attempts: int = 3,
    ) -> tuple[bool, str]:
        last_exc: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                return await asyncio.to_thread(
                    ftps_sync.check_remote_file_status,
                    str(file_path),
                    True,
                    local_hash,
                    remote_path_hint,
                )
            except Exception as exc:
                last_exc = exc
                err = str(exc).lower()
                recoverable = (
                    "timed out" in err
                    or "timeout" in err
                    or "ssl handshake" in err
                    or "broken pipe" in err
                    or "eof occurred" in err
                )
                if attempt >= attempts or not recoverable:
                    raise
                logging.warning(
                    "FTPS audit: retrying check for %s attempt=%s/%s reason=%s",
                    file_path,
                    attempt,
                    attempts,
                    exc,
                )
                try:
                    await asyncio.to_thread(ftps_sync.close)
                except Exception:
                    pass
                await asyncio.sleep(min(4, attempt))
                await asyncio.to_thread(ftps_sync.connect)
                await asyncio.to_thread(ftps_sync.prepare_channel_dir, channel_folder_name)
        if last_exc:
            raise last_exc
        raise RuntimeError("FTPS check failed for unknown reason")

    try:
        if not await client.is_user_authorized():
            return False, "Сессия не авторизована. Сначала нажмите Авторизоваться."
        channel = await resolve_channel_entity(client, channel_ref)
        channel_id = int(utils.get_peer_id(channel))
        channel_title = getattr(channel, "title", channel_ref)
        state = db.get_channel_state_by_ref(channel_ref)
        folder_raw = str(state.get("download_folder", "") or "").strip()
        if folder_raw:
            channel_folder = Path(folder_raw).name
        else:
            channel_folder = sanitize_folder_name(channel_title)

        records = db.list_downloads_by_channel(channel_id)
        if not records:
            set_status(
                ftps_check_running=False,
                ftps_check_checked=0,
                ftps_check_total=0,
                ftps_check_verified=0,
                ftps_check_missing=0,
                ftps_check_failed=0,
                ftps_check_cleaned=0,
                ftps_check_current_file="",
                ftps_check_last_info="",
                ftps_check_missing_examples=[],
            )
            return True, "FTPS проверка: в БД нет локальных файлов для этого канала."

        candidates: list[tuple[int, Path]] = []
        for record in records:
            file_path_raw = (record.get("file_path") or "").strip()
            message_id = int(record.get("message_id") or 0)
            if not file_path_raw or message_id <= 0:
                continue
            file_path = Path(file_path_raw)
            if not file_path.exists() or not file_path.is_file():
                continue
            candidates.append((message_id, file_path))

        set_status(
            ftps_check_running=True,
            ftps_check_checked=0,
            ftps_check_total=len(candidates),
            ftps_check_verified=0,
            ftps_check_missing=0,
            ftps_check_failed=0,
            ftps_check_cleaned=0,
            ftps_check_current_file="",
            ftps_check_last_info="",
            ftps_check_missing_examples=[],
        )

        await asyncio.to_thread(ftps_sync.connect)
        await asyncio.to_thread(ftps_sync.prepare_channel_dir, channel_folder)
        try:
            listing_rows = await asyncio.to_thread(
                ftps_sync.list_remote_entries_ftplib,
                ftps_sync.remote_channel_dir or "/",
                5000,
            )
            db.replace_remote_listing_cache(channel_ref, "FTPS", listing_rows)
        except Exception:
            logging.exception("FTPS audit: failed to cache remote listing in DB")

        checked = 0
        verified = 0
        cleaned = 0
        missing_remote = 0
        failed = 0
        missing_examples: list[str] = []
        for message_id, file_path in candidates:
            checked += 1
            set_status(
                ftps_check_running=True,
                ftps_check_checked=checked,
                ftps_check_total=len(candidates),
                ftps_check_verified=verified,
                ftps_check_missing=missing_remote,
                ftps_check_failed=failed,
                ftps_check_cleaned=cleaned,
                ftps_check_current_file=file_path.name,
                ftps_check_last_info="",
                ftps_check_missing_examples=missing_examples,
            )

            local_hash = ""
            try:
                stat = file_path.stat()
                existing = db.get_local_file_meta_by_path(channel_id, str(file_path))
                if (
                    existing
                    and int(existing.get("file_size", 0)) == int(stat.st_size)
                    and abs(float(existing.get("file_mtime", 0.0)) - float(stat.st_mtime)) < 0.0001
                    and str(existing.get("file_sha256", "")).strip()
                ):
                    local_hash = str(existing.get("file_sha256", "")).strip()
                else:
                    local_hash = _sha256_local_file(file_path)
                    db.upsert_local_file_meta(
                        channel_id=channel_id,
                        message_id=message_id,
                        file_path=str(file_path),
                        file_size=int(stat.st_size),
                        file_mtime=float(stat.st_mtime),
                        file_sha256=local_hash,
                    )
            except Exception:
                logging.exception("FTPS audit: failed to compute local hash for %s", file_path)

            try:
                remote_path_hint = db.get_remote_cached_path(channel_ref, "FTPS", file_path.name)
                ok, info = await _check_ftps_with_retry(
                    file_path,
                    local_hash,
                    channel_folder,
                    remote_path_hint,
                )
            except Exception as exc:
                failed += 1
                logging.warning("FTPS audit: check failed for %s: %s", file_path, exc)
                set_status(
                    ftps_check_running=True,
                    ftps_check_checked=checked,
                    ftps_check_total=len(candidates),
                    ftps_check_verified=verified,
                    ftps_check_missing=missing_remote,
                    ftps_check_failed=failed,
                    ftps_check_cleaned=cleaned,
                    ftps_check_current_file=file_path.name,
                    ftps_check_last_info=f"check_failed: {exc}",
                    ftps_check_missing_examples=missing_examples,
                )
                continue
            if ok:
                verified += 1
                remote_path = ""
                marker = "remote="
                if marker in info:
                    remote_path = info.split(marker, 1)[1].split(";", 1)[0].strip()
                db.mark_remote_uploaded(channel_id, message_id, "FTPS", remote_path)
                if settings.cleanup_local_after_ftps:
                    try:
                        file_path.unlink()
                        db.delete_local_file_meta(channel_id, str(file_path))
                        cleaned += 1
                    except Exception:
                        logging.warning("FTPS audit: failed to delete local file %s", file_path)
            else:
                if "remote_missing" in info:
                    missing_remote += 1
                    if len(missing_examples) < 5:
                        missing_examples.append(info)
                else:
                    failed += 1
            set_status(
                ftps_check_running=True,
                ftps_check_checked=checked,
                ftps_check_total=len(candidates),
                ftps_check_verified=verified,
                ftps_check_missing=missing_remote,
                ftps_check_failed=failed,
                ftps_check_cleaned=cleaned,
                ftps_check_current_file=file_path.name,
                ftps_check_last_info=info,
                ftps_check_missing_examples=missing_examples,
            )

        missing_tail = ""
        if missing_examples:
            missing_tail = " Примеры missing: " + " | ".join(missing_examples)
        set_status(
            ftps_check_running=False,
            ftps_check_checked=checked,
            ftps_check_total=len(candidates),
            ftps_check_verified=verified,
            ftps_check_missing=missing_remote,
            ftps_check_failed=failed,
            ftps_check_cleaned=cleaned,
            ftps_check_current_file="",
            ftps_check_last_info="",
            ftps_check_missing_examples=missing_examples,
        )

        return (
            True,
            "FTPS проверка: "
            f"проверено {checked}, подтверждено {verified}, отсутствуют на сервере {missing_remote}, ошибок {failed}, "
            f"локально удалено {cleaned}.{missing_tail}",
        )
    except Exception as exc:
        set_status(
            ftps_check_running=False,
            ftps_check_failed=int(worker_status.get("ftps_check_failed", 0)) + 1,
            ftps_check_current_file="",
            ftps_check_last_info=f"audit_exception: {exc}",
        )
        return False, f"FTPS проверка: ошибка ({exc})"
    finally:
        try:
            await asyncio.to_thread(ftps_sync.close)
        except Exception:
            pass
        db.close()
        await client.disconnect()
