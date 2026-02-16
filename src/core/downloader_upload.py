import asyncio
import logging
from pathlib import Path
from typing import Callable, Optional

from core.config import sanitize_folder_name
from core.db import AppDatabase
from core.models import Settings
from core.downloader_utils import _build_remote_sync, _get_or_build_local_hash

async def run_remote_uploader(
    settings: Settings,
    db_path: Path,
    status_hook: Optional[Callable[[dict], None]] = None,
    stop_requested: Optional[Callable[[], bool]] = None,
) -> None:
    remote_sync = _build_remote_sync(settings)
    if not remote_sync or not getattr(remote_sync, "enabled", False):
        raise RuntimeError("Включите SFTP или FTPS для загрузки.")

    db = AppDatabase(db_path)
    try:
        state = db.get_channel_state_by_ref(settings.channel)
        channel_id = db.get_channel_id_by_ref(settings.channel)
        folder_raw = str(state.get("download_folder", "") or "").strip()
        if not folder_raw:
            fallback = settings.download_dir / sanitize_folder_name(settings.channel)
            folder_raw = str(fallback)
        local_dir = Path(folder_raw)
        if not local_dir.exists() or not local_dir.is_dir():
            raise RuntimeError(f"Локальная папка для upload не найдена: {local_dir}")

        files = sorted(
            [
                p
                for p in local_dir.iterdir()
                if p.is_file() and p.suffix.lower() != ".part"
            ],
            key=lambda p: p.name.lower(),
        )
        if not files:
            if status_hook:
                status_hook({"event": "upload_done", "message": "Нет файлов для загрузки."})
            return

        file_message_ids: dict[str, int] = {}
        file_channel_ids: dict[str, int] = {}
        file_hashes: dict[str, str] = {}
        for file_path in files:
            path_key = str(file_path)
            resolved_channel_id = int(channel_id or 0)
            message_id = 0
            if resolved_channel_id > 0:
                message_id = db.get_message_id_by_file_path(resolved_channel_id, path_key)
            if message_id <= 0:
                record = db.get_download_record_by_file_path(path_key)
                if record:
                    resolved_channel_id = int(record.get("channel_id") or 0)
                    message_id = int(record.get("message_id") or 0)
            if resolved_channel_id <= 0 or message_id <= 0:
                continue
            file_channel_ids[path_key] = resolved_channel_id
            file_message_ids[path_key] = message_id
            try:
                file_hashes[path_key] = _get_or_build_local_hash(
                    db,
                    resolved_channel_id,
                    message_id,
                    file_path,
                )
            except Exception:
                logging.exception("Failed to build local hash for %s", file_path)

        channel_folder = local_dir.name
        transport = str(getattr(remote_sync, "name", "REMOTE"))
        cleanup_local_after_remote = (
            settings.cleanup_local_after_sftp
            if settings.use_sftp
            else settings.cleanup_local_after_ftps
        )
        concurrency = max(1, int(getattr(settings, "ftps_upload_concurrency", 1)))
        max_retries = 3
        if status_hook:
            status_hook(
                {
                    "event": "upload_started",
                    "message": f"{transport}: подготовка загрузки {len(files)} файлов.",
                    "concurrency": concurrency,
                }
            )

        async def _run_upload_once(
            sync_client: object,
            file_path: Path,
            transfer_id: str,
            force_upload: bool = False,
        ) -> None:
            if stop_requested and stop_requested():
                raise asyncio.CancelledError()
            if status_hook:
                status_hook(
                    {
                        "event": "uploading",
                        "message_id": transfer_id,
                        "file_path": str(file_path),
                        "transport": transport,
                    }
                )

            def _upload_progress(received: int, total: int) -> None:
                if not status_hook:
                    return
                percent = int((received * 100) / total) if total > 0 else 0
                status_hook(
                    {
                        "event": "upload_progress",
                        "message_id": transfer_id,
                        "received": int(received),
                        "total": int(total),
                        "percent": int(percent),
                        "transport": transport,
                    }
                )

            last_exc: Optional[Exception] = None
            uploaded = False
            info = ""
            remote_hint = ""
            if str(getattr(sync_client, "name", "")).upper() == "FTPS":
                remote_hint = db.get_remote_cached_path(settings.channel, "FTPS", file_path.name)
            for attempt in range(1, max_retries + 1):
                if stop_requested and stop_requested():
                    raise asyncio.CancelledError()
                try:
                    uploaded, info = await asyncio.to_thread(
                        sync_client.upload_file_if_needed,
                        str(file_path),
                        _upload_progress,
                        (
                            cleanup_local_after_remote
                            and str(getattr(sync_client, "name", "")).upper() != "FTPS"
                        ),
                        force_upload,
                    )
                    break
                except Exception as exc:
                    if stop_requested and stop_requested():
                        raise asyncio.CancelledError() from exc
                    is_timeout = "timed out" in str(exc).lower()
                    if is_timeout:
                        try:
                            await asyncio.to_thread(sync_client.close)
                        except Exception:
                            pass
                        try:
                            await asyncio.wait_for(asyncio.to_thread(sync_client.connect), timeout=30)
                            await asyncio.wait_for(
                                asyncio.to_thread(sync_client.prepare_channel_dir, channel_folder),
                                timeout=30,
                            )
                            recovered, recovered_info = await asyncio.to_thread(
                                sync_client.check_remote_file_status,
                                str(file_path),
                                (
                                    cleanup_local_after_remote
                                    and str(getattr(sync_client, "name", "")).upper() != "FTPS"
                                ),
                                "",
                                remote_hint,
                            )
                            if recovered:
                                uploaded = True
                                info = f"uploaded_after_timeout; {recovered_info}"
                                logging.warning(
                                    "Recovered upload after timeout transfer_id=%s file=%s info=%s",
                                    transfer_id,
                                    file_path,
                                    recovered_info,
                                )
                                break
                        except Exception as recovery_exc:
                            logging.warning(
                                "Timeout recovery check failed transfer_id=%s file=%s error=%s",
                                transfer_id,
                                file_path,
                                recovery_exc,
                            )
                    last_exc = exc
                    logging.warning(
                        "Upload failed transfer_id=%s attempt=%s/%s file=%s error=%s",
                        transfer_id,
                        attempt,
                        max_retries,
                        file_path,
                        exc,
                    )
                    if attempt >= max_retries:
                        raise RuntimeError(
                            f"upload failed after {max_retries} attempts: {exc}"
                        ) from exc
                    try:
                        await asyncio.to_thread(sync_client.close)
                    except Exception:
                        pass
                    if stop_requested and stop_requested():
                        raise asyncio.CancelledError() from exc
                    await asyncio.sleep(min(6, 2 * attempt))
                    if stop_requested and stop_requested():
                        raise asyncio.CancelledError() from exc
                    await asyncio.wait_for(asyncio.to_thread(sync_client.connect), timeout=30)
                    await asyncio.wait_for(
                        asyncio.to_thread(sync_client.prepare_channel_dir, channel_folder),
                        timeout=30,
                    )
            if last_exc and not info:
                raise RuntimeError(
                    f"upload failed after {max_retries} attempts: {last_exc}"
                ) from last_exc
            remote_path = ""
            marker = "remote="
            if marker in info:
                remote_path = info.split(marker, 1)[1].split(";", 1)[0].strip()
            path_key = str(file_path)
            resolved_channel_id = int(file_channel_ids.get(path_key, 0))
            message_id = int(file_message_ids.get(path_key, 0))
            if resolved_channel_id <= 0 or message_id <= 0:
                record = db.get_download_record_by_file_path(path_key)
                if record:
                    resolved_channel_id = int(record.get("channel_id") or 0)
                    message_id = int(record.get("message_id") or 0)
            if resolved_channel_id > 0 and message_id > 0:
                db.mark_remote_uploaded(resolved_channel_id, message_id, transport, remote_path)
            if status_hook:
                status_hook(
                    {
                        "event": "sftp_uploaded" if uploaded else "sftp_skipped",
                        "message_id": transfer_id,
                        "file_path": str(file_path),
                        "sftp_info": info,
                        "transport": transport,
                    }
                )
                status_hook(
                    {
                        "event": "upload_done",
                        "message_id": transfer_id,
                        "file_path": str(file_path),
                        "transport": transport,
                    }
                )

        if concurrency <= 1:
            await asyncio.to_thread(remote_sync.connect)
            await asyncio.to_thread(remote_sync.prepare_channel_dir, channel_folder)
            if str(getattr(remote_sync, "name", "")).upper() == "FTPS":
                try:
                    listing_rows = await asyncio.to_thread(
                        remote_sync.list_remote_entries_ftplib,
                        getattr(remote_sync, "remote_channel_dir", "") or "/",
                        5000,
                    )
                    db.replace_remote_listing_cache(settings.channel, "FTPS", listing_rows)
                except Exception:
                    logging.exception("FTPS upload: failed to cache remote listing in DB")
            if status_hook:
                status_hook(
                    {
                        "event": "sftp_ready",
                        "message": f"{transport} готов: {getattr(remote_sync, 'remote_channel_dir', '')}",
                        "transport": transport,
                    }
                )
            try:
                for file_path in files:
                    if stop_requested and stop_requested():
                        break
                    transfer_id = f"upload:{file_path.name}"
                    try:
                        await _run_upload_once(remote_sync, file_path, transfer_id)
                    except Exception as exc:
                        logging.exception(
                            "Upload failed transfer_id=%s file=%s", transfer_id, file_path
                        )
                        if status_hook:
                            status_hook(
                                {
                                    "event": "sftp_failed",
                                    "message_id": transfer_id,
                                    "file_path": str(file_path),
                                    "reason": str(exc),
                                    "transport": transport,
                                }
                            )
            finally:
                await asyncio.to_thread(remote_sync.close)
        else:
            if str(getattr(remote_sync, "name", "")).upper() == "FTPS":
                temp_client = type(remote_sync)(settings)
                try:
                    await asyncio.wait_for(asyncio.to_thread(temp_client.connect), timeout=30)
                    await asyncio.wait_for(
                        asyncio.to_thread(temp_client.prepare_channel_dir, channel_folder),
                        timeout=30,
                    )
                    listing_rows = await asyncio.to_thread(
                        temp_client.list_remote_entries_ftplib,
                        getattr(temp_client, "remote_channel_dir", "") or "/",
                        5000,
                    )
                    db.replace_remote_listing_cache(settings.channel, "FTPS", listing_rows)
                except Exception:
                    logging.exception("FTPS upload: failed to cache remote listing in DB")
                finally:
                    try:
                        await asyncio.to_thread(temp_client.close)
                    except Exception:
                        pass
            queue: asyncio.Queue[Path] = asyncio.Queue()
            for path in files:
                queue.put_nowait(path)

            sync_cls = type(remote_sync)

            async def _worker(worker_index: int) -> None:
                sync_client = sync_cls(settings)
                await asyncio.wait_for(asyncio.to_thread(sync_client.connect), timeout=30)
                await asyncio.wait_for(
                    asyncio.to_thread(sync_client.prepare_channel_dir, channel_folder),
                    timeout=30,
                )
                try:
                    while True:
                        if stop_requested and stop_requested():
                            return
                        try:
                            file_path = queue.get_nowait()
                        except asyncio.QueueEmpty:
                            return
                        transfer_id = f"upload:{worker_index}:{file_path.name}"
                        try:
                            await _run_upload_once(sync_client, file_path, transfer_id)
                        except asyncio.CancelledError:
                            return
                        except Exception as exc:
                            logging.exception(
                                "Upload failed transfer_id=%s file=%s", transfer_id, file_path
                            )
                            if status_hook:
                                status_hook(
                                    {
                                        "event": "sftp_failed",
                                        "message_id": transfer_id,
                                        "file_path": str(file_path),
                                        "reason": str(exc),
                                        "transport": transport,
                                    }
                                )
                        finally:
                            queue.task_done()
                finally:
                    await asyncio.to_thread(sync_client.close)

            workers = [asyncio.create_task(_worker(i + 1)) for i in range(concurrency)]
            await asyncio.gather(*workers, return_exceptions=True)

        if cleanup_local_after_remote and not (stop_requested and stop_requested()):
            sync_cls = type(remote_sync)
            verify_rounds = 2
            pending = [p for p in files if p.exists()]
            for round_index in range(1, verify_rounds + 1):
                if not pending:
                    break
                verify_client = sync_cls(settings)
                await asyncio.wait_for(asyncio.to_thread(verify_client.connect), timeout=30)
                await asyncio.wait_for(
                    asyncio.to_thread(verify_client.prepare_channel_dir, channel_folder),
                    timeout=30,
                )
                mismatches: list[Path] = []
                try:
                    for file_path in pending:
                        if stop_requested and stop_requested():
                            break
                        file_hash = file_hashes.get(str(file_path), "")
                        ok, verify_info = await asyncio.to_thread(
                            verify_client.check_remote_file_status,
                            str(file_path),
                            True,
                            file_hash,
                            db.get_remote_cached_path(settings.channel, "FTPS", file_path.name),
                        )
                        if ok:
                            try:
                                file_path.unlink()
                            except Exception:
                                logging.warning("Failed to remove local file after batch verify: %s", file_path)
                            path_key = str(file_path)
                            resolved_channel_id = int(file_channel_ids.get(path_key, 0))
                            if resolved_channel_id <= 0:
                                record = db.get_download_record_by_file_path(path_key)
                                if record:
                                    resolved_channel_id = int(record.get("channel_id") or 0)
                            if resolved_channel_id > 0:
                                db.delete_local_file_meta(resolved_channel_id, path_key)
                            if status_hook:
                                status_hook(
                                    {
                                        "event": "local_cleaned",
                                        "message_id": f"upload:{file_path.name}",
                                        "file_path": str(file_path),
                                        "transport": transport,
                                    }
                                )
                        else:
                            logging.warning(
                                "Batch verify mismatch round=%s file=%s info=%s",
                                round_index,
                                file_path,
                                verify_info,
                            )
                            mismatches.append(file_path)
                finally:
                    await asyncio.to_thread(verify_client.close)

                if not mismatches:
                    break
                if round_index >= verify_rounds:
                    for file_path in mismatches:
                        if status_hook:
                            status_hook(
                                {
                                    "event": "sftp_failed",
                                    "message_id": f"upload:{file_path.name}",
                                    "file_path": str(file_path),
                                    "reason": "Batch hash verify mismatch after retries",
                                    "transport": transport,
                                }
                            )
                    break

                retry_client = sync_cls(settings)
                await asyncio.wait_for(asyncio.to_thread(retry_client.connect), timeout=30)
                await asyncio.wait_for(
                    asyncio.to_thread(retry_client.prepare_channel_dir, channel_folder),
                    timeout=30,
                )
                try:
                    for file_path in mismatches:
                        if stop_requested and stop_requested():
                            break
                        transfer_id = f"upload:rehash:{file_path.name}"
                        await _run_upload_once(retry_client, file_path, transfer_id, True)
                finally:
                    await asyncio.to_thread(retry_client.close)
                pending = [p for p in mismatches if p.exists()]
    finally:
        db.close()


