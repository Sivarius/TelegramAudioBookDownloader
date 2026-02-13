import asyncio
import logging
from pathlib import Path
from typing import Callable, Optional

from telethon import events, utils
from telethon.errors import FloodWaitError
from telethon.tl.custom.message import Message

from core.config import sanitize_folder_name
from core.db import AppDatabase
from core.ftps_client import FTPSSync
from core.models import Settings
from core.sftp_client import SFTPSync
from core.telegram_client import create_telegram_client, is_audio_message, resolve_channel_entity


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
    channel_id = message.chat_id
    if channel_id is None:
        return

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


def _build_remote_sync(settings: Settings) -> Optional[object]:
    if settings.use_sftp:
        return SFTPSync(settings)
    if settings.use_ftps:
        return FTPSSync(settings)
    return None


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

        async def _run_upload_once(sync_client: object, file_path: Path, transfer_id: str) -> None:
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
                    )
                    break
                except Exception as exc:
                    if stop_requested and stop_requested():
                        raise asyncio.CancelledError() from exc
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
            if channel_id > 0:
                message_id = db.get_message_id_by_file_path(channel_id, str(file_path))
                if message_id > 0:
                    db.mark_remote_uploaded(channel_id, message_id, transport, remote_path)
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
            return

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
    finally:
        db.close()


async def initial_backfill(
    client,
    db: AppDatabase,
    channel,
    download_dir: Path,
    limit: int,
    allowed_message_ids: Optional[set[int]] = None,
    status_hook: Optional[Callable[[dict], None]] = None,
    min_id: int = 0,
    channel_ref: str = "",
    channel_title: str = "",
    stop_requested: Optional[Callable[[], bool]] = None,
    concurrency: int = 1,
    remote_sync: Optional[object] = None,
    cleanup_local_after_remote: bool = False,
) -> None:
    if limit == 0:
        return

    if min_id > 0:
        logging.info("Resuming channel from message_id > %s", min_id)
        iterator = client.iter_messages(channel, min_id=min_id, reverse=True, limit=None)
    else:
        logging.info("Startup scan: reading last %s messages...", limit)
        iterator = client.iter_messages(channel, limit=limit, reverse=True)

    concurrency_value = max(1, concurrency)
    semaphore = asyncio.Semaphore(concurrency_value)
    tasks: set[asyncio.Task] = set()

    async def _process_message(message: Message) -> None:
        nonlocal concurrency_value, semaphore
        while True:
            async with semaphore:
                try:
                    await download_if_needed(
                        message,
                        db,
                        download_dir,
                        status_hook=status_hook,
                        channel_ref=channel_ref,
                        channel_title=channel_title,
                        remote_sync=remote_sync,
                        cleanup_local_after_remote=cleanup_local_after_remote,
                    )
                    return
                except FloodWaitError as flood:
                    wait_seconds = max(1, int(getattr(flood, "seconds", 1)))
                    if concurrency_value > 1:
                        concurrency_value -= 1
                        semaphore = asyncio.Semaphore(max(1, concurrency_value))
                    if status_hook:
                        status_hook(
                            {
                                "event": "throttled",
                                "message_id": message.id,
                                "seconds": wait_seconds,
                                "concurrency": max(1, concurrency_value),
                            }
                        )
                    logging.warning(
                        "FloodWait %ss during backfill message_id=%s, concurrency=%s",
                        wait_seconds,
                        message.id,
                        max(1, concurrency_value),
                    )
            await asyncio.sleep(wait_seconds)
            if stop_requested and stop_requested():
                return

    async for message in iterator:
        if stop_requested and stop_requested():
            logging.info("Backfill interrupted by stop signal.")
            break
        if allowed_message_ids is not None and message.id not in allowed_message_ids:
            continue

        task = asyncio.create_task(_process_message(message))
        tasks.add(task)
        task.add_done_callback(tasks.discard)

        if len(tasks) >= max(4, concurrency_value * 4):
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for finished in done:
                exc = finished.exception()
                if exc:
                    logging.exception("Backfill task failed: %s", exc)

    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for item in results:
            if isinstance(item, Exception):
                logging.exception("Backfill task failed: %s", item)

    logging.info("Startup scan complete")


async def run_downloader(
    settings: Settings,
    db_path: Path,
    allowed_message_ids: Optional[set[int]] = None,
    status_hook: Optional[Callable[[dict], None]] = None,
    stop_requested: Optional[Callable[[], bool]] = None,
    live_mode: bool = True,
) -> None:
    db = AppDatabase(db_path)
    remote_sync: Optional[object] = _build_remote_sync(settings)
    client = None
    try:
        settings.download_dir.mkdir(parents=True, exist_ok=True)
        db.store_settings(settings)

        client = create_telegram_client(settings)
        await client.start(phone=settings.phone)

        try:
            channel = await resolve_channel_entity(client, settings.channel)
        except Exception as exc:
            raise RuntimeError(
                "Cannot resolve CHANNEL_ID. Check configured channel and account access."
            ) from exc

        logging.info("Connected. Watching channel: %s", settings.channel)
        channel_marked_id = utils.get_peer_id(channel)
        channel_title = getattr(channel, "title", settings.channel)
        channel_folder = sanitize_folder_name(channel_title)
        effective_download_dir = settings.download_dir / channel_folder
        effective_download_dir.mkdir(parents=True, exist_ok=True)
        if remote_sync and getattr(remote_sync, "enabled", False):
            try:
                await asyncio.to_thread(remote_sync.connect)
                await asyncio.to_thread(remote_sync.prepare_channel_dir, channel_folder)
                transport = str(getattr(remote_sync, "name", "REMOTE"))
                if status_hook:
                    status_hook(
                        {
                            "event": "sftp_ready",
                            "message": f"{transport} готов: {getattr(remote_sync, 'remote_channel_dir', '')}",
                            "transport": transport,
                        }
                    )
            except Exception as sftp_exc:
                raise RuntimeError(f"Remote upload setup failed: {sftp_exc}") from sftp_exc

        last_downloaded_id = 0
        if allowed_message_ids is None:
            last_downloaded_id = db.get_last_downloaded_message_id(channel_marked_id)
            if last_downloaded_id > 0:
                logging.info(
                    "Found previous progress for channel_id=%s last_message_id=%s",
                    channel_marked_id,
                    last_downloaded_id,
                )

        await initial_backfill(
            client=client,
            db=db,
            channel=channel,
            download_dir=effective_download_dir,
            limit=settings.startup_scan_limit,
            allowed_message_ids=allowed_message_ids,
            status_hook=status_hook,
            min_id=last_downloaded_id,
            channel_ref=settings.channel,
            channel_title=channel_title,
            stop_requested=stop_requested,
            concurrency=settings.download_concurrency,
            remote_sync=remote_sync,
            cleanup_local_after_remote=(
                settings.cleanup_local_after_sftp if settings.use_sftp else settings.cleanup_local_after_ftps
            ),
        )

        if not live_mode:
            logging.info("One-shot mode complete. Live listener disabled.")
            return

        live_concurrency = max(1, settings.download_concurrency)
        download_semaphore = asyncio.Semaphore(live_concurrency)
        live_tasks: set[asyncio.Task] = set()

        async def _process_live_message(message: Message) -> None:
            nonlocal download_semaphore, live_concurrency
            while True:
                async with download_semaphore:
                    try:
                        await download_if_needed(
                            message,
                            db,
                            effective_download_dir,
                            status_hook=status_hook,
                            channel_ref=settings.channel,
                            channel_title=channel_title,
                            remote_sync=remote_sync,
                            cleanup_local_after_remote=(
                                settings.cleanup_local_after_sftp
                                if settings.use_sftp
                                else settings.cleanup_local_after_ftps
                            ),
                        )
                        return
                    except FloodWaitError as flood:
                        wait_seconds = max(1, int(getattr(flood, "seconds", 1)))
                        if live_concurrency > 1:
                            live_concurrency -= 1
                            download_semaphore = asyncio.Semaphore(max(1, live_concurrency))
                        if status_hook:
                            status_hook(
                                {
                                    "event": "throttled",
                                    "message_id": message.id,
                                    "seconds": wait_seconds,
                                    "concurrency": live_concurrency,
                                }
                            )
                        logging.warning(
                            "FloodWait %ss in live mode message_id=%s, concurrency=%s",
                            wait_seconds,
                            message.id,
                            live_concurrency,
                        )
                await asyncio.sleep(wait_seconds)
                if stop_requested and stop_requested():
                    return

        @client.on(events.NewMessage(chats=channel))
        async def on_new_message(event: events.NewMessage.Event) -> None:
            task = asyncio.create_task(_process_live_message(event.message))
            live_tasks.add(task)
            task.add_done_callback(live_tasks.discard)

        logging.info("Listener started. Waiting for new audio files...")
        listener_task = asyncio.create_task(client.run_until_disconnected())
        try:
            while not listener_task.done():
                if stop_requested and stop_requested():
                    logging.info("Stop signal received. Disconnecting Telegram client...")
                    await client.disconnect()
                    break
                await asyncio.sleep(0.5)
            await listener_task
            if live_tasks:
                await asyncio.gather(*live_tasks, return_exceptions=True)
        finally:
            if not listener_task.done():
                listener_task.cancel()
    finally:
        if client is not None:
            try:
                await client.disconnect()
            except Exception:
                logging.exception("Failed to disconnect Telegram client")
        if remote_sync and getattr(remote_sync, "enabled", False):
            try:
                await asyncio.to_thread(remote_sync.close)
            except Exception:
                logging.exception("Failed to close remote upload client")
        db.close()
