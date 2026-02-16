import asyncio
import logging
from pathlib import Path
from typing import Callable, Optional

from telethon import events, utils

from core.config import sanitize_folder_name
from core.db import AppDatabase
from core.models import Settings
from core.telegram_client import create_telegram_client, resolve_channel_entity
from core.downloader_backfill import initial_backfill
from core.downloader_transfer import download_if_needed
from core.downloader_upload import run_remote_uploader
from core.downloader_utils import _build_remote_sync

async def run_downloader(
    settings: Settings,
    db_path: Path,
    allowed_message_ids: Optional[set[int]] = None,
    status_hook: Optional[Callable[[dict], None]] = None,
    stop_requested: Optional[Callable[[], bool]] = None,
    live_mode: bool = True,
) -> None:
    db = AppDatabase(db_path)
    defer_ftps_batch_upload = bool(settings.use_ftps and not settings.use_sftp and not live_mode)
    remote_sync: Optional[object] = None if defer_ftps_batch_upload else _build_remote_sync(settings)
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
            if defer_ftps_batch_upload:
                if status_hook:
                    status_hook(
                        {
                            "event": "upload_started",
                            "message": "FTPS: запуск пакетной автозагрузки после скачивания.",
                            "concurrency": max(1, int(getattr(settings, "ftps_upload_concurrency", 1))),
                        }
                    )
                await run_remote_uploader(
                    settings=settings,
                    db_path=db_path,
                    status_hook=status_hook,
                    stop_requested=stop_requested,
                )
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
