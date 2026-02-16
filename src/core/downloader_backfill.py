import asyncio
import logging
from pathlib import Path
from typing import Callable, Optional

from telethon.errors import FloodWaitError
from telethon.tl.custom.message import Message

from core.db import AppDatabase
from core.downloader_transfer import download_if_needed

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


