import asyncio
import logging
from dataclasses import replace
from pathlib import Path

from core.config import load_settings, setup_logging
from core.db import AppDatabase
from core.downloader import run_downloader
from core.models import Settings
from core.telegram_client import create_telegram_client, is_audio_message, resolve_channel_entity


def prompt_download_dir(default_path: Path) -> Path:
    prompt = f"Enter directory to save audio files [{default_path}]: "
    user_input = input(prompt).strip()
    selected = Path(user_input).expanduser() if user_input else default_path
    return selected.resolve()


async def run() -> None:
    setup_logging()

    db_path = Path("bot_data.sqlite3")
    db = AppDatabase(db_path)
    try:
        settings = load_settings(db)
        selected_download_dir = prompt_download_dir(settings.download_dir)
        settings = replace(settings, download_dir=selected_download_dir)
    finally:
        db.close()

    await run_downloader(settings, db_path)


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logging.info("Stopped by user")
    except Exception as exc:
        logging.error(str(exc))
        raise
