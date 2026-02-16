import asyncio
import logging
import sqlite3
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

from telethon import utils

from core.db import AppDatabase
from core.models import Settings
from core.telegram_client import create_telegram_client, is_audio_message, resolve_channel_entity


def upsert_current_channel_preference(settings: Settings, db_path: Path) -> None:
    channel_ref = (settings.channel or "").strip()
    if not channel_ref or channel_ref == "_":
        return
    db = AppDatabase(db_path)
    try:
        existing = db.get_channel_preferences(channel_ref)
        db.upsert_channel_preferences(
            channel_ref=channel_ref,
            channel_id=int(existing.get("channel_id", 0)) if existing else 0,
            channel_title=(existing.get("channel_title") or channel_ref) if existing else channel_ref,
            check_new=bool(existing.get("check_new")) if existing else False,
            auto_download=bool(existing.get("auto_download")) if existing else False,
            auto_sftp=bool(existing.get("auto_sftp")) if existing else False,
            auto_ftps=bool(existing.get("auto_ftps")) if existing else False,
            cleanup_local=(
                bool(existing.get("cleanup_local"))
                if existing
                else bool(settings.cleanup_local_after_sftp or settings.cleanup_local_after_ftps)
            ),
        )
    finally:
        db.close()


def parse_db_time(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def is_check_due(last_checked_at: str, auto_check_interval_seconds: int) -> bool:
    last_time = parse_db_time(last_checked_at)
    if not last_time:
        return True
    return (datetime.now() - last_time).total_seconds() >= auto_check_interval_seconds


def saved_channels_fallback(saved_channels: list[dict], message: str) -> tuple[bool, str, list[dict]]:
    items: list[dict] = []
    for saved in saved_channels:
        channel_ref = (saved.get("channel_ref") or "").strip() or str(saved.get("channel_id", 0))
        channel_title = saved.get("channel_title") or channel_ref
        has_new_audio = bool(saved.get("has_new_audio"))
        status = "Есть новые аудио" if has_new_audio else "Нет новых аудио"
        items.append(
            {
                "channel_id": int(saved.get("channel_id") or 0),
                "channel_ref": channel_ref,
                "channel_title": channel_title,
                "check_new": bool(saved.get("check_new")),
                "auto_download": bool(saved.get("auto_download")),
                "auto_sftp": bool(saved.get("auto_sftp")),
                "auto_ftps": bool(saved.get("auto_ftps")),
                "cleanup_local": bool(saved.get("cleanup_local")),
                "last_checked_at": saved.get("last_checked_at") or "",
                "latest_audio_id": int(saved.get("latest_audio_id") or 0),
                "last_message_id": 0,
                "last_file_path": "",
                "has_new_audio": has_new_audio,
                "status": status,
                "last_error": saved.get("last_error") or "",
                "updated_at": saved.get("updated_at") or "",
            }
        )
    return True, message, items


def collect_saved_channels_cached(db_path: Path) -> tuple[bool, str, list[dict]]:
    db = AppDatabase(db_path)
    try:
        saved_channels = db.list_channel_preferences()
        if not saved_channels:
            for state in db.list_channel_states():
                db.ensure_channel_preferences(
                    int(state.get("channel_id") or 0),
                    state.get("channel_ref") or str(state.get("channel_id") or ""),
                    state.get("channel_title") or "",
                )
            saved_channels = db.list_channel_preferences()

        if not saved_channels:
            return True, "Сохраненные каналы отсутствуют.", []

        items: list[dict] = []
        for saved in saved_channels:
            channel_ref = (saved.get("channel_ref") or "").strip() or str(saved.get("channel_id", 0))
            channel_title = saved.get("channel_title") or channel_ref
            has_new_audio = bool(saved.get("has_new_audio"))
            status = "Есть новые аудио" if has_new_audio else "Нет новых аудио"
            state = db.get_channel_state_by_ref(channel_ref)
            items.append(
                {
                    "channel_id": int(saved.get("channel_id") or 0),
                    "channel_ref": channel_ref,
                    "channel_title": channel_title,
                    "check_new": bool(saved.get("check_new")),
                    "auto_download": bool(saved.get("auto_download")),
                    "auto_sftp": bool(saved.get("auto_sftp")),
                    "auto_ftps": bool(saved.get("auto_ftps")),
                    "cleanup_local": bool(saved.get("cleanup_local")),
                    "last_checked_at": saved.get("last_checked_at") or "",
                    "latest_audio_id": int(saved.get("latest_audio_id") or 0),
                    "last_message_id": int(state.get("last_message_id") or 0),
                    "last_file_path": state.get("last_file_path") or "",
                    "has_new_audio": has_new_audio,
                    "status": status,
                    "last_error": saved.get("last_error") or "",
                    "updated_at": saved.get("updated_at") or "",
                }
            )
        return True, f"Каналов в истории: {len(items)} (кэш)", items
    finally:
        db.close()


async def latest_audio_message_id(client, channel, scan_limit: int = 200) -> int:
    async for message in client.iter_messages(channel, limit=scan_limit):
        if is_audio_message(message):
            return int(message.id)
    return 0


async def collect_saved_channels_status(
    settings: Settings,
    db_path: Path,
    worker_busy: Callable[[], bool],
    auto_check_interval_seconds: int,
    only_due: bool = False,
) -> tuple[bool, str, list[dict]]:
    db = AppDatabase(db_path)
    try:
        saved_channels = db.list_channel_preferences()
        if not saved_channels:
            for state in db.list_channel_states():
                db.ensure_channel_preferences(
                    int(state.get("channel_id") or 0),
                    state.get("channel_ref") or str(state.get("channel_id") or ""),
                    state.get("channel_title") or "",
                )
            saved_channels = db.list_channel_preferences()
    finally:
        db.close()

    if not saved_channels:
        return True, "Сохраненные каналы отсутствуют.", []

    if worker_busy():
        return saved_channels_fallback(
            saved_channels,
            "Проверка каналов отложена: идёт активное скачивание.",
        )

    client = create_telegram_client(settings)
    try:
        await client.connect()
    except sqlite3.OperationalError as exc:
        if "database is locked" in str(exc).lower():
            return saved_channels_fallback(
                saved_channels,
                "Проверка каналов временно недоступна: Telethon session занята активной загрузкой.",
            )
        raise
    try:
        if not await client.is_user_authorized():
            items = []
            for item in saved_channels:
                items.append(
                    {
                        **item,
                        "channel_ref": item["channel_ref"] or str(item.get("channel_id", 0)),
                        "status": "Требуется авторизация",
                        "has_new_audio": False,
                    }
                )
            return False, "Сессия не авторизована. Сначала нажмите Авторизоваться.", items

        items: list[dict] = []
        for saved in saved_channels:
            channel_ref = (saved.get("channel_ref") or "").strip() or str(saved.get("channel_id", 0))
            if only_due and not is_check_due(saved.get("last_checked_at", ""), auto_check_interval_seconds):
                continue
            channel_title = saved.get("channel_title") or channel_ref
            has_new_audio = bool(saved.get("has_new_audio", False))
            status = "Нет новых аудио"
            latest_audio_id = int(saved.get("latest_audio_id") or 0)
            error_text = ""
            last_state = {"last_message_id": 0, "last_file_path": ""}
            db_state = AppDatabase(db_path)
            try:
                last_state = db_state.get_channel_state_by_ref(channel_ref)
            finally:
                db_state.close()
            try:
                entity = await resolve_channel_entity(client, channel_ref)
                marked_channel_id = utils.get_peer_id(entity)
                latest_audio_id = await latest_audio_message_id(client, entity)
                db_inner = AppDatabase(db_path)
                try:
                    last_message_id = db_inner.get_last_message_id_by_channel_ref(channel_ref)
                finally:
                    db_inner.close()
                has_new_audio = latest_audio_id > last_message_id if latest_audio_id > 0 else False
                status = "Есть новые аудио" if has_new_audio else "Нет новых аудио"
                channel_title = getattr(entity, "title", channel_title)
                canonical_ref = str(marked_channel_id)
                db_inner = AppDatabase(db_path)
                try:
                    db_inner.upsert_channel_preferences(
                        channel_ref=canonical_ref,
                        channel_id=marked_channel_id,
                        channel_title=channel_title,
                        check_new=bool(saved.get("check_new")),
                        auto_download=bool(saved.get("auto_download")),
                        auto_sftp=bool(saved.get("auto_sftp")),
                        auto_ftps=bool(saved.get("auto_ftps")),
                        cleanup_local=bool(saved.get("cleanup_local")),
                    )
                    db_inner.update_channel_check_status(
                        channel_ref=canonical_ref,
                        has_new_audio=has_new_audio,
                        latest_audio_id=latest_audio_id,
                        last_error="",
                    )
                finally:
                    db_inner.close()
                channel_ref = canonical_ref
            except Exception as exc:
                status = f"Ошибка проверки: {exc}"
                error_text = str(exc)
                db_inner = AppDatabase(db_path)
                try:
                    db_inner.update_channel_check_status(
                        channel_ref=channel_ref,
                        has_new_audio=False,
                        latest_audio_id=0,
                        last_error=error_text,
                    )
                finally:
                    db_inner.close()
            items.append(
                {
                    "channel_id": int(saved.get("channel_id") or 0),
                    "channel_ref": channel_ref,
                    "channel_title": channel_title,
                    "check_new": bool(saved.get("check_new")),
                    "auto_download": bool(saved.get("auto_download")),
                    "auto_sftp": bool(saved.get("auto_sftp")),
                    "auto_ftps": bool(saved.get("auto_ftps")),
                    "cleanup_local": bool(saved.get("cleanup_local")),
                    "last_checked_at": saved.get("last_checked_at") or "",
                    "latest_audio_id": latest_audio_id,
                    "last_message_id": int(last_state.get("last_message_id") or 0),
                    "last_file_path": last_state.get("last_file_path") or "",
                    "has_new_audio": has_new_audio,
                    "status": status,
                    "last_error": error_text or (saved.get("last_error") or ""),
                    "updated_at": saved.get("updated_at") or "",
                }
            )

        return True, f"Проверено каналов: {len(items)}", items
    finally:
        await client.disconnect()


def run_periodic_checks_once(
    db_path: Path,
    is_periodic_checks_enabled: Callable[[], bool],
    worker_busy: Callable[[], bool],
    load_saved_form: Callable[[], dict],
    build_settings: Callable[..., Settings],
    collect_saved_channels_status_cb: Callable[..., tuple[bool, str, list[dict]]],
    start_worker: Callable[..., tuple[bool, str]],
) -> None:
    try:
        if not is_periodic_checks_enabled():
            return
        if worker_busy():
            return
        form = load_saved_form()
        if not form.get("api_id") or not form.get("api_hash") or not form.get("phone"):
            return
        settings = build_settings(form, require_channel=False)
        ok, _, channels = asyncio.run(collect_saved_channels_status_cb(settings, only_due=True))
        if not ok:
            return
        for item in channels:
            if not item.get("check_new"):
                continue
            if not item.get("has_new_audio"):
                continue
            if not item.get("auto_download"):
                continue
            if worker_busy():
                logging.info("Auto-download skipped: worker is busy")
                continue
            channel_ref = str(item.get("channel_ref") or "").strip()
            if not channel_ref:
                continue
            run_settings = replace(
                settings,
                channel=channel_ref,
                use_sftp=bool(settings.use_sftp and item.get("auto_sftp")),
                use_ftps=bool(settings.use_ftps and item.get("auto_ftps")),
                cleanup_local_after_sftp=bool(item.get("cleanup_local")),
                cleanup_local_after_ftps=bool(item.get("cleanup_local")),
            )
            started, msg = start_worker(run_settings, None, live_mode=False, source="auto")
            logging.info("Auto-download for %s: %s", channel_ref, msg)
            if started:
                break
    except Exception:
        logging.exception("Periodic check run failed")
