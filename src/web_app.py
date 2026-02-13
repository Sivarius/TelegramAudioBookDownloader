import asyncio
from collections import deque
import logging
import os
import sqlite3
import threading
import time
import webbrowser
from datetime import datetime
from dataclasses import replace
from pathlib import Path
from typing import Optional

from flask import Flask, render_template, request
from telethon import utils
from telethon.errors import PhoneCodeExpiredError, PhoneCodeInvalidError, SessionPasswordNeededError

from core.config import setup_logging
from core.db import AppDatabase
from core.downloader import run_downloader, run_remote_uploader
from core.models import Settings
from core.telegram_client import create_telegram_client, is_audio_message, resolve_channel_entity
from web_form_ops import (
    _build_settings,
    _clear_phone_code_hash as _clear_phone_code_hash_base,
    _delete_session_files,
    _form_from_request as _form_from_request_base,
    _is_periodic_checks_enabled as _is_periodic_checks_enabled_base,
    _load_phone_code_hash as _load_phone_code_hash_base,
    _load_saved_form as _load_saved_form_base,
    _safe_int,
    _save_phone_code_hash as _save_phone_code_hash_base,
    _store_enable_periodic_checks as _store_enable_periodic_checks_base,
    _store_remember_me as _store_remember_me_base,
    _store_settings as _store_settings_base,
    _validate_ftps,
    _validate_proxy,
    _validate_sftp,
)
from web_routes_basic import register_basic_routes


DB_PATH = Path(os.getenv("DB_PATH", "bot_data.sqlite3"))
HOST = os.getenv("APP_HOST", "127.0.0.1")
PORT = int(os.getenv("APP_PORT", "8080"))
OPEN_BROWSER = os.getenv("OPEN_BROWSER", "1").strip().lower() in {"1", "true", "yes", "on"}
PREVIEW_LIMIT = 300
AUTO_CHECK_INTERVAL_SECONDS = int(os.getenv("AUTO_CHECK_INTERVAL_SECONDS", "7200"))

app = Flask(__name__)
setup_logging()


debug_log_lock = threading.Lock()
debug_log_buffer: deque[str] = deque(maxlen=400)


class InMemoryLogHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
        except Exception:
            msg = record.getMessage()
        with debug_log_lock:
            debug_log_buffer.append(msg)


def _setup_debug_log_handler() -> None:
    root = logging.getLogger()
    if any(isinstance(h, InMemoryLogHandler) for h in root.handlers):
        return
    handler = InMemoryLogHandler()
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    root.addHandler(handler)


_setup_debug_log_handler()

worker_lock = threading.Lock()
worker_thread: Optional[threading.Thread] = None
worker_stop_event = threading.Event()
worker_loop: Optional[asyncio.AbstractEventLoop] = None
worker_main_task: Optional[asyncio.Task] = None
monitor_thread: Optional[threading.Thread] = None
monitor_stop_event = threading.Event()
runtime_session_name = ""
runtime_remember_me = True
status_lock = threading.Lock()
status_cond = threading.Condition(status_lock)
status_version = 0
worker_status = {
    "running": False,
    "message": "Ожидание запуска.",
    "downloaded": 0,
    "failed": 0,
    "skipped": 0,
    "current_message_id": "",
    "last_file": "",
    "updated_at": "",
    "configured_concurrency": 1,
    "current_concurrency": 1,
    "sftp_uploaded": 0,
    "sftp_skipped": 0,
    "sftp_failed": 0,
    "progress_percent": 0,
    "progress_received": 0,
    "progress_total": 0,
    "upload_progress_percent": 0,
    "upload_progress_received": 0,
    "upload_progress_total": 0,
    "upload_progress_speed_bps": 0.0,
    "upload_progress_eta_sec": 0.0,
    "file_progresses": {},
    "upload_file_progresses": {},
    "mode": "idle",
}
preview_cache: list[dict] = []
auth_status = {
    "authorized": False,
    "message": "Авторизация не выполнялась.",
    "updated_at": "",
}
proxy_status = {
    "enabled": False,
    "available": False,
    "message": "MTProxy не используется.",
    "updated_at": "",
}
sftp_status = {
    "enabled": False,
    "available": False,
    "message": "SFTP не используется.",
    "updated_at": "",
}
ftps_status = {
    "enabled": False,
    "available": False,
    "message": "FTPS не используется.",
    "updated_at": "",
}


def _set_status(**kwargs) -> None:
    global status_version
    worker_status.update(kwargs)
    worker_status["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with status_cond:
        status_version += 1
        status_cond.notify_all()


def _set_auth_status(authorized: bool, message: str) -> None:
    global status_version
    auth_status["authorized"] = authorized
    auth_status["message"] = message
    auth_status["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with status_cond:
        status_version += 1
        status_cond.notify_all()


def _set_proxy_status(enabled: bool, available: bool, message: str) -> None:
    global status_version
    proxy_status["enabled"] = enabled
    proxy_status["available"] = available
    proxy_status["message"] = message
    proxy_status["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with status_cond:
        status_version += 1
        status_cond.notify_all()


def _set_sftp_status(enabled: bool, available: bool, message: str) -> None:
    global status_version
    sftp_status["enabled"] = enabled
    sftp_status["available"] = available
    sftp_status["message"] = message
    sftp_status["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with status_cond:
        status_version += 1
        status_cond.notify_all()


def _set_ftps_status(enabled: bool, available: bool, message: str) -> None:
    global status_version
    ftps_status["enabled"] = enabled
    ftps_status["available"] = available
    ftps_status["message"] = message
    ftps_status["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with status_cond:
        status_version += 1
        status_cond.notify_all()


def _status_payload() -> dict:
    return {
        **worker_status,
        "auth_authorized": auth_status["authorized"],
        "auth_message": auth_status["message"],
        "auth_updated_at": auth_status["updated_at"],
        "proxy_enabled": proxy_status["enabled"],
        "proxy_available": proxy_status["available"],
        "proxy_message": proxy_status["message"],
        "proxy_updated_at": proxy_status["updated_at"],
        "sftp_enabled": sftp_status["enabled"],
        "sftp_available": sftp_status["available"],
        "sftp_message": sftp_status["message"],
        "sftp_updated_at": sftp_status["updated_at"],
        "ftps_enabled": ftps_status["enabled"],
        "ftps_available": ftps_status["available"],
        "ftps_message": ftps_status["message"],
        "ftps_updated_at": ftps_status["updated_at"],
    }


def _load_saved_form() -> dict:
    return _load_saved_form_base(DB_PATH)


def _form_from_request() -> dict:
    return _form_from_request_base(DB_PATH)


def _store_settings(settings: Settings) -> None:
    _store_settings_base(DB_PATH, settings)


def _store_remember_me(remember_me: bool) -> None:
    _store_remember_me_base(DB_PATH, remember_me)


def _store_enable_periodic_checks(enabled: bool) -> None:
    _store_enable_periodic_checks_base(DB_PATH, enabled)


def _is_periodic_checks_enabled() -> bool:
    return _is_periodic_checks_enabled_base(DB_PATH)


def _save_phone_code_hash(phone: str, phone_code_hash: str) -> None:
    _save_phone_code_hash_base(DB_PATH, phone, phone_code_hash)


def _load_phone_code_hash(phone: str) -> str:
    return _load_phone_code_hash_base(DB_PATH, phone)


def _clear_phone_code_hash() -> None:
    _clear_phone_code_hash_base(DB_PATH)


async def _latest_audio_message_id(client, channel, scan_limit: int = 200) -> int:
    async for message in client.iter_messages(channel, limit=scan_limit):
        if is_audio_message(message):
            return int(message.id)
    return 0


def _upsert_current_channel_preference(settings: Settings) -> None:
    channel_ref = (settings.channel or "").strip()
    if not channel_ref or channel_ref == "_":
        return
    db = AppDatabase(DB_PATH)
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


def _parse_db_time(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def _is_check_due(last_checked_at: str) -> bool:
    last_time = _parse_db_time(last_checked_at)
    if not last_time:
        return True
    return (datetime.now() - last_time).total_seconds() >= AUTO_CHECK_INTERVAL_SECONDS


def _saved_channels_fallback(saved_channels: list[dict], message: str) -> tuple[bool, str, list[dict]]:
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


def _collect_saved_channels_cached() -> tuple[bool, str, list[dict]]:
    db = AppDatabase(DB_PATH)
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


async def _collect_saved_channels_status(
    settings: Settings, only_due: bool = False
) -> tuple[bool, str, list[dict]]:
    db = AppDatabase(DB_PATH)
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

    if worker_thread and worker_thread.is_alive():
        return _saved_channels_fallback(
            saved_channels,
            "Проверка каналов отложена: идёт активное скачивание.",
        )

    client = create_telegram_client(settings)
    try:
        await client.connect()
    except sqlite3.OperationalError as exc:
        if "database is locked" in str(exc).lower():
            return _saved_channels_fallback(
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
            if only_due and not _is_check_due(saved.get("last_checked_at", "")):
                continue
            channel_title = saved.get("channel_title") or channel_ref
            has_new_audio = bool(saved.get("has_new_audio", False))
            status = "Нет новых аудио"
            latest_audio_id = int(saved.get("latest_audio_id") or 0)
            error_text = ""
            last_state = {"last_message_id": 0, "last_file_path": ""}
            db_state = AppDatabase(DB_PATH)
            try:
                last_state = db_state.get_channel_state_by_ref(channel_ref)
            finally:
                db_state.close()
            try:
                entity = await resolve_channel_entity(client, channel_ref)
                marked_channel_id = utils.get_peer_id(entity)
                latest_audio_id = await _latest_audio_message_id(client, entity)
                db_inner = AppDatabase(DB_PATH)
                try:
                    last_message_id = db_inner.get_last_message_id_by_channel_ref(channel_ref)
                finally:
                    db_inner.close()
                has_new_audio = latest_audio_id > last_message_id if latest_audio_id > 0 else False
                status = "Есть новые аудио" if has_new_audio else "Нет новых аудио"
                channel_title = getattr(entity, "title", channel_title)
                canonical_ref = str(marked_channel_id)
                db_inner = AppDatabase(DB_PATH)
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
                db_inner = AppDatabase(DB_PATH)
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


def _start_monitor_thread() -> None:
    global monitor_thread
    if monitor_thread and monitor_thread.is_alive():
        return
    monitor_stop_event.clear()

    def _monitor_target() -> None:
        while not monitor_stop_event.is_set():
            try:
                _run_periodic_checks_once()
            except Exception:
                logging.exception("Periodic monitor loop error")
            for _ in range(60):
                if monitor_stop_event.is_set():
                    break
                time.sleep(1)

    monitor_thread = threading.Thread(target=_monitor_target, daemon=True)
    monitor_thread.start()


def _run_periodic_checks_once() -> None:
    try:
        if not _is_periodic_checks_enabled():
            return
        if worker_thread and worker_thread.is_alive():
            return
        form = _load_saved_form()
        if not form.get("api_id") or not form.get("api_hash") or not form.get("phone"):
            return
        settings = _build_settings(form, require_channel=False)
        ok, _, channels = asyncio.run(_collect_saved_channels_status(settings, only_due=True))
        if not ok:
            return
        for item in channels:
            if not item.get("check_new"):
                continue
            if not item.get("has_new_audio"):
                continue
            if not item.get("auto_download"):
                continue
            if worker_thread and worker_thread.is_alive():
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
            started, msg = _start_worker(run_settings, None, live_mode=False, source="auto")
            logging.info("Auto-download for %s: %s", channel_ref, msg)
            if started:
                break
    except Exception:
        logging.exception("Periodic check run failed")


async def _authorize_user(settings: Settings, code: str, password: str) -> tuple[bool, str, bool, bool]:
    client = create_telegram_client(settings)
    await client.connect()

    try:
        if await client.is_user_authorized():
            _clear_phone_code_hash()
            return True, "Учетная запись уже авторизована.", False, False

        if not code:
            sent = await client.send_code_request(settings.phone)
            if getattr(sent, "phone_code_hash", None):
                _save_phone_code_hash(settings.phone, str(sent.phone_code_hash))
            return False, "Код отправлен в Telegram. Введите код и нажмите Авторизоваться снова.", True, False

        code_hash = _load_phone_code_hash(settings.phone)
        if not code_hash:
            return False, "Не найден phone_code_hash. Нажмите Авторизоваться без кода для повторной отправки.", True, False

        try:
            await client.sign_in(phone=settings.phone, code=code, phone_code_hash=code_hash)
            _clear_phone_code_hash()
            return True, "Авторизация прошла успешно.", False, False
        except PhoneCodeExpiredError:
            _clear_phone_code_hash()
            return False, "Код устарел. Нажмите Авторизоваться без кода для новой отправки.", True, False
        except PhoneCodeInvalidError:
            return False, "Неверный код. Проверьте код из Telegram и попробуйте снова.", True, False
        except SessionPasswordNeededError:
            if not password:
                return False, "Для аккаунта включен пароль 2FA. Введите пароль.", False, True
            await client.sign_in(password=password)
            _clear_phone_code_hash()
            return True, "Авторизация с 2FA прошла успешно.", False, False
    finally:
        await client.disconnect()


async def _validate_channel(settings: Settings) -> tuple[bool, str]:
    client = create_telegram_client(settings)
    await client.connect()

    try:
        if not await client.is_user_authorized():
            return False, "Сессия не авторизована. Сначала нажмите Авторизоваться."

        await resolve_channel_entity(client, settings.channel)
        return True, "Канал доступен."
    except Exception:
        return False, "Не удалось найти канал. Проверьте CHANNEL_ID и доступ аккаунта."
    finally:
        await client.disconnect()


async def _fetch_preview(settings: Settings, limit: int = PREVIEW_LIMIT) -> tuple[bool, str, list[dict]]:
    client = create_telegram_client(settings)
    await client.connect()

    try:
        if not await client.is_user_authorized():
            return False, "Сначала выполните авторизацию.", []

        channel = await resolve_channel_entity(client, settings.channel)
        channel_id = utils.get_peer_id(channel)
        db = AppDatabase(DB_PATH)

        items: list[dict] = []
        index = 1
        try:
            async for message in client.iter_messages(channel, limit=limit, reverse=True):
                if not is_audio_message(message):
                    continue

                file_name = ""
                if message.file and getattr(message.file, "name", None):
                    file_name = str(message.file.name)

                title = file_name or (message.message or "audio")
                date_text = message.date.strftime("%Y-%m-%d %H:%M") if message.date else ""
                is_downloaded = db.already_downloaded(channel_id, int(message.id))
                items.append(
                    {
                        "index": index,
                        "message_id": message.id,
                        "title": title.replace("\n", " ")[:80],
                        "date": date_text,
                        "downloaded": is_downloaded,
                        "remote_uploaded": db.is_remote_uploaded(channel_id, int(message.id)),
                    }
                )
                index += 1
        finally:
            db.close()

        return True, f"Найдено аудио: {len(items)}", items
    except Exception as exc:
        return False, f"Ошибка предпросмотра: {exc}", []
    finally:
        await client.disconnect()


async def _resolve_last_downloaded_message_id(settings: Settings) -> int:
    client = create_telegram_client(settings)
    await client.connect()
    try:
        channel = await resolve_channel_entity(client, settings.channel)
        channel_id = utils.get_peer_id(channel)
        db = AppDatabase(DB_PATH)
        try:
            return db.get_last_downloaded_message_id(channel_id)
        finally:
            db.close()
    finally:
        await client.disconnect()


def _pick_range_ids(items: list[dict], from_index_raw: str, to_index_raw: str) -> Optional[set[int]]:
    if not from_index_raw and not to_index_raw:
        return None

    if not items:
        return set()

    from_index = _safe_int(from_index_raw, 1)
    to_index = _safe_int(to_index_raw, len(items))

    if from_index <= 0:
        from_index = 1
    if to_index <= 0:
        to_index = len(items)
    if from_index > to_index:
        from_index, to_index = to_index, from_index

    selected = {int(i["message_id"]) for i in items if from_index <= int(i["index"]) <= to_index}
    return selected


def _start_worker(
    settings: Settings,
    allowed_message_ids: Optional[set[int]],
    live_mode: bool = True,
    source: str = "manual",
    upload_only: bool = False,
) -> tuple[bool, str]:
    global worker_thread, worker_loop, worker_main_task

    with worker_lock:
        if worker_thread and worker_thread.is_alive():
            return False, "Скачивание уже запущено."

        configured_concurrency = (
            max(1, int(getattr(settings, "ftps_upload_concurrency", 1)))
            if upload_only
            else settings.download_concurrency
        )
        worker_stop_event.clear()
        _set_status(
            running=True,
            message=("Upload запущен" if upload_only else "Скачивание запущено") + f" ({source}).",
            downloaded=0,
            failed=0,
            skipped=0,
            sftp_uploaded=0,
            sftp_skipped=0,
            sftp_failed=0,
            progress_percent=0,
            progress_received=0,
            progress_total=0,
            upload_progress_percent=0,
            upload_progress_received=0,
            upload_progress_total=0,
            upload_progress_speed_bps=0.0,
            upload_progress_eta_sec=0.0,
            file_progresses={},
            upload_file_progresses={},
            current_message_id="",
            last_file="",
            configured_concurrency=configured_concurrency,
            current_concurrency=configured_concurrency,
            mode="upload" if upload_only else "download",
        )

        def _update_progress_item(
            message_id: str,
            *,
            percent: Optional[int] = None,
            received: Optional[int] = None,
            total: Optional[int] = None,
            state: Optional[str] = None,
            file_path: Optional[str] = None,
            speed_bps: Optional[float] = None,
            eta_sec: Optional[float] = None,
            error: Optional[str] = None,
        ) -> None:
            current = dict(worker_status.get("file_progresses", {}))
            item = dict(current.get(message_id, {}))
            if percent is not None:
                item["percent"] = int(percent)
            if received is not None:
                item["received"] = int(received)
            if total is not None:
                item["total"] = int(total)
            if state is not None:
                item["state"] = state
            if file_path:
                item["file_path"] = file_path
            if speed_bps is not None:
                item["speed_bps"] = float(speed_bps)
            if eta_sec is not None:
                item["eta_sec"] = float(eta_sec)
            if error is not None:
                item["error"] = str(error)
            item["updated_at"] = datetime.now().strftime("%H:%M:%S")
            current[message_id] = item
            if len(current) > 40:
                keys = list(current.keys())
                for key in keys[:-40]:
                    current.pop(key, None)
            _set_status(file_progresses=current)

        def _update_upload_progress_item(
            transfer_id: str,
            *,
            percent: Optional[int] = None,
            received: Optional[int] = None,
            total: Optional[int] = None,
            state: Optional[str] = None,
            file_path: Optional[str] = None,
            speed_bps: Optional[float] = None,
            eta_sec: Optional[float] = None,
            error: Optional[str] = None,
        ) -> None:
            current = dict(worker_status.get("upload_file_progresses", {}))
            item = dict(current.get(transfer_id, {}))
            if percent is not None:
                item["percent"] = int(percent)
            if received is not None:
                item["received"] = int(received)
            if total is not None:
                item["total"] = int(total)
            if state is not None:
                item["state"] = state
            if file_path:
                item["file_path"] = file_path
            if speed_bps is not None:
                item["speed_bps"] = float(speed_bps)
            if eta_sec is not None:
                item["eta_sec"] = float(eta_sec)
            if error is not None:
                item["error"] = str(error)
            item["updated_at"] = datetime.now().strftime("%H:%M:%S")
            current[transfer_id] = item
            if len(current) > 40:
                keys = list(current.keys())
                for key in keys[:-40]:
                    current.pop(key, None)
            _set_status(upload_file_progresses=current)

        def _status_hook(payload: dict) -> None:
            progress_runtime = getattr(_status_hook, "_progress_runtime", {})
            upload_runtime = getattr(_status_hook, "_upload_runtime", {})
            setattr(_status_hook, "_progress_runtime", progress_runtime)
            setattr(_status_hook, "_upload_runtime", upload_runtime)
            event = payload.get("event", "")
            message_id = str(payload.get("message_id", ""))
            is_upload_transfer = message_id.startswith("upload:")
            if event == "downloading":
                _update_progress_item(message_id, percent=0, received=0, total=0, state="downloading")
                progress_runtime[message_id] = {
                    "last_received": 0,
                    "last_ts": time.time(),
                    "speed_bps": 0.0,
                }
                _set_status(
                    current_message_id=message_id,
                    message=f"Скачивание message_id={message_id}",
                    progress_percent=0,
                    progress_received=0,
                    progress_total=0,
                )
            elif event == "progress":
                received = int(payload.get("received", 0))
                total = int(payload.get("total", 0))
                now = time.time()
                state = progress_runtime.get(
                    message_id, {"last_received": 0, "last_ts": now, "speed_bps": 0.0}
                )
                delta_bytes = received - int(state.get("last_received", 0))
                delta_time = now - float(state.get("last_ts", now))
                speed_bps = float(state.get("speed_bps", 0.0))
                if delta_bytes > 0 and delta_time > 0.2:
                    speed_bps = delta_bytes / delta_time
                eta_sec = 0.0
                if speed_bps > 1 and total > received:
                    eta_sec = (total - received) / speed_bps
                progress_runtime[message_id] = {
                    "last_received": received,
                    "last_ts": now,
                    "speed_bps": speed_bps,
                }
                _update_progress_item(
                    message_id,
                    percent=int(payload.get("percent", 0)),
                    received=received,
                    total=total,
                    state="downloading",
                    speed_bps=speed_bps,
                    eta_sec=eta_sec,
                )
                _set_status(
                    current_message_id=message_id,
                    progress_percent=int(payload.get("percent", 0)),
                    progress_received=received,
                    progress_total=total,
                )
            elif event == "downloaded":
                _update_progress_item(
                    message_id,
                    percent=100,
                    state="done",
                    file_path=str(payload.get("file_path", "")),
                    eta_sec=0.0,
                )
                progress_runtime.pop(message_id, None)
                _set_status(
                    downloaded=int(worker_status.get("downloaded", 0)) + 1,
                    current_message_id=message_id,
                    last_file=str(payload.get("file_path", "")),
                    progress_percent=100,
                    message=f"Скачан message_id={message_id}",
                )
            elif event == "failed":
                _update_progress_item(message_id, state="failed")
                progress_runtime.pop(message_id, None)
                _set_status(
                    failed=int(worker_status.get("failed", 0)) + 1,
                    current_message_id=message_id,
                    message=f"Ошибка message_id={message_id}",
                )
            elif event == "skipped":
                _set_status(skipped=int(worker_status.get("skipped", 0)) + 1)
            elif event == "throttled":
                _set_status(
                    current_message_id=message_id,
                    current_concurrency=int(payload.get("concurrency", worker_status.get("current_concurrency", 1))),
                    message=(
                        f"FloodWait {payload.get('seconds', '?')}s; "
                        f"понижаю параллельность до {payload.get('concurrency', 1)}"
                    ),
                )
            elif event == "sftp_ready":
                _set_status(message=str(payload.get("message", "Удаленный протокол готов.")))
            elif event == "sftp_uploaded":
                if not is_upload_transfer:
                    _update_progress_item(message_id, state="sftp_uploaded")
                _update_upload_progress_item(message_id, state="uploaded", percent=100, eta_sec=0.0)
                upload_runtime.pop(message_id, None)
                transport = str(payload.get("transport", "SFTP"))
                _set_status(
                    sftp_uploaded=int(worker_status.get("sftp_uploaded", 0)) + 1,
                    message=f"{transport} загружен: message_id={message_id}",
                )
            elif event == "sftp_skipped":
                if not is_upload_transfer:
                    _update_progress_item(message_id, state="sftp_skipped")
                _update_upload_progress_item(message_id, state="skipped", eta_sec=0.0)
                upload_runtime.pop(message_id, None)
                transport = str(payload.get("transport", "SFTP"))
                _set_status(
                    sftp_skipped=int(worker_status.get("sftp_skipped", 0)) + 1,
                    message=f"{transport} пропуск: message_id={message_id}",
                )
            elif event == "sftp_failed":
                reason = str(payload.get("reason", "")).strip()
                if not is_upload_transfer:
                    _update_progress_item(message_id, state="sftp_failed", error=reason)
                _update_upload_progress_item(message_id, state="failed", error=reason)
                upload_runtime.pop(message_id, None)
                transport = str(payload.get("transport", "SFTP"))
                if reason:
                    logging.error("%s failed message_id=%s reason=%s", transport, message_id, reason)
                _set_status(
                    sftp_failed=int(worker_status.get("sftp_failed", 0)) + 1,
                    message=(
                        f"{transport} ошибка: message_id={message_id}"
                        + (f" ({reason})" if reason else "")
                    ),
                )
            elif event == "uploading":
                transfer_id = message_id or str(payload.get("file_path", "upload"))
                _update_upload_progress_item(
                    transfer_id,
                    percent=0,
                    received=0,
                    total=0,
                    state="uploading",
                    file_path=str(payload.get("file_path", "")),
                    speed_bps=0.0,
                    eta_sec=0.0,
                )
                upload_runtime[transfer_id] = {
                    "last_received": 0,
                    "last_ts": time.time(),
                    "speed_bps": 0.0,
                    "started_ts": time.time(),
                }
                _set_status(message=f"Upload: {transfer_id}")
            elif event == "upload_progress":
                transfer_id = message_id or str(payload.get("file_path", "upload"))
                received = int(payload.get("received", 0))
                total = int(payload.get("total", 0))
                now = time.time()
                state = upload_runtime.get(
                    transfer_id,
                    {"last_received": 0, "last_ts": now, "speed_bps": 0.0, "started_ts": now},
                )
                delta_bytes = received - int(state.get("last_received", 0))
                delta_time = now - float(state.get("last_ts", now))
                started_ts = float(state.get("started_ts", now))
                speed_bps_prev = float(state.get("speed_bps", 0.0))
                speed_bps_inst = 0.0
                if delta_bytes > 0 and delta_time > 0:
                    speed_bps_inst = delta_bytes / delta_time
                avg_time = max(0.001, now - started_ts)
                speed_bps_avg = float(received) / avg_time if received > 0 else 0.0
                speed_bps = max(speed_bps_inst, speed_bps_avg, speed_bps_prev)
                eta_sec = 0.0
                if speed_bps > 1 and total > received:
                    eta_sec = (total - received) / speed_bps
                upload_runtime[transfer_id] = {
                    "last_received": received,
                    "last_ts": now,
                    "speed_bps": speed_bps,
                    "started_ts": started_ts,
                }
                _update_upload_progress_item(
                    transfer_id,
                    percent=int(payload.get("percent", 0)),
                    received=received,
                    total=total,
                    state="uploading",
                    speed_bps=speed_bps,
                    eta_sec=eta_sec,
                )
                _set_status(
                    upload_progress_percent=int(payload.get("percent", 0)),
                    upload_progress_received=received,
                    upload_progress_total=total,
                    upload_progress_speed_bps=speed_bps,
                    upload_progress_eta_sec=eta_sec,
                )
            elif event == "upload_started":
                _set_status(
                    current_concurrency=int(payload.get("concurrency", worker_status.get("current_concurrency", 1))),
                    message=str(payload.get("message", "Upload started.")),
                )
            elif event == "upload_done":
                transfer_id = message_id or ""
                if transfer_id:
                    final_speed = 0.0
                    state = upload_runtime.get(transfer_id, {})
                    if state:
                        started_ts = float(state.get("started_ts", 0.0))
                        last_ts = float(state.get("last_ts", 0.0))
                        last_received = int(state.get("last_received", 0))
                        if started_ts > 0 and last_ts > started_ts and last_received > 0:
                            final_speed = float(last_received) / (last_ts - started_ts)
                    _update_upload_progress_item(
                        transfer_id,
                        state="done",
                        percent=100,
                        eta_sec=0.0,
                        speed_bps=final_speed if final_speed > 0 else None,
                    )
                    upload_runtime.pop(transfer_id, None)
                    if final_speed > 0:
                        _set_status(upload_progress_speed_bps=final_speed)
                _set_status(upload_progress_eta_sec=0.0)
            elif event == "local_cleaned":
                transport = str(payload.get("transport", "SFTP"))
                _set_status(message=f"Локальный файл удален после {transport}: message_id={message_id}")

        def _target() -> None:
            loop: Optional[asyncio.AbstractEventLoop] = None
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                main_task = loop.create_task(
                    run_remote_uploader(
                        settings=settings,
                        db_path=DB_PATH,
                        status_hook=_status_hook,
                        stop_requested=lambda: worker_stop_event.is_set(),
                    )
                    if upload_only
                    else run_downloader(
                        settings=settings,
                        db_path=DB_PATH,
                        allowed_message_ids=allowed_message_ids,
                        status_hook=_status_hook,
                        stop_requested=lambda: worker_stop_event.is_set(),
                        live_mode=live_mode,
                    )
                )
                with worker_lock:
                    worker_loop = loop
                    worker_main_task = main_task
                loop.run_until_complete(main_task)
            except asyncio.CancelledError:
                _set_status(message="Загрузка прервана пользователем.", running=False, mode="idle")
            except Exception as exc:
                logging.exception("Downloader crashed")
                _set_status(message=f"Ошибка загрузчика: {exc}", running=False, mode="idle")
            finally:
                with worker_lock:
                    worker_loop = None
                    worker_main_task = None
                if loop is not None:
                    try:
                        pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
                        for task in pending:
                            task.cancel()
                        if pending:
                            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                    except Exception:
                        logging.exception("Failed to finalize worker event loop")
                    finally:
                        loop.close()
                _set_status(running=False, mode="idle")

        worker_thread = threading.Thread(target=_target, daemon=True)
        worker_thread.start()

    return True, ("Upload-загрузчик запущен" if upload_only else "Загрузчик запущен") + f" ({source})."


def _render(form: dict, message: str = "", need_code: bool = False, need_password: bool = False):
    return render_template(
        "index.html",
        form=form,
        status=worker_status,
        auth_status=auth_status,
        proxy_status=proxy_status,
        sftp_status=sftp_status,
        ftps_status=ftps_status,
        message=message,
        need_code=need_code,
        need_password=need_password,
        preview=preview_cache,
    )


def _register_basic_routes() -> None:
    register_basic_routes(
        app,
        {
            "db_path": DB_PATH,
            "render": _render,
            "load_saved_form": _load_saved_form,
            "status_payload": _status_payload,
            "status_cond": status_cond,
            "get_status_version": lambda: status_version,
            "debug_log_lock": debug_log_lock,
            "debug_log_buffer": debug_log_buffer,
            "form_from_request": _form_from_request,
            "store_enable_periodic_checks": _store_enable_periodic_checks,
            "build_settings": _build_settings,
            "store_settings": _store_settings,
            "upsert_current_channel_preference": _upsert_current_channel_preference,
            "fetch_preview": _fetch_preview,
            "get_preview_cache": lambda: preview_cache,
            "set_preview_cache": lambda value: _set_preview_cache(value),
            "proxy_status": proxy_status,
            "sftp_status": sftp_status,
            "ftps_status": ftps_status,
            "validate_sftp": _validate_sftp,
            "set_sftp_status": _set_sftp_status,
            "validate_ftps": _validate_ftps,
            "set_ftps_status": _set_ftps_status,
            "collect_saved_channels_status": _collect_saved_channels_status,
            "collect_saved_channels_cached": _collect_saved_channels_cached,
            "safe_int": _safe_int,
            "resolve_last_downloaded_message_id": _resolve_last_downloaded_message_id,
        },
    )


def _set_preview_cache(value: list[dict]) -> None:
    global preview_cache
    preview_cache = value


_register_basic_routes()


@app.post("/authorize")
def authorize():
    global runtime_remember_me, runtime_session_name
    form = _form_from_request()

    try:
        settings = _build_settings(form)
    except ValueError as exc:
        return _render(form, str(exc))

    runtime_remember_me = bool(form["remember_me"])
    runtime_session_name = settings.session_name
    _store_remember_me(runtime_remember_me)
    _store_enable_periodic_checks(bool(form["enable_periodic_checks"]))
    if not runtime_remember_me:
        _delete_session_files(settings.session_name)

    proxy_ok, proxy_message = asyncio.run(_validate_proxy(settings))
    if settings.use_mtproxy:
        _set_proxy_status(True, proxy_ok, proxy_message)
        if not proxy_ok:
            return _render(form, proxy_message)
    else:
        _set_proxy_status(False, True, "MTProxy не используется.")

    sftp_ok, sftp_message = asyncio.run(_validate_sftp(settings))
    if settings.use_sftp:
        _set_sftp_status(True, sftp_ok, sftp_message)
        if not sftp_ok:
            return _render(form, sftp_message)
    else:
        _set_sftp_status(False, True, "SFTP не используется.")

    ftps_ok, ftps_message = asyncio.run(_validate_ftps(settings))
    if settings.use_ftps:
        _set_ftps_status(True, ftps_ok, ftps_message)
        if not ftps_ok:
            return _render(form, ftps_message)
    else:
        _set_ftps_status(False, True, "FTPS не используется.")

    _store_settings(settings)
    _upsert_current_channel_preference(settings)

    try:
        ok, auth_message, need_code, need_password = asyncio.run(
            _authorize_user(settings, form["code"], form["password"])
        )
    except Exception as exc:
        _set_auth_status(False, f"Ошибка авторизации: {exc}")
        return _render(form, f"Ошибка авторизации: {exc}")

    if not ok:
        _set_auth_status(False, auth_message)
        return _render(form, auth_message, need_code=need_code, need_password=need_password)

    _set_auth_status(True, auth_message)

    preview_ok, preview_message, items = asyncio.run(_fetch_preview(settings))
    global preview_cache
    preview_cache = items if preview_ok else []

    return _render(
        form,
        f"{proxy_status['message']} {sftp_status['message']} {ftps_status['message']} {auth_message} {preview_message}",
    )


@app.post("/start")
def start_download():
    global runtime_remember_me, runtime_session_name
    form = _form_from_request()

    try:
        settings = _build_settings(form)
    except ValueError as exc:
        return _render(form, str(exc))

    runtime_remember_me = bool(form["remember_me"])
    runtime_session_name = settings.session_name
    _store_remember_me(runtime_remember_me)
    _store_enable_periodic_checks(bool(form["enable_periodic_checks"]))

    proxy_ok, proxy_message = asyncio.run(_validate_proxy(settings))
    if settings.use_mtproxy:
        _set_proxy_status(True, proxy_ok, proxy_message)
        if not proxy_ok:
            return _render(form, proxy_message)
    else:
        _set_proxy_status(False, True, "MTProxy не используется.")

    sftp_ok, sftp_message = asyncio.run(_validate_sftp(settings))
    if settings.use_sftp:
        _set_sftp_status(True, sftp_ok, sftp_message)
        if not sftp_ok:
            return _render(form, sftp_message)
    else:
        _set_sftp_status(False, True, "SFTP не используется.")

    ftps_ok, ftps_message = asyncio.run(_validate_ftps(settings))
    if settings.use_ftps:
        _set_ftps_status(True, ftps_ok, ftps_message)
        if not ftps_ok:
            return _render(form, ftps_message)
    else:
        _set_ftps_status(False, True, "FTPS не используется.")

    _store_settings(settings)
    _upsert_current_channel_preference(settings)

    channel_ok, channel_message = asyncio.run(_validate_channel(settings))
    if not channel_ok:
        return _render(form, channel_message)

    global preview_cache
    if not preview_cache:
        preview_ok, _, items = asyncio.run(_fetch_preview(settings))
        preview_cache = items if preview_ok else []

    allowed_message_ids = _pick_range_ids(preview_cache, form["from_index"], form["to_index"])
    if allowed_message_ids is not None and len(allowed_message_ids) == 0:
        return _render(form, "В выбранном диапазоне нет аудио.")

    started, start_message = _start_worker(settings, allowed_message_ids, live_mode=True, source="manual")
    if started:
        return _render(
            form,
            f"{proxy_status['message']} {sftp_status['message']} {ftps_status['message']} {channel_message} {start_message}",
        )
    return _render(form, start_message)


@app.post("/start_upload")
def start_upload():
    global runtime_remember_me, runtime_session_name
    form = _form_from_request()

    try:
        settings = _build_settings(form, require_channel=True)
    except ValueError as exc:
        return _render(form, str(exc))

    runtime_remember_me = bool(form["remember_me"])
    runtime_session_name = settings.session_name
    _store_remember_me(runtime_remember_me)
    _store_enable_periodic_checks(bool(form["enable_periodic_checks"]))
    _store_settings(settings)
    _upsert_current_channel_preference(settings)

    sftp_ok, sftp_message = asyncio.run(_validate_sftp(settings))
    if settings.use_sftp:
        _set_sftp_status(True, sftp_ok, sftp_message)
        if not sftp_ok:
            return _render(form, sftp_message)
    else:
        _set_sftp_status(False, True, "SFTP не используется.")

    ftps_ok, ftps_message = asyncio.run(_validate_ftps(settings))
    if settings.use_ftps:
        _set_ftps_status(True, ftps_ok, ftps_message)
        if not ftps_ok:
            return _render(form, ftps_message)
    else:
        _set_ftps_status(False, True, "FTPS не используется.")

    if not settings.use_sftp and not settings.use_ftps:
        return _render(form, "Для upload включите SFTP или FTPS.")

    started, start_message = _start_worker(
        settings,
        allowed_message_ids=None,
        live_mode=False,
        source="manual-upload",
        upload_only=True,
    )
    if started:
        return _render(form, f"{sftp_status['message']} {ftps_status['message']} {start_message}")
    return _render(form, start_message)


@app.post("/stop_server")
def stop_server():
    global worker_thread, monitor_thread

    worker_stop_event.set()
    with worker_lock:
        if worker_loop and worker_main_task and not worker_main_task.done():
            try:
                worker_loop.call_soon_threadsafe(worker_main_task.cancel)
            except Exception:
                logging.exception("Failed to cancel worker task during server stop")
    monitor_stop_event.set()
    _set_status(message="Остановка сервера...", running=False, mode="idle")

    if worker_thread and worker_thread.is_alive():
        worker_thread.join(timeout=10)
    if monitor_thread and monitor_thread.is_alive():
        monitor_thread.join(timeout=5)

    if not runtime_remember_me:
        _delete_session_files(runtime_session_name)

    shutdown_func = request.environ.get("werkzeug.server.shutdown")
    if shutdown_func is not None:
        shutdown_func()
        return "Сервер остановлен."

    def _force_exit() -> None:
        # Flask 3 / certain launch modes may not expose werkzeug.server.shutdown.
        # Fallback: terminate process after response is sent.
        time.sleep(0.5)
        os._exit(0)

    threading.Thread(target=_force_exit, daemon=True).start()
    return "Сервер остановлен (fallback)."


@app.post("/stop_download")
def stop_download():
    global worker_thread
    form = _form_from_request()
    if not (worker_thread and worker_thread.is_alive()):
        _set_status(running=False, mode="idle", message="Активная загрузка не выполняется.")
        return _render(form, "Активная загрузка не выполняется.")

    worker_stop_event.set()
    with worker_lock:
        if worker_loop and worker_main_task and not worker_main_task.done():
            try:
                worker_loop.call_soon_threadsafe(worker_main_task.cancel)
            except Exception:
                logging.exception("Failed to cancel worker task")
    _set_status(message="Запрошена остановка текущей загрузки...")
    worker_thread.join(timeout=15)

    if worker_thread and worker_thread.is_alive():
        return _render(form, "Остановка запрошена, ожидается завершение текущих задач.")

    _set_status(running=False, mode="idle", message="Текущая задача остановлена.")
    return _render(form, "Текущая задача остановлена.")


if __name__ == "__main__":
    _start_monitor_thread()
    if OPEN_BROWSER:
        threading.Timer(1.0, lambda: webbrowser.open(f"http://{HOST}:{PORT}")).start()
    app.run(host=HOST, port=PORT, debug=False, use_reloader=False)
