import asyncio
from collections import deque
import json
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

from flask import Flask, Response, jsonify, render_template, request, stream_with_context
from telethon import utils
from telethon.errors import PhoneCodeExpiredError, PhoneCodeInvalidError, SessionPasswordNeededError

from core.config import setup_logging
from core.db import AppDatabase
from core.downloader import run_downloader
from core.ftps_client import FTPSSync
from core.models import Settings
from core.sftp_client import SFTPSync
from core.telegram_client import create_telegram_client, is_audio_message, resolve_channel_entity


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
    "file_progresses": {},
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


def _safe_int(value: str, default: int = 0) -> int:
    try:
        return int(value)
    except ValueError:
        return default


def _load_saved_form() -> dict:
    db = AppDatabase(DB_PATH)
    try:
        return {
            "api_id": db.get_setting("API_ID") or "",
            "api_hash": db.get_setting("API_HASH") or "",
            "phone": db.get_setting("PHONE") or "",
            "channel_id": db.get_setting("CHANNEL_ID") or "",
            "download_dir": db.get_setting("DOWNLOAD_DIR") or str(Path("downloads").resolve()),
            "session_name": db.get_setting("SESSION_NAME") or os.getenv("SESSION_NAME", "user_session"),
            "startup_scan_limit": db.get_setting("STARTUP_SCAN_LIMIT") or "200",
            "download_concurrency": db.get_setting("DOWNLOAD_CONCURRENCY") or "3",
            "code": "",
            "password": "",
            "from_index": "",
            "to_index": "",
            "use_mtproxy": (db.get_setting("USE_MTPROXY") or "0") == "1",
            "mtproxy_link": db.get_setting("MTPROXY_LINK") or "",
            "use_sftp": (db.get_setting("USE_SFTP") or "0") == "1",
            "sftp_host": db.get_setting("SFTP_HOST") or "",
            "sftp_port": db.get_setting("SFTP_PORT") or "22",
            "sftp_username": db.get_setting("SFTP_USERNAME") or "",
            "sftp_password": db.get_setting("SFTP_PASSWORD") or "",
            "sftp_remote_dir": db.get_setting("SFTP_REMOTE_DIR") or "/uploads",
            "cleanup_local_after_sftp": (db.get_setting("CLEANUP_LOCAL_AFTER_SFTP") or "0") == "1",
            "use_ftps": (db.get_setting("USE_FTPS") or "0") == "1",
            "ftps_host": db.get_setting("FTPS_HOST") or "",
            "ftps_port": db.get_setting("FTPS_PORT") or "21",
            "ftps_username": db.get_setting("FTPS_USERNAME") or "",
            "ftps_password": db.get_setting("FTPS_PASSWORD") or "",
            "ftps_remote_dir": db.get_setting("FTPS_REMOTE_DIR") or "/uploads",
            "cleanup_local_after_ftps": (db.get_setting("CLEANUP_LOCAL_AFTER_FTPS") or "0") == "1",
            "download_new": False,
            "remember_me": (db.get_setting("REMEMBER_ME") or "1") != "0",
            "enable_periodic_checks": (db.get_setting("ENABLE_PERIODIC_CHECKS") or "0") == "1",
        }
    finally:
        db.close()


def _form_from_request() -> dict:
    saved = _load_saved_form()
    return {
        "api_id": request.form.get("api_id", saved["api_id"]).strip(),
        "api_hash": request.form.get("api_hash", saved["api_hash"]).strip(),
        "phone": request.form.get("phone", saved["phone"]).strip(),
        "channel_id": request.form.get("channel_id", saved["channel_id"]).strip(),
        "download_dir": request.form.get("download_dir", saved["download_dir"]).strip(),
        "session_name": request.form.get("session_name", saved["session_name"]).strip() or "user_session",
        "startup_scan_limit": request.form.get("startup_scan_limit", saved["startup_scan_limit"]).strip(),
        "download_concurrency": request.form.get("download_concurrency", saved["download_concurrency"]).strip(),
        "code": request.form.get("code", "").strip(),
        "password": request.form.get("password", "").strip(),
        "from_index": request.form.get("from_index", "").strip(),
        "to_index": request.form.get("to_index", "").strip(),
        "use_mtproxy": request.form.get("use_mtproxy") == "on",
        "mtproxy_link": request.form.get("mtproxy_link", saved["mtproxy_link"]).strip(),
        "use_sftp": request.form.get("use_sftp") == "on",
        "sftp_host": request.form.get("sftp_host", saved["sftp_host"]).strip(),
        "sftp_port": request.form.get("sftp_port", saved["sftp_port"]).strip(),
        "sftp_username": request.form.get("sftp_username", saved["sftp_username"]).strip(),
        "sftp_password": request.form.get("sftp_password", saved["sftp_password"]).strip(),
        "sftp_remote_dir": request.form.get("sftp_remote_dir", saved["sftp_remote_dir"]).strip(),
        "cleanup_local_after_sftp": request.form.get("cleanup_local_after_sftp") == "on",
        "use_ftps": request.form.get("use_ftps") == "on",
        "ftps_host": request.form.get("ftps_host", saved["ftps_host"]).strip(),
        "ftps_port": request.form.get("ftps_port", saved["ftps_port"]).strip(),
        "ftps_username": request.form.get("ftps_username", saved["ftps_username"]).strip(),
        "ftps_password": request.form.get("ftps_password", saved["ftps_password"]).strip(),
        "ftps_remote_dir": request.form.get("ftps_remote_dir", saved["ftps_remote_dir"]).strip(),
        "cleanup_local_after_ftps": request.form.get("cleanup_local_after_ftps") == "on",
        "download_new": request.form.get("download_new") == "on",
        "remember_me": request.form.get("remember_me") == "on",
        "enable_periodic_checks": request.form.get("enable_periodic_checks") == "on",
    }


def _build_settings(form: dict, require_channel: bool = True) -> Settings:
    if not form["api_id"] or not form["api_hash"] or not form["phone"]:
        raise ValueError("Заполните API_ID, API_HASH и PHONE.")
    if require_channel and not form["channel_id"]:
        raise ValueError("Заполните CHANNEL_ID.")

    api_id = _safe_int(form["api_id"], 0)
    if api_id <= 0:
        raise ValueError("API_ID должен быть числом больше 0.")

    scan_limit = _safe_int(form["startup_scan_limit"], 200)
    if scan_limit < 0:
        scan_limit = 200

    download_concurrency = _safe_int(form["download_concurrency"], 3)
    if download_concurrency < 1:
        download_concurrency = 1

    if form["use_mtproxy"] and not form["mtproxy_link"]:
        raise ValueError("Включен MTProxy, но строка прокси пустая.")

    sftp_port = _safe_int(form["sftp_port"], 22)
    if sftp_port <= 0:
        sftp_port = 22
    if form["use_sftp"] and (not form["sftp_host"] or not form["sftp_username"]):
        raise ValueError("Для SFTP заполните host и username.")

    ftps_port = _safe_int(form["ftps_port"], 21)
    if ftps_port <= 0:
        ftps_port = 21
    if form["use_ftps"] and (not form["ftps_host"] or not form["ftps_username"]):
        raise ValueError("Для FTPS заполните host и username.")
    if form["use_sftp"] and form["use_ftps"]:
        raise ValueError("Одновременно можно включить только один протокол: SFTP или FTPS.")

    return Settings(
        api_id=api_id,
        api_hash=form["api_hash"],
        phone=form["phone"],
        channel=form["channel_id"] or "_",
        download_dir=Path(form["download_dir"] or "downloads").expanduser().resolve(),
        session_name=form["session_name"],
        startup_scan_limit=scan_limit,
        download_concurrency=download_concurrency,
        use_mtproxy=bool(form["use_mtproxy"]),
        mtproxy_link=form["mtproxy_link"],
        use_sftp=bool(form["use_sftp"]),
        sftp_host=form["sftp_host"],
        sftp_port=sftp_port,
        sftp_username=form["sftp_username"],
        sftp_password=form["sftp_password"],
        sftp_remote_dir=form["sftp_remote_dir"] or "/uploads",
        cleanup_local_after_sftp=bool(form["cleanup_local_after_sftp"]),
        use_ftps=bool(form["use_ftps"]),
        ftps_host=form["ftps_host"],
        ftps_port=ftps_port,
        ftps_username=form["ftps_username"],
        ftps_password=form["ftps_password"],
        ftps_remote_dir=form["ftps_remote_dir"] or "/uploads",
        cleanup_local_after_ftps=bool(form["cleanup_local_after_ftps"]),
    )


def _store_settings(settings: Settings) -> None:
    db = AppDatabase(DB_PATH)
    try:
        db.store_settings(settings)
    finally:
        db.close()


def _store_remember_me(remember_me: bool) -> None:
    db = AppDatabase(DB_PATH)
    try:
        db.set_setting("REMEMBER_ME", "1" if remember_me else "0")
    finally:
        db.close()


def _store_enable_periodic_checks(enabled: bool) -> None:
    db = AppDatabase(DB_PATH)
    try:
        db.set_setting("ENABLE_PERIODIC_CHECKS", "1" if enabled else "0")
    finally:
        db.close()


def _is_periodic_checks_enabled() -> bool:
    db = AppDatabase(DB_PATH)
    try:
        return (db.get_setting("ENABLE_PERIODIC_CHECKS") or "0") == "1"
    finally:
        db.close()


def _delete_session_files(session_name: str) -> None:
    if not session_name:
        return

    candidates = [
        Path(f"{session_name}.session"),
        Path(f"{session_name}.session-journal"),
        Path(f"{session_name}.session-wal"),
        Path(f"{session_name}.session-shm"),
    ]
    for file_path in candidates:
        try:
            if file_path.exists():
                file_path.unlink()
        except Exception:
            logging.warning("Failed to remove session file: %s", file_path)


def _save_phone_code_hash(phone: str, phone_code_hash: str) -> None:
    db = AppDatabase(DB_PATH)
    try:
        db.set_setting("AUTH_PHONE", phone)
        db.set_setting("PHONE_CODE_HASH", phone_code_hash)
    finally:
        db.close()


def _load_phone_code_hash(phone: str) -> str:
    db = AppDatabase(DB_PATH)
    try:
        saved_phone = (db.get_setting("AUTH_PHONE") or "").strip()
        if saved_phone != phone:
            return ""
        return (db.get_setting("PHONE_CODE_HASH") or "").strip()
    finally:
        db.close()


def _clear_phone_code_hash() -> None:
    db = AppDatabase(DB_PATH)
    try:
        db.set_setting("AUTH_PHONE", "")
        db.set_setting("PHONE_CODE_HASH", "")
    finally:
        db.close()


async def _validate_proxy(settings: Settings) -> tuple[bool, str]:
    if not settings.use_mtproxy:
        return True, "MTProxy выключен."

    client = create_telegram_client(settings)
    try:
        await asyncio.wait_for(client.connect(), timeout=12)
        if not client.is_connected():
            return False, "MTProxy: подключение не установлено."
        return True, "MTProxy: подключение установлено."
    except Exception as exc:
        return False, f"MTProxy: ошибка подключения ({exc})."
    finally:
        await client.disconnect()


async def _validate_sftp(settings: Settings) -> tuple[bool, str]:
    if not settings.use_sftp:
        return True, "SFTP выключен."
    sftp_sync = SFTPSync(settings)
    return await asyncio.to_thread(sftp_sync.validate_connection)


async def _validate_ftps(settings: Settings) -> tuple[bool, str]:
    if not settings.use_ftps:
        return True, "FTPS выключен."
    ftps_sync = FTPSSync(settings)
    return await asyncio.to_thread(ftps_sync.validate_connection)


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
        db.upsert_channel_preferences(
            channel_ref=channel_ref,
            channel_id=0,
            channel_title=channel_ref,
            check_new=False,
            auto_download=False,
            auto_sftp=False,
            auto_ftps=False,
            cleanup_local=bool(settings.cleanup_local_after_sftp or settings.cleanup_local_after_ftps),
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
) -> tuple[bool, str]:
    global worker_thread, worker_loop, worker_main_task

    with worker_lock:
        if worker_thread and worker_thread.is_alive():
            return False, "Скачивание уже запущено."

        worker_stop_event.clear()
        _set_status(
            running=True,
            message=f"Скачивание запущено ({source}).",
            downloaded=0,
            failed=0,
            skipped=0,
            sftp_uploaded=0,
            sftp_skipped=0,
            sftp_failed=0,
            progress_percent=0,
            progress_received=0,
            progress_total=0,
            file_progresses={},
            current_message_id="",
            last_file="",
            configured_concurrency=settings.download_concurrency,
            current_concurrency=settings.download_concurrency,
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
            item["updated_at"] = datetime.now().strftime("%H:%M:%S")
            current[message_id] = item
            if len(current) > 40:
                keys = list(current.keys())
                for key in keys[:-40]:
                    current.pop(key, None)
            _set_status(file_progresses=current)

        def _status_hook(payload: dict) -> None:
            progress_runtime = getattr(_status_hook, "_progress_runtime", {})
            setattr(_status_hook, "_progress_runtime", progress_runtime)
            event = payload.get("event", "")
            message_id = str(payload.get("message_id", ""))
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
                _update_progress_item(message_id, state="sftp_uploaded")
                transport = str(payload.get("transport", "SFTP"))
                _set_status(
                    sftp_uploaded=int(worker_status.get("sftp_uploaded", 0)) + 1,
                    message=f"{transport} загружен: message_id={message_id}",
                )
            elif event == "sftp_skipped":
                _update_progress_item(message_id, state="sftp_skipped")
                transport = str(payload.get("transport", "SFTP"))
                _set_status(
                    sftp_skipped=int(worker_status.get("sftp_skipped", 0)) + 1,
                    message=f"{transport} пропуск: message_id={message_id}",
                )
            elif event == "sftp_failed":
                _update_progress_item(message_id, state="sftp_failed")
                transport = str(payload.get("transport", "SFTP"))
                _set_status(
                    sftp_failed=int(worker_status.get("sftp_failed", 0)) + 1,
                    message=f"{transport} ошибка: message_id={message_id}",
                )
            elif event == "local_cleaned":
                transport = str(payload.get("transport", "SFTP"))
                _set_status(message=f"Локальный файл удален после {transport}: message_id={message_id}")

        def _target() -> None:
            loop: Optional[asyncio.AbstractEventLoop] = None
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                main_task = loop.create_task(
                    run_downloader(
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
                _set_status(message="Загрузка прервана пользователем.", running=False)
            except Exception as exc:
                logging.exception("Downloader crashed")
                _set_status(message=f"Ошибка загрузчика: {exc}", running=False)
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
                _set_status(running=False)

        worker_thread = threading.Thread(target=_target, daemon=True)
        worker_thread.start()

    return True, f"Загрузчик запущен ({source})."


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


@app.get("/")
def index():
    return _render(_load_saved_form())


@app.get("/status")
def status():
    return jsonify(_status_payload())


@app.get("/status_stream")
def status_stream():
    @stream_with_context
    def _event_stream():
        last_seen = -1
        while True:
            with status_cond:
                if last_seen == -1:
                    payload = _status_payload()
                    last_seen = status_version
                else:
                    status_cond.wait(timeout=25)
                    payload = _status_payload()
                    last_seen = status_version
            yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"

    return Response(_event_stream(), mimetype="text/event-stream")


@app.get("/debug_logs")
def debug_logs():
    with debug_log_lock:
        lines = list(debug_log_buffer)
    return jsonify({"lines": lines, "count": len(lines)})


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


@app.post("/preview")
def refresh_preview():
    form = _form_from_request()
    _store_enable_periodic_checks(bool(form["enable_periodic_checks"]))

    try:
        settings = _build_settings(form)
    except ValueError as exc:
        return _render(form, str(exc))

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
    else:
        _set_sftp_status(False, True, "SFTP не используется.")

    ftps_ok, ftps_message = asyncio.run(_validate_ftps(settings))
    if settings.use_ftps:
        _set_ftps_status(True, ftps_ok, ftps_message)
    else:
        _set_ftps_status(False, True, "FTPS не используется.")

    _upsert_current_channel_preference(settings)
    preview_ok, preview_message, items = asyncio.run(_fetch_preview(settings))
    global preview_cache
    preview_cache = items if preview_ok else []

    return _render(form, f"{proxy_status['message']} {sftp_status['message']} {ftps_status['message']} {preview_message}")


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


@app.post("/check_sftp")
def check_sftp():
    form = _form_from_request()
    try:
        settings = _build_settings(form, require_channel=False)
    except ValueError as exc:
        return _render(form, str(exc))

    sftp_ok, sftp_message = asyncio.run(_validate_sftp(settings))
    if settings.use_sftp:
        _set_sftp_status(True, sftp_ok, sftp_message)
    else:
        _set_sftp_status(False, True, "SFTP не используется.")
    return _render(form, sftp_status["message"])


@app.post("/check_ftps")
def check_ftps():
    form = _form_from_request()
    try:
        settings = _build_settings(form, require_channel=False)
    except ValueError as exc:
        return _render(form, str(exc))

    ftps_ok, ftps_message = asyncio.run(_validate_ftps(settings))
    if settings.use_ftps:
        _set_ftps_status(True, ftps_ok, ftps_message)
    else:
        _set_ftps_status(False, True, "FTPS не используется.")
    return _render(form, ftps_status["message"])


@app.post("/channels_status")
def channels_status():
    form = _form_from_request()
    _store_enable_periodic_checks(bool(form["enable_periodic_checks"]))
    try:
        settings = _build_settings(form, require_channel=False)
    except ValueError as exc:
        return jsonify({"ok": False, "message": str(exc), "items": []}), 400

    ok, message, items = asyncio.run(_collect_saved_channels_status(settings))
    return jsonify({"ok": ok, "message": message, "items": items})


@app.post("/channels_preferences_update")
def channels_preferences_update():
    form = _form_from_request()
    _store_enable_periodic_checks(bool(form["enable_periodic_checks"]))
    try:
        settings = _build_settings(form, require_channel=False)
    except ValueError as exc:
        return jsonify({"ok": False, "message": str(exc)}), 400

    channel_ref = request.form.get("channel_ref", "").strip()
    channel_id = _safe_int(request.form.get("channel_id", "0"), 0)
    channel_title = request.form.get("channel_title", "").strip()
    check_new = request.form.get("check_new") == "1"
    auto_download = request.form.get("auto_download") == "1"
    auto_sftp = request.form.get("auto_sftp") == "1"
    auto_ftps = request.form.get("auto_ftps") == "1"
    cleanup_local = request.form.get("cleanup_local") == "1"

    if not channel_ref:
        return jsonify({"ok": False, "message": "channel_ref is required"}), 400

    db = AppDatabase(DB_PATH)
    try:
        db.upsert_channel_preferences(
            channel_ref=channel_ref,
            channel_id=channel_id,
            channel_title=channel_title or channel_ref,
            check_new=check_new,
            auto_download=auto_download,
            auto_sftp=auto_sftp,
            auto_ftps=auto_ftps,
            cleanup_local=cleanup_local,
        )
    finally:
        db.close()

    # Update global default cleanup option for manual runs.
    settings = replace(
        settings,
        cleanup_local_after_sftp=cleanup_local,
        cleanup_local_after_ftps=cleanup_local,
    )
    _store_settings(settings)
    return jsonify({"ok": True, "message": "Настройки канала сохранены."})


@app.post("/suggest_new_range")
def suggest_new_range():
    form = _form_from_request()
    try:
        settings = _build_settings(form, require_channel=True)
    except ValueError as exc:
        return jsonify({"ok": False, "message": str(exc)}), 400

    global preview_cache
    if not preview_cache:
        ok_preview, _, items = asyncio.run(_fetch_preview(settings))
        preview_cache = items if ok_preview else []

    if not preview_cache:
        return jsonify({"ok": False, "message": "Нет данных предпросмотра. Нажмите Обновить предпросмотр."}), 200

    try:
        last_downloaded_id = asyncio.run(_resolve_last_downloaded_message_id(settings))
    except Exception as exc:
        return jsonify({"ok": False, "message": f"Не удалось вычислить диапазон новых: {exc}"}), 200

    from_index = 0
    to_index = int(len(preview_cache))
    for item in preview_cache:
        if int(item.get("message_id", 0)) > int(last_downloaded_id):
            from_index = int(item.get("index", 0))
            break

    if from_index <= 0:
        return jsonify(
            {
                "ok": True,
                "message": "Новых глав не найдено.",
                "from_index": "",
                "to_index": "",
                "has_new": False,
                "last_downloaded_id": int(last_downloaded_id),
            }
        )

    return jsonify(
        {
            "ok": True,
            "message": "Диапазон новых глав определен.",
            "from_index": str(from_index),
            "to_index": str(to_index),
            "has_new": True,
            "last_downloaded_id": int(last_downloaded_id),
        }
    )


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
    _set_status(message="Остановка сервера...", running=False)

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
        _set_status(running=False, message="Активная загрузка не выполняется.")
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

    _set_status(running=False, message="Текущая загрузка остановлена.")
    return _render(form, "Текущая загрузка остановлена.")


if __name__ == "__main__":
    _start_monitor_thread()
    if OPEN_BROWSER:
        threading.Timer(1.0, lambda: webbrowser.open(f"http://{HOST}:{PORT}")).start()
    app.run(host=HOST, port=PORT, debug=False, use_reloader=False)
