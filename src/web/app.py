import asyncio
import logging
import os
import threading
import time
import webbrowser
from dataclasses import replace
from pathlib import Path
from typing import Optional

from flask import Flask, render_template
from telethon.errors import PhoneCodeExpiredError, PhoneCodeInvalidError, SessionPasswordNeededError

from core.config import setup_logging
from core.db import AppDatabase
from core.models import Settings
from core.telegram_client import create_telegram_client, is_audio_message, resolve_channel_entity
from web.ops.ftps_ops import fetch_ftps_preview, ftps_audit_selected_channel
from web.ops.telegram_ops import (
    fetch_preview,
    pick_range_ids,
    resolve_effective_last_downloaded_message_id,
    resolve_last_downloaded_message_id,
)
from web.channels import (
    collect_saved_channels_cached as channels_collect_saved_channels_cached,
    collect_saved_channels_status as channels_collect_saved_channels_status,
    run_periodic_checks_once as channels_run_periodic_checks_once,
    upsert_current_channel_preference as channels_upsert_current_channel_preference,
)
import web.runtime as runtime_data
from web.runtime import (
    _set_auth_status,
    _set_debug_mode,
    _set_ftps_remote_preview_cache,
    _set_ftps_remote_preview_meta,
    _set_ftps_status,
    _set_preview_cache,
    _set_proxy_status,
    _set_sftp_status,
    _set_status,
    _status_payload,
    auth_status,
    configure_runtime_paths,
    debug_log_buffer,
    debug_log_lock,
    ftps_status,
    get_status_version,
    proxy_status,
    sftp_status,
    status_cond,
    worker_status,
)
from web.form_ops import (
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
from web.routes_basic import register_basic_routes
from web.actions import register_action_routes
from web.worker import build_status_hook, run_worker_target


DB_PATH = Path(os.getenv("DB_PATH", "bot_data.sqlite3"))
HOST = os.getenv("APP_HOST", "127.0.0.1")
PORT = int(os.getenv("APP_PORT", "8080"))
OPEN_BROWSER = os.getenv("OPEN_BROWSER", "1").strip().lower() in {"1", "true", "yes", "on"}
PREVIEW_LIMIT = 300
AUTO_CHECK_INTERVAL_SECONDS = int(os.getenv("AUTO_CHECK_INTERVAL_SECONDS", "7200"))

app = Flask(__name__, template_folder="../templates", static_folder="../static")
setup_logging()
configure_runtime_paths(DB_PATH)

worker_lock = threading.Lock()
worker_thread: Optional[threading.Thread] = None
worker_stop_event = threading.Event()
worker_loop: Optional[asyncio.AbstractEventLoop] = None
worker_main_task: Optional[asyncio.Task] = None
monitor_thread: Optional[threading.Thread] = None
monitor_stop_event = threading.Event()
runtime_state = {"session_name": "", "remember_me": True}


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


def _upsert_current_channel_preference(settings: Settings) -> None:
    channels_upsert_current_channel_preference(settings, DB_PATH)


def _collect_saved_channels_cached() -> tuple[bool, str, list[dict]]:
    return channels_collect_saved_channels_cached(DB_PATH)


async def _collect_saved_channels_status(
    settings: Settings, only_due: bool = False
) -> tuple[bool, str, list[dict]]:
    return await channels_collect_saved_channels_status(
        settings=settings,
        db_path=DB_PATH,
        worker_busy=lambda: bool(worker_thread and worker_thread.is_alive()),
        auto_check_interval_seconds=AUTO_CHECK_INTERVAL_SECONDS,
        only_due=only_due,
    )


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
    channels_run_periodic_checks_once(
        db_path=DB_PATH,
        is_periodic_checks_enabled=_is_periodic_checks_enabled,
        worker_busy=lambda: bool(worker_thread and worker_thread.is_alive()),
        load_saved_form=_load_saved_form,
        build_settings=_build_settings,
        collect_saved_channels_status_cb=_collect_saved_channels_status,
        start_worker=_start_worker,
    )


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
    return await fetch_preview(settings, DB_PATH, limit=limit)


async def _fetch_ftps_preview(settings: Settings, limit: int = 300) -> tuple[bool, str, list[dict]]:
    return await fetch_ftps_preview(settings, DB_PATH, limit=limit)


async def _resolve_last_downloaded_message_id(settings: Settings) -> int:
    return await resolve_last_downloaded_message_id(settings, DB_PATH)


async def _resolve_effective_last_downloaded_message_id(settings: Settings) -> int:
    return await resolve_effective_last_downloaded_message_id(settings, DB_PATH)


async def _ftps_audit_selected_channel(settings: Settings) -> tuple[bool, str]:
    return await ftps_audit_selected_channel(settings, DB_PATH, _set_status, worker_status)


def _pick_range_ids(items: list[dict], from_index_raw: str, to_index_raw: str) -> Optional[set[int]]:
    return pick_range_ids(items, from_index_raw, to_index_raw, _safe_int)


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

        _status_hook = build_status_hook(_set_status, worker_status)

        def _target() -> None:
            def _set_worker_runtime(loop_obj: Optional[asyncio.AbstractEventLoop], task_obj: Optional[asyncio.Task]) -> None:
                global worker_loop, worker_main_task
                with worker_lock:
                    worker_loop = loop_obj
                    worker_main_task = task_obj

            run_worker_target(
                settings=settings,
                db_path=DB_PATH,
                allowed_message_ids=allowed_message_ids,
                upload_only=upload_only,
                live_mode=live_mode,
                worker_stop_event=worker_stop_event,
                set_status=_set_status,
                status_hook=_status_hook,
                set_worker_runtime=_set_worker_runtime,
            )

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
        preview=runtime_data.preview_cache,
        ftps_remote_preview=runtime_data.ftps_remote_preview_cache,
        ftps_remote_preview_meta=runtime_data.ftps_remote_preview_meta,
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
            "get_status_version": get_status_version,
            "debug_log_lock": debug_log_lock,
            "debug_log_buffer": debug_log_buffer,
            "set_debug_mode": _set_debug_mode,
            "form_from_request": _form_from_request,
            "store_enable_periodic_checks": _store_enable_periodic_checks,
            "build_settings": _build_settings,
            "store_settings": _store_settings,
            "upsert_current_channel_preference": _upsert_current_channel_preference,
            "fetch_preview": _fetch_preview,
            "fetch_ftps_preview": _fetch_ftps_preview,
            "get_preview_cache": lambda: runtime_data.preview_cache,
            "set_preview_cache": lambda value: _set_preview_cache(value),
            "get_ftps_remote_preview_cache": lambda: runtime_data.ftps_remote_preview_cache,
            "set_ftps_remote_preview_cache": lambda value: _set_ftps_remote_preview_cache(value),
            "get_ftps_remote_preview_meta": lambda: runtime_data.ftps_remote_preview_meta,
            "set_ftps_remote_preview_meta": lambda value: _set_ftps_remote_preview_meta(value),
            "proxy_status": proxy_status,
            "sftp_status": sftp_status,
            "ftps_status": ftps_status,
            "validate_sftp": _validate_sftp,
            "set_sftp_status": _set_sftp_status,
            "validate_ftps": _validate_ftps,
            "set_ftps_status": _set_ftps_status,
            "audit_ftps_selected_channel": _ftps_audit_selected_channel,
            "collect_saved_channels_status": _collect_saved_channels_status,
            "collect_saved_channels_cached": _collect_saved_channels_cached,
            "safe_int": _safe_int,
            "resolve_last_downloaded_message_id": _resolve_last_downloaded_message_id,
            "resolve_effective_last_downloaded_message_id": _resolve_effective_last_downloaded_message_id,
        },
    )


_register_basic_routes()
register_action_routes(
    app,
    {
        "render": _render,
        "form_from_request": _form_from_request,
        "build_settings": _build_settings,
        "store_remember_me": _store_remember_me,
        "store_enable_periodic_checks": _store_enable_periodic_checks,
        "store_settings": _store_settings,
        "upsert_current_channel_preference": _upsert_current_channel_preference,
        "delete_session_files": _delete_session_files,
        "validate_proxy": _validate_proxy,
        "validate_sftp": _validate_sftp,
        "validate_ftps": _validate_ftps,
        "validate_channel": _validate_channel,
        "authorize_user": _authorize_user,
        "fetch_preview": _fetch_preview,
        "pick_range_ids": _pick_range_ids,
        "start_worker": _start_worker,
        "set_auth_status": _set_auth_status,
        "set_proxy_status": _set_proxy_status,
        "set_sftp_status": _set_sftp_status,
        "set_ftps_status": _set_ftps_status,
        "set_preview_cache": _set_preview_cache,
        "set_status": _set_status,
        "get_preview_cache": lambda: runtime_data.preview_cache,
        "proxy_status": proxy_status,
        "sftp_status": sftp_status,
        "ftps_status": ftps_status,
        "runtime_state": runtime_state,
        "worker_stop_event": worker_stop_event,
        "monitor_stop_event": monitor_stop_event,
        "worker_lock": worker_lock,
        "get_worker_loop": lambda: worker_loop,
        "get_worker_main_task": lambda: worker_main_task,
        "get_worker_thread": lambda: worker_thread,
        "get_monitor_thread": lambda: monitor_thread,
    },
)


if __name__ == "__main__":
    _start_monitor_thread()
    if OPEN_BROWSER:
        threading.Timer(1.0, lambda: webbrowser.open(f"http://{HOST}:{PORT}")).start()
    app.run(host=HOST, port=PORT, debug=False, use_reloader=False)
