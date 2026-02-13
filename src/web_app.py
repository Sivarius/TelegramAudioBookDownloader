import asyncio
import logging
import threading
import webbrowser
from datetime import datetime
from pathlib import Path
from typing import Optional

from flask import Flask, jsonify, render_template, request
from telethon.errors import PhoneCodeExpiredError, PhoneCodeInvalidError, SessionPasswordNeededError

from core.config import setup_logging
from core.db import AppDatabase
from core.downloader import run_downloader
from core.models import Settings
from core.telegram_client import create_telegram_client, is_audio_message, resolve_channel_entity


DB_PATH = Path("bot_data.sqlite3")
HOST = "127.0.0.1"
PORT = 8080
PREVIEW_LIMIT = 300

app = Flask(__name__)
setup_logging()

worker_lock = threading.Lock()
worker_thread: Optional[threading.Thread] = None
worker_stop_event = threading.Event()
runtime_session_name = ""
runtime_remember_me = True
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


def _set_status(**kwargs) -> None:
    worker_status.update(kwargs)
    worker_status["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _set_auth_status(authorized: bool, message: str) -> None:
    auth_status["authorized"] = authorized
    auth_status["message"] = message
    auth_status["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _set_proxy_status(enabled: bool, available: bool, message: str) -> None:
    proxy_status["enabled"] = enabled
    proxy_status["available"] = available
    proxy_status["message"] = message
    proxy_status["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


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
            "session_name": db.get_setting("SESSION_NAME") or "user_session",
            "startup_scan_limit": db.get_setting("STARTUP_SCAN_LIMIT") or "200",
            "download_concurrency": db.get_setting("DOWNLOAD_CONCURRENCY") or "3",
            "code": "",
            "password": "",
            "from_index": "",
            "to_index": "",
            "use_mtproxy": (db.get_setting("USE_MTPROXY") or "0") == "1",
            "mtproxy_link": db.get_setting("MTPROXY_LINK") or "",
            "remember_me": (db.get_setting("REMEMBER_ME") or "1") != "0",
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
        "remember_me": request.form.get("remember_me") == "on",
    }


def _build_settings(form: dict) -> Settings:
    if not form["api_id"] or not form["api_hash"] or not form["phone"] or not form["channel_id"]:
        raise ValueError("Заполните API_ID, API_HASH, PHONE и CHANNEL_ID.")

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

    return Settings(
        api_id=api_id,
        api_hash=form["api_hash"],
        phone=form["phone"],
        channel=form["channel_id"],
        download_dir=Path(form["download_dir"] or "downloads").expanduser().resolve(),
        session_name=form["session_name"],
        startup_scan_limit=scan_limit,
        download_concurrency=download_concurrency,
        use_mtproxy=bool(form["use_mtproxy"]),
        mtproxy_link=form["mtproxy_link"],
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

        items: list[dict] = []
        index = 1
        async for message in client.iter_messages(channel, limit=limit, reverse=True):
            if not is_audio_message(message):
                continue

            file_name = ""
            if message.file and getattr(message.file, "name", None):
                file_name = str(message.file.name)

            title = file_name or (message.message or "audio")
            date_text = message.date.strftime("%Y-%m-%d %H:%M") if message.date else ""
            items.append(
                {
                    "index": index,
                    "message_id": message.id,
                    "title": title.replace("\n", " ")[:80],
                    "date": date_text,
                }
            )
            index += 1

        return True, f"Найдено аудио: {len(items)}", items
    except Exception as exc:
        return False, f"Ошибка предпросмотра: {exc}", []
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


def _start_worker(settings: Settings, allowed_message_ids: Optional[set[int]]) -> tuple[bool, str]:
    global worker_thread

    with worker_lock:
        if worker_thread and worker_thread.is_alive():
            return False, "Скачивание уже запущено."

        worker_stop_event.clear()
        _set_status(
            running=True,
            message="Скачивание запущено.",
            downloaded=0,
            failed=0,
            skipped=0,
            current_message_id="",
            last_file="",
            configured_concurrency=settings.download_concurrency,
            current_concurrency=settings.download_concurrency,
        )

        def _status_hook(payload: dict) -> None:
            event = payload.get("event", "")
            message_id = str(payload.get("message_id", ""))
            if event == "downloading":
                _set_status(current_message_id=message_id, message=f"Скачивание message_id={message_id}")
            elif event == "downloaded":
                _set_status(
                    downloaded=int(worker_status.get("downloaded", 0)) + 1,
                    current_message_id=message_id,
                    last_file=str(payload.get("file_path", "")),
                    message=f"Скачан message_id={message_id}",
                )
            elif event == "failed":
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

        def _target() -> None:
            try:
                asyncio.run(
                    run_downloader(
                        settings=settings,
                        db_path=DB_PATH,
                        allowed_message_ids=allowed_message_ids,
                        status_hook=_status_hook,
                        stop_requested=lambda: worker_stop_event.is_set(),
                    )
                )
            except Exception as exc:
                logging.exception("Downloader crashed")
                _set_status(message=f"Ошибка загрузчика: {exc}", running=False)
            finally:
                _set_status(running=False)

        worker_thread = threading.Thread(target=_target, daemon=True)
        worker_thread.start()

    return True, "Загрузчик запущен."


def _render(form: dict, message: str = "", need_code: bool = False, need_password: bool = False):
    return render_template(
        "index.html",
        form=form,
        status=worker_status,
        auth_status=auth_status,
        proxy_status=proxy_status,
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
    return jsonify(
        {
            **worker_status,
            "auth_authorized": auth_status["authorized"],
            "auth_message": auth_status["message"],
            "auth_updated_at": auth_status["updated_at"],
            "proxy_enabled": proxy_status["enabled"],
            "proxy_available": proxy_status["available"],
            "proxy_message": proxy_status["message"],
            "proxy_updated_at": proxy_status["updated_at"],
        }
    )


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
    if not runtime_remember_me:
        _delete_session_files(settings.session_name)

    proxy_ok, proxy_message = asyncio.run(_validate_proxy(settings))
    if settings.use_mtproxy:
        _set_proxy_status(True, proxy_ok, proxy_message)
        if not proxy_ok:
            return _render(form, proxy_message)
    else:
        _set_proxy_status(False, True, "MTProxy не используется.")

    _store_settings(settings)

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

    return _render(form, f"{proxy_status['message']} {auth_message} {preview_message}")


@app.post("/preview")
def refresh_preview():
    form = _form_from_request()

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

    preview_ok, preview_message, items = asyncio.run(_fetch_preview(settings))
    global preview_cache
    preview_cache = items if preview_ok else []

    return _render(form, f"{proxy_status['message']} {preview_message}")


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

    proxy_ok, proxy_message = asyncio.run(_validate_proxy(settings))
    if settings.use_mtproxy:
        _set_proxy_status(True, proxy_ok, proxy_message)
        if not proxy_ok:
            return _render(form, proxy_message)
    else:
        _set_proxy_status(False, True, "MTProxy не используется.")

    _store_settings(settings)

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

    started, start_message = _start_worker(settings, allowed_message_ids)
    if started:
        return _render(form, f"{proxy_status['message']} {channel_message} {start_message}")
    return _render(form, start_message)


@app.post("/stop_server")
def stop_server():
    global worker_thread

    worker_stop_event.set()
    _set_status(message="Остановка сервера...", running=False)

    if worker_thread and worker_thread.is_alive():
        worker_thread.join(timeout=10)

    if not runtime_remember_me:
        _delete_session_files(runtime_session_name)

    shutdown_func = request.environ.get("werkzeug.server.shutdown")
    if shutdown_func is not None:
        shutdown_func()
        return "Сервер остановлен."

    return "Не удалось остановить сервер через Werkzeug hook.", 500


if __name__ == "__main__":
    threading.Timer(1.0, lambda: webbrowser.open(f"http://{HOST}:{PORT}")).start()
    app.run(host=HOST, port=PORT, debug=False, use_reloader=False)
