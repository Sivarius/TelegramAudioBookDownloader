import asyncio
import logging
import os
from pathlib import Path

from flask import request

from core.db import AppDatabase
from core.ftps_client import FTPSSync
from core.models import Settings
from core.sftp_client import SFTPSync
from core.telegram_client import create_telegram_client


def _safe_int(value: str, default: int = 0) -> int:
    try:
        return int(value)
    except ValueError:
        return default


def _load_saved_form(db_path: Path) -> dict:
    db = AppDatabase(db_path)
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
            "ftps_encoding": db.get_setting("FTPS_ENCODING") or "auto",
            "ftps_verify_tls": (db.get_setting("FTPS_VERIFY_TLS") or "1") == "1",
            "ftps_passive_mode": (db.get_setting("FTPS_PASSIVE_MODE") or "1") == "1",
            "ftps_security_mode": db.get_setting("FTPS_SECURITY_MODE") or "explicit",
            "ftps_upload_concurrency": db.get_setting("FTPS_UPLOAD_CONCURRENCY") or "2",
            "cleanup_local_after_ftps": (db.get_setting("CLEANUP_LOCAL_AFTER_FTPS") or "0") == "1",
            "ftps_verify_hash": (db.get_setting("FTPS_VERIFY_HASH") or "1") == "1",
            "download_new": False,
            "remember_me": (db.get_setting("REMEMBER_ME") or "1") != "0",
            "enable_periodic_checks": (db.get_setting("ENABLE_PERIODIC_CHECKS") or "0") == "1",
        }
    finally:
        db.close()


def _form_from_request(db_path: Path) -> dict:
    saved = _load_saved_form(db_path)
    full_form_submit = any(
        key in request.form
        for key in (
            "api_id",
            "api_hash",
            "phone",
            "channel_id",
            "download_dir",
            "session_name",
            "startup_scan_limit",
            "download_concurrency",
        )
    )

    def _checkbox(name: str) -> bool:
        if full_form_submit:
            return request.form.get(name) == "on"
        return bool(saved.get(name, False))

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
        "use_mtproxy": _checkbox("use_mtproxy"),
        "mtproxy_link": request.form.get("mtproxy_link", saved["mtproxy_link"]).strip(),
        "use_sftp": _checkbox("use_sftp"),
        "sftp_host": request.form.get("sftp_host", saved["sftp_host"]).strip(),
        "sftp_port": request.form.get("sftp_port", saved["sftp_port"]).strip(),
        "sftp_username": request.form.get("sftp_username", saved["sftp_username"]).strip(),
        "sftp_password": request.form.get("sftp_password", saved["sftp_password"]).strip(),
        "sftp_remote_dir": request.form.get("sftp_remote_dir", saved["sftp_remote_dir"]).strip(),
        "cleanup_local_after_sftp": _checkbox("cleanup_local_after_sftp"),
        "use_ftps": _checkbox("use_ftps"),
        "ftps_host": request.form.get("ftps_host", saved["ftps_host"]).strip(),
        "ftps_port": request.form.get("ftps_port", saved["ftps_port"]).strip(),
        "ftps_username": request.form.get("ftps_username", saved["ftps_username"]).strip(),
        "ftps_password": request.form.get("ftps_password", saved["ftps_password"]).strip(),
        "ftps_remote_dir": request.form.get("ftps_remote_dir", saved["ftps_remote_dir"]).strip(),
        "ftps_encoding": request.form.get("ftps_encoding", saved["ftps_encoding"]).strip().lower(),
        "ftps_verify_tls": _checkbox("ftps_verify_tls"),
        "ftps_passive_mode": _checkbox("ftps_passive_mode"),
        "ftps_security_mode": request.form.get("ftps_security_mode", saved["ftps_security_mode"]).strip().lower(),
        "ftps_upload_concurrency": request.form.get("ftps_upload_concurrency", saved["ftps_upload_concurrency"]).strip(),
        "cleanup_local_after_ftps": _checkbox("cleanup_local_after_ftps"),
        "ftps_verify_hash": _checkbox("ftps_verify_hash"),
        "download_new": _checkbox("download_new"),
        "remember_me": _checkbox("remember_me"),
        "enable_periodic_checks": _checkbox("enable_periodic_checks"),
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
    ftps_encoding = form["ftps_encoding"] if form["ftps_encoding"] in {"auto", "utf-8", "cp1251", "latin-1"} else "auto"
    ftps_security_mode = form["ftps_security_mode"] if form["ftps_security_mode"] in {"explicit", "implicit"} else "explicit"
    ftps_upload_concurrency = _safe_int(form["ftps_upload_concurrency"], 2)
    if ftps_upload_concurrency < 1:
        ftps_upload_concurrency = 1
    if form["use_ftps"] and (not form["ftps_host"] or not form["ftps_username"]):
        raise ValueError("Для FTPS заполните host и username.")
    if form["use_sftp"] and form["use_ftps"]:
        raise ValueError("Одновременно можно включить только один протокол: SFTP или FTPS.")
    ftps_passive_mode = bool(form["ftps_passive_mode"])
    if form["use_ftps"] and not ftps_passive_mode:
        # Active mode is not supported by current aioftp flow.
        ftps_passive_mode = True

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
        ftps_encoding=ftps_encoding,
        ftps_verify_tls=bool(form["ftps_verify_tls"]),
        ftps_passive_mode=ftps_passive_mode,
        ftps_security_mode=ftps_security_mode,
        ftps_upload_concurrency=ftps_upload_concurrency,
        cleanup_local_after_ftps=bool(form["cleanup_local_after_ftps"]),
        ftps_verify_hash=bool(form["ftps_verify_hash"]),
    )


def _store_settings(db_path: Path, settings: Settings) -> None:
    if settings.use_ftps and not settings.ftps_passive_mode:
        settings.ftps_passive_mode = True
    db = AppDatabase(db_path)
    try:
        db.store_settings(settings)
    finally:
        db.close()


def _store_remember_me(db_path: Path, remember_me: bool) -> None:
    db = AppDatabase(db_path)
    try:
        db.set_setting("REMEMBER_ME", "1" if remember_me else "0")
    finally:
        db.close()


def _store_enable_periodic_checks(db_path: Path, enabled: bool) -> None:
    db = AppDatabase(db_path)
    try:
        db.set_setting("ENABLE_PERIODIC_CHECKS", "1" if enabled else "0")
    finally:
        db.close()


def _is_periodic_checks_enabled(db_path: Path) -> bool:
    db = AppDatabase(db_path)
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


def _save_phone_code_hash(db_path: Path, phone: str, phone_code_hash: str) -> None:
    db = AppDatabase(db_path)
    try:
        db.set_setting("AUTH_PHONE", phone)
        db.set_setting("PHONE_CODE_HASH", phone_code_hash)
    finally:
        db.close()


def _load_phone_code_hash(db_path: Path, phone: str) -> str:
    db = AppDatabase(db_path)
    try:
        saved_phone = (db.get_setting("AUTH_PHONE") or "").strip()
        if saved_phone != phone:
            return ""
        return (db.get_setting("PHONE_CODE_HASH") or "").strip()
    finally:
        db.close()


def _clear_phone_code_hash(db_path: Path) -> None:
    db = AppDatabase(db_path)
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
