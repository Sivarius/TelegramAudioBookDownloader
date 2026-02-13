import logging
import os
import re
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, urlparse

from dotenv import load_dotenv

from core.db import AppDatabase
from core.models import Settings


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def _env_or_db(env_key: str, db: AppDatabase, db_key: Optional[str] = None) -> str:
    value = os.getenv(env_key, "").strip()
    if value:
        return value

    db_value = db.get_setting(db_key or env_key)
    return db_value.strip() if db_value else ""


def _require_non_empty(name: str, value: str) -> str:
    if not value:
        raise ValueError(f"Missing required setting: {name}")
    return value


def _to_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def parse_mtproxy_link(proxy_link: str) -> tuple[str, int, str]:
    raw = proxy_link.strip()
    if not raw:
        raise ValueError("MTPROXY_LINK is empty.")

    query = raw
    if "://" in raw:
        parsed = urlparse(raw)
        query = parsed.query or raw

    params = parse_qs(query.lstrip("?"), keep_blank_values=True)
    server = (params.get("server", [""])[0] or "").strip()
    port_raw = (params.get("port", [""])[0] or "").strip()
    secret = (params.get("secret", [""])[0] or "").strip()

    if not server or not port_raw or not secret:
        raise ValueError(
            "Invalid MTPROXY_LINK. Expected format: server=HOST&port=PORT&secret=SECRET"
        )

    try:
        port = int(port_raw)
    except ValueError as exc:
        raise ValueError("Invalid MTProxy port in MTPROXY_LINK.") from exc

    if port <= 0 or port > 65535:
        raise ValueError("Invalid MTProxy port range in MTPROXY_LINK.")

    return server, port, secret


def sanitize_folder_name(name: str) -> str:
    cleaned = re.sub(r'[<>:"/\\|?*]+', "_", name).strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    if not cleaned:
        cleaned = "channel"
    return cleaned[:120]


def load_settings(db: AppDatabase) -> Settings:
    load_dotenv()

    api_id_raw = _require_non_empty("API_ID", _env_or_db("API_ID", db))
    api_hash = _require_non_empty("API_HASH", _env_or_db("API_HASH", db))
    phone = _require_non_empty("PHONE", _env_or_db("PHONE", db))

    channel = _env_or_db("CHANNEL_ID", db, "CHANNEL_ID")
    if not channel:
        channel = _env_or_db("CHANNEL", db, "CHANNEL_ID")
    channel = _require_non_empty("CHANNEL_ID", channel)

    try:
        api_id = int(api_id_raw)
    except ValueError as exc:
        raise ValueError("API_ID must be an integer") from exc

    default_download_dir = _env_or_db("DOWNLOAD_DIR", db) or "downloads"
    session_name = _env_or_db("SESSION_NAME", db) or "user_session"

    startup_scan_limit_raw = _env_or_db("STARTUP_SCAN_LIMIT", db) or "200"
    try:
        startup_scan_limit = int(startup_scan_limit_raw)
    except ValueError as exc:
        raise ValueError("STARTUP_SCAN_LIMIT must be an integer") from exc

    if startup_scan_limit < 0:
        raise ValueError("STARTUP_SCAN_LIMIT must be >= 0")

    download_concurrency_raw = _env_or_db("DOWNLOAD_CONCURRENCY", db) or "3"
    try:
        download_concurrency = int(download_concurrency_raw)
    except ValueError as exc:
        raise ValueError("DOWNLOAD_CONCURRENCY must be an integer") from exc
    if download_concurrency < 1:
        raise ValueError("DOWNLOAD_CONCURRENCY must be >= 1")

    use_mtproxy = _to_bool(_env_or_db("USE_MTPROXY", db) or "0")
    mtproxy_link = _env_or_db("MTPROXY_LINK", db)

    if use_mtproxy and not mtproxy_link:
        raise ValueError("USE_MTPROXY is enabled but MTPROXY_LINK is empty.")

    use_sftp = _to_bool(_env_or_db("USE_SFTP", db) or "0")
    sftp_host = _env_or_db("SFTP_HOST", db)
    sftp_port_raw = _env_or_db("SFTP_PORT", db) or "22"
    sftp_username = _env_or_db("SFTP_USERNAME", db)
    sftp_password = _env_or_db("SFTP_PASSWORD", db)
    sftp_remote_dir = _env_or_db("SFTP_REMOTE_DIR", db) or "/uploads"
    cleanup_local_after_sftp = _to_bool(
        _env_or_db("CLEANUP_LOCAL_AFTER_SFTP", db) or "0"
    )
    try:
        sftp_port = int(sftp_port_raw)
    except ValueError as exc:
        raise ValueError("SFTP_PORT must be an integer") from exc

    if use_sftp and (not sftp_host or not sftp_username):
        raise ValueError("USE_SFTP is enabled but SFTP_HOST/SFTP_USERNAME is empty.")

    use_ftps = _to_bool(_env_or_db("USE_FTPS", db) or "0")
    ftps_host = _env_or_db("FTPS_HOST", db)
    ftps_port_raw = _env_or_db("FTPS_PORT", db) or "21"
    ftps_username = _env_or_db("FTPS_USERNAME", db)
    ftps_password = _env_or_db("FTPS_PASSWORD", db)
    ftps_remote_dir = _env_or_db("FTPS_REMOTE_DIR", db) or "/uploads"
    cleanup_local_after_ftps = _to_bool(
        _env_or_db("CLEANUP_LOCAL_AFTER_FTPS", db) or "0"
    )
    try:
        ftps_port = int(ftps_port_raw)
    except ValueError as exc:
        raise ValueError("FTPS_PORT must be an integer") from exc

    if use_ftps and (not ftps_host or not ftps_username):
        raise ValueError("USE_FTPS is enabled but FTPS_HOST/FTPS_USERNAME is empty.")

    if use_sftp and use_ftps:
        raise ValueError("USE_SFTP and USE_FTPS cannot be enabled together.")

    return Settings(
        api_id=api_id,
        api_hash=api_hash,
        phone=phone,
        channel=channel,
        download_dir=Path(default_download_dir).resolve(),
        session_name=session_name,
        startup_scan_limit=startup_scan_limit,
        download_concurrency=download_concurrency,
        use_mtproxy=use_mtproxy,
        mtproxy_link=mtproxy_link,
        use_sftp=use_sftp,
        sftp_host=sftp_host,
        sftp_port=sftp_port,
        sftp_username=sftp_username,
        sftp_password=sftp_password,
        sftp_remote_dir=sftp_remote_dir,
        cleanup_local_after_sftp=cleanup_local_after_sftp,
        use_ftps=use_ftps,
        ftps_host=ftps_host,
        ftps_port=ftps_port,
        ftps_username=ftps_username,
        ftps_password=ftps_password,
        ftps_remote_dir=ftps_remote_dir,
        cleanup_local_after_ftps=cleanup_local_after_ftps,
    )
