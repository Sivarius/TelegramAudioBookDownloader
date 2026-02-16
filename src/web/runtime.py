import logging
import threading
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Optional


debug_log_lock = threading.Lock()
debug_log_buffer: deque[str] = deque(maxlen=400)
debug_mode_enabled = False
debug_file_handler: Optional[logging.Handler] = None
debug_file_path = Path("bot_data.sqlite3").parent / "debug.log.txt"

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
    "ftps_check_running": False,
    "ftps_check_checked": 0,
    "ftps_check_total": 0,
    "ftps_check_verified": 0,
    "ftps_check_missing": 0,
    "ftps_check_failed": 0,
    "ftps_check_cleaned": 0,
    "ftps_check_current_file": "",
    "ftps_check_last_info": "",
    "ftps_check_missing_examples": [],
    "file_progresses": {},
    "upload_file_progresses": {},
    "mode": "idle",
}
preview_cache: list[dict] = []
ftps_remote_preview_cache: list[dict] = []
ftps_remote_preview_meta: str = ""
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


class InMemoryLogHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
        except Exception:
            msg = record.getMessage()
        with debug_log_lock:
            debug_log_buffer.append(msg)


class SuppressDebugLogsAccessFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        if "/debug_logs" in message:
            return False
        return True


def configure_runtime_paths(db_path: Path) -> None:
    global debug_file_path
    debug_file_path = db_path.parent / "debug.log.txt"
    _setup_debug_log_handler()


def _setup_debug_log_handler() -> None:
    root = logging.getLogger()
    if any(isinstance(h, InMemoryLogHandler) for h in root.handlers):
        return
    handler = InMemoryLogHandler()
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    root.addHandler(handler)
    werkzeug_logger = logging.getLogger("werkzeug")
    if not any(isinstance(f, SuppressDebugLogsAccessFilter) for f in werkzeug_logger.filters):
        werkzeug_logger.addFilter(SuppressDebugLogsAccessFilter())


def _set_debug_mode(enabled: bool) -> None:
    global debug_mode_enabled, debug_file_handler
    root = logging.getLogger()
    debug_mode_enabled = bool(enabled)
    if debug_mode_enabled:
        if debug_file_handler is None:
            debug_file_path.parent.mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(debug_file_path, encoding="utf-8")
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(
                logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
            )
            root.addHandler(file_handler)
            debug_file_handler = file_handler
        root.setLevel(logging.DEBUG)
    else:
        if debug_file_handler is not None:
            try:
                root.removeHandler(debug_file_handler)
                debug_file_handler.close()
            except Exception:
                pass
            debug_file_handler = None
        root.setLevel(logging.INFO)


def _inc_status_version() -> None:
    global status_version
    with status_cond:
        status_version += 1
        status_cond.notify_all()


def _set_status(**kwargs) -> None:
    worker_status.update(kwargs)
    worker_status["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _inc_status_version()


def _set_auth_status(authorized: bool, message: str) -> None:
    auth_status["authorized"] = authorized
    auth_status["message"] = message
    auth_status["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _inc_status_version()


def _set_proxy_status(enabled: bool, available: bool, message: str) -> None:
    proxy_status["enabled"] = enabled
    proxy_status["available"] = available
    proxy_status["message"] = message
    proxy_status["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _inc_status_version()


def _set_sftp_status(enabled: bool, available: bool, message: str) -> None:
    sftp_status["enabled"] = enabled
    sftp_status["available"] = available
    sftp_status["message"] = message
    sftp_status["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _inc_status_version()


def _set_ftps_status(enabled: bool, available: bool, message: str) -> None:
    ftps_status["enabled"] = enabled
    ftps_status["available"] = available
    ftps_status["message"] = message
    ftps_status["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _inc_status_version()


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


def _set_preview_cache(value: list[dict]) -> None:
    preview_cache.clear()
    preview_cache.extend(value or [])


def _set_ftps_remote_preview_cache(value: list[dict]) -> None:
    ftps_remote_preview_cache.clear()
    ftps_remote_preview_cache.extend(value or [])


def _set_ftps_remote_preview_meta(value: str) -> None:
    global ftps_remote_preview_meta
    ftps_remote_preview_meta = str(value or "")


def get_status_version() -> int:
    return int(status_version)
