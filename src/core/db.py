import sqlite3
from pathlib import Path
from typing import Optional

from core.models import Settings


class AppDatabase:
    def __init__(self, db_path: Path) -> None:
        db_path = Path(db_path)
        # Guard against Docker bind-mount edge case where a missing host file
        # may become a directory inside container.
        if db_path.exists() and db_path.is_dir():
            db_path = db_path / "bot_data.sqlite3"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(db_path)
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS downloads (
                channel_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                file_path TEXT,
                downloaded_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (channel_id, message_id)
            )
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS app_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS channel_state (
                channel_id INTEGER PRIMARY KEY,
                channel_ref TEXT,
                channel_title TEXT,
                download_folder TEXT,
                last_message_id INTEGER NOT NULL DEFAULT 0,
                last_file_path TEXT,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS channel_preferences (
                channel_ref TEXT PRIMARY KEY,
                channel_id INTEGER,
                channel_title TEXT,
                check_new INTEGER NOT NULL DEFAULT 0,
                auto_download INTEGER NOT NULL DEFAULT 0,
                auto_sftp INTEGER NOT NULL DEFAULT 0,
                auto_ftps INTEGER NOT NULL DEFAULT 0,
                cleanup_local INTEGER NOT NULL DEFAULT 0,
                last_checked_at TEXT,
                has_new_audio INTEGER NOT NULL DEFAULT 0,
                latest_audio_id INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        self._ensure_channel_preferences_columns()
        self._conn.commit()

    def _ensure_channel_preferences_columns(self) -> None:
        cur = self._conn.execute("PRAGMA table_info(channel_preferences)")
        columns = {str(row[1]) for row in cur.fetchall()}
        if "auto_ftps" not in columns:
            self._conn.execute(
                "ALTER TABLE channel_preferences ADD COLUMN auto_ftps INTEGER NOT NULL DEFAULT 0"
            )

    def get_setting(self, key: str) -> Optional[str]:
        cur = self._conn.execute("SELECT value FROM app_settings WHERE key = ?", (key,))
        row = cur.fetchone()
        return row[0] if row else None

    def set_setting(self, key: str, value: str) -> None:
        self._conn.execute(
            """
            INSERT INTO app_settings(key, value, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = CURRENT_TIMESTAMP
            """,
            (key, value),
        )
        self._conn.commit()

    def store_settings(self, settings: Settings) -> None:
        self.set_setting("API_ID", str(settings.api_id))
        self.set_setting("API_HASH", settings.api_hash)
        self.set_setting("PHONE", settings.phone)
        self.set_setting("CHANNEL_ID", settings.channel)
        self.set_setting("DOWNLOAD_DIR", str(settings.download_dir))
        self.set_setting("SESSION_NAME", settings.session_name)
        self.set_setting("STARTUP_SCAN_LIMIT", str(settings.startup_scan_limit))
        self.set_setting("DOWNLOAD_CONCURRENCY", str(settings.download_concurrency))
        self.set_setting("USE_MTPROXY", "1" if settings.use_mtproxy else "0")
        self.set_setting("MTPROXY_LINK", settings.mtproxy_link)
        self.set_setting("USE_SFTP", "1" if settings.use_sftp else "0")
        self.set_setting("SFTP_HOST", settings.sftp_host)
        self.set_setting("SFTP_PORT", str(settings.sftp_port))
        self.set_setting("SFTP_USERNAME", settings.sftp_username)
        self.set_setting("SFTP_PASSWORD", settings.sftp_password)
        self.set_setting("SFTP_REMOTE_DIR", settings.sftp_remote_dir)
        self.set_setting(
            "CLEANUP_LOCAL_AFTER_SFTP",
            "1" if settings.cleanup_local_after_sftp else "0",
        )
        self.set_setting("USE_FTPS", "1" if settings.use_ftps else "0")
        self.set_setting("FTPS_HOST", settings.ftps_host)
        self.set_setting("FTPS_PORT", str(settings.ftps_port))
        self.set_setting("FTPS_USERNAME", settings.ftps_username)
        self.set_setting("FTPS_PASSWORD", settings.ftps_password)
        self.set_setting("FTPS_REMOTE_DIR", settings.ftps_remote_dir)
        self.set_setting("FTPS_VERIFY_TLS", "1" if settings.ftps_verify_tls else "0")
        self.set_setting("FTPS_PASSIVE_MODE", "1" if settings.ftps_passive_mode else "0")
        self.set_setting("FTPS_SECURITY_MODE", settings.ftps_security_mode)
        self.set_setting("FTPS_UPLOAD_CONCURRENCY", str(settings.ftps_upload_concurrency))
        self.set_setting(
            "CLEANUP_LOCAL_AFTER_FTPS",
            "1" if settings.cleanup_local_after_ftps else "0",
        )

    def already_downloaded(self, channel_id: int, message_id: int) -> bool:
        cur = self._conn.execute(
            "SELECT 1 FROM downloads WHERE channel_id = ? AND message_id = ?",
            (channel_id, message_id),
        )
        return cur.fetchone() is not None

    def mark_downloaded(self, channel_id: int, message_id: int, file_path: str) -> None:
        self._conn.execute(
            """
            INSERT INTO downloads(channel_id, message_id, file_path, downloaded_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(channel_id, message_id) DO UPDATE SET
                file_path = excluded.file_path,
                downloaded_at = CURRENT_TIMESTAMP
            """,
            (channel_id, message_id, file_path),
        )
        self._conn.commit()

    def get_downloaded_file_path(self, channel_id: int, message_id: int) -> str:
        cur = self._conn.execute(
            "SELECT file_path FROM downloads WHERE channel_id = ? AND message_id = ?",
            (channel_id, message_id),
        )
        row = cur.fetchone()
        return (row[0] or "").strip() if row else ""

    def unmark_downloaded(self, channel_id: int, message_id: int) -> None:
        self._conn.execute(
            "DELETE FROM downloads WHERE channel_id = ? AND message_id = ?",
            (channel_id, message_id),
        )
        self._conn.commit()

    def get_last_downloaded_message_id(self, channel_id: int) -> int:
        cur = self._conn.execute(
            "SELECT last_message_id FROM channel_state WHERE channel_id = ?",
            (channel_id,),
        )
        row = cur.fetchone()
        if row and row[0]:
            return int(row[0])

        cur = self._conn.execute(
            "SELECT COALESCE(MAX(message_id), 0) FROM downloads WHERE channel_id = ?",
            (channel_id,),
        )
        row = cur.fetchone()
        return int(row[0]) if row and row[0] else 0

    def update_channel_state(
        self,
        channel_id: int,
        channel_ref: str,
        channel_title: str,
        download_folder: str,
        last_message_id: int,
        last_file_path: str,
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO channel_state(
                channel_id, channel_ref, channel_title, download_folder,
                last_message_id, last_file_path, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(channel_id) DO UPDATE SET
                channel_ref = excluded.channel_ref,
                channel_title = excluded.channel_title,
                download_folder = excluded.download_folder,
                last_message_id = CASE
                    WHEN excluded.last_message_id > channel_state.last_message_id
                    THEN excluded.last_message_id
                    ELSE channel_state.last_message_id
                END,
                last_file_path = CASE
                    WHEN excluded.last_message_id > channel_state.last_message_id
                    THEN excluded.last_file_path
                    ELSE channel_state.last_file_path
                END,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                channel_id,
                channel_ref,
                channel_title,
                download_folder,
                last_message_id,
                last_file_path,
            ),
        )
        self.ensure_channel_preferences(channel_id, channel_ref, channel_title)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def list_channel_states(self) -> list[dict]:
        cur = self._conn.execute(
            """
            SELECT channel_id, channel_ref, channel_title, last_message_id, updated_at
            FROM channel_state
            ORDER BY updated_at DESC
            """
        )
        rows = cur.fetchall()
        result: list[dict] = []
        for row in rows:
            result.append(
                {
                    "channel_id": int(row[0]),
                    "channel_ref": row[1] or "",
                    "channel_title": row[2] or "",
                    "last_message_id": int(row[3] or 0),
                    "updated_at": row[4] or "",
                }
            )
        return result

    def ensure_channel_preferences(
        self, channel_id: int, channel_ref: str, channel_title: str
    ) -> None:
        ref = (channel_ref or "").strip()
        if not ref:
            return
        self._conn.execute(
            """
            INSERT INTO channel_preferences(
                channel_ref, channel_id, channel_title, updated_at
            )
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(channel_ref) DO UPDATE SET
                channel_id = excluded.channel_id,
                channel_title = CASE
                    WHEN excluded.channel_title <> '' THEN excluded.channel_title
                    ELSE channel_preferences.channel_title
                END,
                updated_at = CURRENT_TIMESTAMP
            """,
            (ref, int(channel_id), channel_title or ""),
        )
        self._conn.commit()

    def list_channel_preferences(self) -> list[dict]:
        cur = self._conn.execute(
            """
            SELECT
                channel_ref, channel_id, channel_title,
                check_new, auto_download, auto_sftp, auto_ftps, cleanup_local,
                last_checked_at, has_new_audio, latest_audio_id, last_error, updated_at
            FROM channel_preferences
            ORDER BY updated_at DESC
            """
        )
        rows = cur.fetchall()
        items: list[dict] = []
        for row in rows:
            items.append(
                {
                    "channel_ref": row[0] or "",
                    "channel_id": int(row[1] or 0),
                    "channel_title": row[2] or "",
                    "check_new": bool(int(row[3] or 0)),
                    "auto_download": bool(int(row[4] or 0)),
                    "auto_sftp": bool(int(row[5] or 0)),
                    "auto_ftps": bool(int(row[6] or 0)),
                    "cleanup_local": bool(int(row[7] or 0)),
                    "last_checked_at": row[8] or "",
                    "has_new_audio": bool(int(row[9] or 0)),
                    "latest_audio_id": int(row[10] or 0),
                    "last_error": row[11] or "",
                    "updated_at": row[12] or "",
                }
            )
        return items

    def get_channel_preferences(self, channel_ref: str) -> Optional[dict]:
        ref = (channel_ref or "").strip()
        if not ref:
            return None
        cur = self._conn.execute(
            """
            SELECT
                channel_ref, channel_id, channel_title,
                check_new, auto_download, auto_sftp, auto_ftps, cleanup_local,
                last_checked_at, has_new_audio, latest_audio_id, last_error, updated_at
            FROM channel_preferences
            WHERE channel_ref = ?
            """,
            (ref,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "channel_ref": row[0] or "",
            "channel_id": int(row[1] or 0),
            "channel_title": row[2] or "",
            "check_new": bool(int(row[3] or 0)),
            "auto_download": bool(int(row[4] or 0)),
            "auto_sftp": bool(int(row[5] or 0)),
            "auto_ftps": bool(int(row[6] or 0)),
            "cleanup_local": bool(int(row[7] or 0)),
            "last_checked_at": row[8] or "",
            "has_new_audio": bool(int(row[9] or 0)),
            "latest_audio_id": int(row[10] or 0),
            "last_error": row[11] or "",
            "updated_at": row[12] or "",
        }

    def upsert_channel_preferences(
        self,
        channel_ref: str,
        channel_id: int,
        channel_title: str,
        check_new: bool,
        auto_download: bool,
        auto_sftp: bool,
        auto_ftps: bool,
        cleanup_local: bool,
    ) -> None:
        ref = (channel_ref or "").strip()
        if not ref:
            return
        self._conn.execute(
            """
            INSERT INTO channel_preferences(
                channel_ref, channel_id, channel_title,
                check_new, auto_download, auto_sftp, auto_ftps, cleanup_local, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(channel_ref) DO UPDATE SET
                channel_id = excluded.channel_id,
                channel_title = CASE
                    WHEN excluded.channel_title <> '' THEN excluded.channel_title
                    ELSE channel_preferences.channel_title
                END,
                check_new = excluded.check_new,
                auto_download = excluded.auto_download,
                auto_sftp = excluded.auto_sftp,
                auto_ftps = excluded.auto_ftps,
                cleanup_local = excluded.cleanup_local,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                ref,
                int(channel_id),
                channel_title or "",
                1 if check_new else 0,
                1 if auto_download else 0,
                1 if auto_sftp else 0,
                1 if auto_ftps else 0,
                1 if cleanup_local else 0,
            ),
        )
        self._conn.commit()

    def update_channel_check_status(
        self,
        channel_ref: str,
        has_new_audio: bool,
        latest_audio_id: int,
        last_error: str = "",
    ) -> None:
        ref = (channel_ref or "").strip()
        if not ref:
            return
        self._conn.execute(
            """
            UPDATE channel_preferences
            SET
                has_new_audio = ?,
                latest_audio_id = ?,
                last_error = ?,
                last_checked_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE channel_ref = ?
            """,
            (1 if has_new_audio else 0, int(latest_audio_id), last_error or "", ref),
        )
        self._conn.commit()

    def get_last_message_id_by_channel_ref(self, channel_ref: str) -> int:
        ref = (channel_ref or "").strip()
        if not ref:
            return 0
        cur = self._conn.execute(
            "SELECT last_message_id FROM channel_state WHERE channel_ref = ?",
            (ref,),
        )
        row = cur.fetchone()
        if row and row[0]:
            return int(row[0])
        return 0

    def get_channel_state_by_ref(self, channel_ref: str) -> dict:
        ref = (channel_ref or "").strip()
        if not ref:
            return {"last_message_id": 0, "last_file_path": "", "download_folder": ""}
        cur = self._conn.execute(
            """
            SELECT last_message_id, last_file_path, download_folder
            FROM channel_state
            WHERE channel_ref = ?
            """,
            (ref,),
        )
        row = cur.fetchone()
        if not row:
            return {"last_message_id": 0, "last_file_path": "", "download_folder": ""}
        return {
            "last_message_id": int(row[0] or 0),
            "last_file_path": row[1] or "",
            "download_folder": row[2] or "",
        }
