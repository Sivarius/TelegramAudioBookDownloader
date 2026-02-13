import sqlite3
from pathlib import Path
from typing import Optional

from core.models import Settings


class AppDatabase:
    def __init__(self, db_path: Path) -> None:
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
        self._conn.commit()

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

    def already_downloaded(self, channel_id: int, message_id: int) -> bool:
        cur = self._conn.execute(
            "SELECT 1 FROM downloads WHERE channel_id = ? AND message_id = ?",
            (channel_id, message_id),
        )
        return cur.fetchone() is not None

    def mark_downloaded(self, channel_id: int, message_id: int, file_path: str) -> None:
        self._conn.execute(
            "INSERT OR IGNORE INTO downloads(channel_id, message_id, file_path) VALUES (?, ?, ?)",
            (channel_id, message_id, file_path),
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
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()
