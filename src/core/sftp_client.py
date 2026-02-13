import os
import posixpath
import threading
from typing import Optional

import paramiko

from core.models import Settings


class SFTPSync:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._ssh: Optional[paramiko.SSHClient] = None
        self._sftp: Optional[paramiko.SFTPClient] = None
        self._remote_channel_dir = ""
        self._remote_file_names: set[str] = set()
        self._io_lock = threading.Lock()

    @property
    def enabled(self) -> bool:
        return bool(self.settings.use_sftp)

    @property
    def remote_channel_dir(self) -> str:
        return self._remote_channel_dir

    def connect(self) -> None:
        if not self.enabled:
            return

        if not self.settings.sftp_host or not self.settings.sftp_username:
            raise ValueError("SFTP включен, но не заполнены host/username.")

        if self.settings.sftp_port <= 0 or self.settings.sftp_port > 65535:
            raise ValueError("SFTP_PORT должен быть в диапазоне 1..65535.")

        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(
            hostname=self.settings.sftp_host,
            port=self.settings.sftp_port,
            username=self.settings.sftp_username,
            password=self.settings.sftp_password or None,
            timeout=12,
            banner_timeout=12,
            auth_timeout=12,
        )
        self._ssh = ssh
        self._sftp = ssh.open_sftp()

    def close(self) -> None:
        if self._sftp:
            self._sftp.close()
            self._sftp = None
        if self._ssh:
            self._ssh.close()
            self._ssh = None

    def validate_connection(self) -> tuple[bool, str]:
        if not self.enabled:
            return True, "SFTP выключен."

        try:
            self.connect()
            base_remote = (self.settings.sftp_remote_dir or "/").strip() or "/"
            self.ensure_remote_dir(base_remote)
            return True, f"SFTP: подключение установлено. Каталог доступен: {base_remote}"
        except Exception as exc:
            return False, f"SFTP: ошибка подключения ({exc})"
        finally:
            self.close()

    def prepare_channel_dir(self, channel_folder: str) -> None:
        if not self.enabled:
            return
        if not self._sftp:
            raise RuntimeError("SFTP is not connected.")

        base_remote = (self.settings.sftp_remote_dir or "/").strip() or "/"
        self.ensure_remote_dir(base_remote)
        channel_remote = posixpath.join(base_remote.rstrip("/"), channel_folder)
        self.ensure_remote_dir(channel_remote)
        self._remote_channel_dir = channel_remote
        self._remote_file_names = set(self._sftp.listdir(channel_remote))

    def ensure_remote_dir(self, remote_dir: str) -> None:
        if not self._sftp:
            raise RuntimeError("SFTP is not connected.")

        path = remote_dir.strip()
        if not path:
            return

        is_abs = path.startswith("/")
        parts = [p for p in path.split("/") if p]
        current = "/" if is_abs else ""

        for part in parts:
            current = posixpath.join(current, part) if current else part
            try:
                self._sftp.stat(current)
            except IOError:
                self._sftp.mkdir(current)

    def upload_file_if_needed(self, local_file_path: str) -> tuple[bool, str]:
        if not self.enabled:
            return False, "SFTP выключен."
        if not self._sftp:
            return False, "SFTP не подключен."
        if not self._remote_channel_dir:
            return False, "Не подготовлен удаленный каталог."

        file_name = os.path.basename(local_file_path)
        if not file_name:
            return False, "Пустое имя файла."

        with self._io_lock:
            if file_name in self._remote_file_names:
                return False, "Файл уже есть на SFTP."

            remote_path = posixpath.join(self._remote_channel_dir, file_name)
            self._sftp.put(local_file_path, remote_path)
            self._remote_file_names.add(file_name)
            return True, remote_path
