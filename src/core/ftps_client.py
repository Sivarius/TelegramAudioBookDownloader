import hashlib
import os
import posixpath
import ssl
import threading
from ftplib import FTP_TLS, error_perm
from typing import Optional

from core.models import Settings


class FTPSSync:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._ftps: Optional[FTP_TLS] = None
        self._remote_channel_dir = ""
        self._remote_file_names: set[str] = set()
        self._io_lock = threading.Lock()

    @property
    def enabled(self) -> bool:
        return bool(self.settings.use_ftps)

    @property
    def remote_channel_dir(self) -> str:
        return self._remote_channel_dir

    @property
    def name(self) -> str:
        return "FTPS"

    def connect(self) -> None:
        if not self.enabled:
            return

        if not self.settings.ftps_host or not self.settings.ftps_username:
            raise ValueError("FTPS включен, но не заполнены host/username.")
        if self.settings.ftps_port <= 0 or self.settings.ftps_port > 65535:
            raise ValueError("FTPS_PORT должен быть в диапазоне 1..65535.")

        if self.settings.ftps_verify_tls:
            context = ssl.create_default_context()
        else:
            context = ssl._create_unverified_context()
        ftps = FTP_TLS(context=context, timeout=12)
        ftps.connect(host=self.settings.ftps_host, port=self.settings.ftps_port)
        ftps.login(user=self.settings.ftps_username, passwd=self.settings.ftps_password or "")
        ftps.prot_p()
        ftps.set_pasv(True)
        self._ftps = ftps

    def close(self) -> None:
        if not self._ftps:
            return
        try:
            self._ftps.quit()
        except Exception:
            try:
                self._ftps.close()
            except Exception:
                pass
        self._ftps = None

    def validate_connection(self) -> tuple[bool, str]:
        if not self.enabled:
            return True, "FTPS выключен."
        try:
            self.connect()
            base_remote = (self.settings.ftps_remote_dir or "/").strip() or "/"
            self.ensure_remote_dir(base_remote)
            tls_mode = "verify=on" if self.settings.ftps_verify_tls else "verify=off"
            return True, f"FTPS: подключение установлено ({tls_mode}). Каталог доступен: {base_remote}"
        except ssl.SSLCertVerificationError as exc:
            return (
                False,
                "FTPS: ошибка TLS-сертификата. "
                "Проверьте цепочку сертификатов на сервере или отключите проверку TLS в настройках FTPS. "
                f"({exc})",
            )
        except Exception as exc:
            return False, f"FTPS: ошибка подключения ({exc})"
        finally:
            self.close()

    def prepare_channel_dir(self, channel_folder: str) -> None:
        if not self.enabled:
            return
        if not self._ftps:
            raise RuntimeError("FTPS is not connected.")

        base_remote = (self.settings.ftps_remote_dir or "/").strip() or "/"
        self.ensure_remote_dir(base_remote)
        channel_remote = posixpath.join(base_remote.rstrip("/"), channel_folder)
        self.ensure_remote_dir(channel_remote)
        self._remote_channel_dir = channel_remote
        try:
            names = self._ftps.nlst(channel_remote)
        except Exception:
            names = []
        self._remote_file_names = {posixpath.basename(name.rstrip("/")) for name in names}

    def ensure_remote_dir(self, remote_dir: str) -> None:
        if not self._ftps:
            raise RuntimeError("FTPS is not connected.")
        path = remote_dir.strip()
        if not path:
            return
        is_abs = path.startswith("/")
        parts = [p for p in path.split("/") if p]
        current = "/" if is_abs else ""
        for part in parts:
            current = posixpath.join(current, part) if current else part
            try:
                self._ftps.cwd(current)
            except error_perm:
                self._ftps.mkd(current)
                self._ftps.cwd(current)

    def upload_file_if_needed(self, local_file_path: str) -> tuple[bool, str]:
        if not self.enabled:
            return False, "FTPS выключен."
        if not self._ftps:
            return False, "FTPS не подключен."
        if not self._remote_channel_dir:
            return False, "Не подготовлен удаленный каталог."

        file_name = os.path.basename(local_file_path)
        if not file_name:
            return False, "Пустое имя файла."

        with self._io_lock:
            remote_path = posixpath.join(self._remote_channel_dir, file_name)
            uploaded = False
            if file_name in self._remote_file_names:
                uploaded = False
            else:
                with open(local_file_path, "rb") as file_obj:
                    self._ftps.storbinary(f"STOR {remote_path}", file_obj, blocksize=1024 * 256)
                self._remote_file_names.add(file_name)
                uploaded = True

            size_match, hash_match = self.verify_remote_file(local_file_path, remote_path)
            verified = size_match and hash_match
            reason = "uploaded" if uploaded else "already_exists"
            return (
                uploaded,
                f"{reason}; remote={remote_path}; size_match={size_match}; hash_match={hash_match}; verified={verified}",
            )

    def verify_remote_file(self, local_file_path: str, remote_file_path: str) -> tuple[bool, bool]:
        if not self._ftps:
            raise RuntimeError("FTPS не подключен.")

        local_size = os.path.getsize(local_file_path)
        remote_size = self._ftps.size(remote_file_path) or 0
        size_match = int(remote_size) == int(local_size)

        local_hash = self._sha256_local(local_file_path)
        remote_hash = self._sha256_remote(remote_file_path)
        hash_match = local_hash == remote_hash
        return size_match, hash_match

    @staticmethod
    def _sha256_local(path: str) -> str:
        digest = hashlib.sha256()
        with open(path, "rb") as file_obj:
            while True:
                chunk = file_obj.read(1024 * 512)
                if not chunk:
                    break
                digest.update(chunk)
        return digest.hexdigest()

    def _sha256_remote(self, remote_path: str) -> str:
        if not self._ftps:
            raise RuntimeError("FTPS не подключен.")
        digest = hashlib.sha256()

        def _collector(chunk: bytes) -> None:
            digest.update(chunk)

        self._ftps.retrbinary(f"RETR {remote_path}", _collector, blocksize=1024 * 512)
        return digest.hexdigest()
