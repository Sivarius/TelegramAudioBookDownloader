import hashlib
import os
import posixpath
import socket
import ssl
import threading
from ftplib import FTP, FTP_TLS, error_perm
from typing import Callable, Optional

from core.models import Settings

FTPS_TIMEOUT_SECONDS = 30


class ReusedSslSocket(ssl.SSLSocket):
    def unwrap(self):
        # Some FTPS servers close data TLS channel without proper TLS close_notify.
        # Avoid surfacing noisy unwrap errors in this case.
        return None


class ReusedSessionFTP_TLS(FTP_TLS):
    """Explicit FTPS with shared TLS session for data channel."""

    def ntransfercmd(self, cmd: str, rest=None):
        conn, size = FTP.ntransfercmd(self, cmd, rest)
        if self._prot_p:
            session = getattr(self.sock, "session", None)
            if session is not None:
                conn = self.context.wrap_socket(
                    conn,
                    server_hostname=self.host,
                    session=session,
                )
            else:
                conn = self.context.wrap_socket(conn, server_hostname=self.host)
            conn.__class__ = ReusedSslSocket
        return conn, size


class ImplicitFTP_TLS(ReusedSessionFTP_TLS):
    """Minimal implicit FTPS client (TLS starts on connect)."""

    def connect(self, host: str = "", port: int = 0) -> str:
        if host:
            self.host = host
        if port:
            self.port = port
        self.sock = socket.create_connection(
            (self.host, self.port),
            timeout=self.timeout,
        )
        self.af = self.sock.family
        self.sock = self.context.wrap_socket(self.sock, server_hostname=self.host)
        self.file = self.sock.makefile("r", encoding=self.encoding)
        self.welcome = self.getresp()
        return self.welcome


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

    def _resolve_ftps_encoding(self, ftps: FTP_TLS) -> str:
        configured = (self.settings.ftps_encoding or "auto").strip().lower()
        if configured != "auto":
            return configured
        try:
            feat = ftps.sendcmd("FEAT")
            upper_feat = feat.upper()
            if "UTF8" in upper_feat:
                try:
                    ftps.sendcmd("OPTS UTF8 ON")
                except Exception:
                    pass
                return "utf-8"
        except Exception:
            pass
        return "cp1251"

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
        initial_encoding = (
            (self.settings.ftps_encoding or "auto").strip().lower()
            if (self.settings.ftps_encoding or "auto").strip().lower() != "auto"
            else "utf-8"
        )
        if self.settings.ftps_security_mode == "implicit":
            ftps: FTP_TLS = ImplicitFTP_TLS(
                context=context,
                timeout=FTPS_TIMEOUT_SECONDS,
                encoding=initial_encoding,
            )
        else:
            ftps = ReusedSessionFTP_TLS(
                context=context,
                timeout=FTPS_TIMEOUT_SECONDS,
                encoding=initial_encoding,
            )
        ftps.connect(host=self.settings.ftps_host, port=self.settings.ftps_port)
        if ftps.sock is not None:
            ftps.sock.settimeout(FTPS_TIMEOUT_SECONDS)
        ftps.login(user=self.settings.ftps_username, passwd=self.settings.ftps_password or "")
        ftps.encoding = self._resolve_ftps_encoding(ftps)
        ftps.prot_p()
        ftps.set_pasv(bool(self.settings.ftps_passive_mode))
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
            base_remote = self.ensure_remote_dir((self.settings.ftps_remote_dir or "/").strip() or "/")
            tls_mode = "verify=on" if self.settings.ftps_verify_tls else "verify=off"
            transfer_mode = "passive" if self.settings.ftps_passive_mode else "active"
            secure_mode = self.settings.ftps_security_mode
            encoding = getattr(self._ftps, "encoding", "utf-8")
            return (
                True,
                "FTPS: подключение установлено "
                f"({tls_mode}; mode={transfer_mode}; security={secure_mode}; encoding={encoding}). "
                f"Каталог доступен: {base_remote}",
            )
        except ssl.SSLCertVerificationError as exc:
            return (
                False,
                "FTPS: ошибка TLS-сертификата. "
                "Проверьте цепочку сертификатов на сервере или отключите проверку TLS в настройках FTPS. "
                f"({exc})",
            )
        except (TimeoutError, socket.timeout) as exc:
            return (
                False,
                "FTPS: таймаут чтения/ответа сервера. "
                "Проверьте доступность хоста/порта, настройки firewall/NAT и режим FTPS на сервере. "
                f"(timeout={FTPS_TIMEOUT_SECONDS}s; {exc})",
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

        base_remote = self.ensure_remote_dir((self.settings.ftps_remote_dir or "/").strip() or "/")
        channel_remote = posixpath.join(base_remote.rstrip("/"), channel_folder) if base_remote != "/" else f"/{channel_folder}"
        channel_remote = self.ensure_remote_dir(channel_remote)
        self._remote_channel_dir = channel_remote
        try:
            names = self._ftps.nlst(channel_remote)
        except Exception:
            names = []
        self._remote_file_names = {posixpath.basename(name.rstrip("/")) for name in names}

    def _keepalive(self) -> None:
        if not self._ftps:
            return
        self._ftps.voidcmd("NOOP")

    @staticmethod
    def _is_recoverable_error(exc: Exception) -> bool:
        if isinstance(exc, (ssl.SSLEOFError, BrokenPipeError, TimeoutError, socket.timeout)):
            return True
        if isinstance(exc, OSError):
            if getattr(exc, "errno", None) in {32, 54, 104, 110}:
                return True
        if isinstance(exc, error_perm):
            text = str(exc).lower()
            if "425" in text or "426" in text:
                return True
        text = str(exc).lower()
        return "timed out" in text or "broken pipe" in text or "eof occurred" in text

    def _reconnect_current_channel_dir_locked(self) -> None:
        channel_folder = ""
        if self._remote_channel_dir:
            channel_folder = posixpath.basename(self._remote_channel_dir.rstrip("/"))
        self.close()
        self.connect()
        if channel_folder:
            self.prepare_channel_dir(channel_folder)

    def ensure_remote_dir(self, remote_dir: str) -> str:
        if not self._ftps:
            raise RuntimeError("FTPS is not connected.")
        path = self._normalize_remote_dir(remote_dir)
        if not path:
            return "."

        candidates = [path]
        if path.startswith("/"):
            rel = path.lstrip("/")
            if rel:
                candidates.append(rel)

        last_exc: Optional[Exception] = None
        for candidate in candidates:
            try:
                self._ensure_remote_dir_once(candidate)
                return candidate
            except error_perm as exc:
                # Some FTPS servers deny absolute paths in jailed home; retry as relative.
                last_exc = exc

        if last_exc:
            raise last_exc
        return path

    @staticmethod
    def _normalize_remote_dir(path: str) -> str:
        normalized = (path or "").replace("\\", "/").strip()
        while "//" in normalized:
            normalized = normalized.replace("//", "/")
        return normalized

    def _ensure_remote_dir_once(self, path: str) -> None:
        if not self._ftps:
            raise RuntimeError("FTPS is not connected.")
        if path in {"", ".", "./"}:
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

    def upload_file_if_needed(
        self,
        local_file_path: str,
        progress_callback: Optional[Callable[[int, int], None]] = None,
        verify_hash: bool = False,
        force_upload: bool = False,
    ) -> tuple[bool, str]:
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
            attempts = 3
            last_exc: Optional[Exception] = None
            for attempt in range(1, attempts + 1):
                try:
                    self._keepalive()
                    remote_path = posixpath.join(self._remote_channel_dir, file_name)
                    uploaded = False
                    reuploaded = False
                    remote_exists = False
                    if file_name in self._remote_file_names:
                        remote_exists = True
                    else:
                        try:
                            remote_exists = self._remote_exists(remote_path)
                        except Exception:
                            remote_exists = False

                    if remote_exists and not force_upload:
                        size_match, hash_match = self.verify_remote_file(
                            local_file_path,
                            remote_path,
                            verify_hash=verify_hash,
                        )
                        verified = size_match and hash_match
                        if verified:
                            self._remote_file_names.add(file_name)
                            return (
                                False,
                                f"already_exists; remote={remote_path}; size_match={size_match}; hash_match={hash_match}; verified={verified}",
                            )
                        reuploaded = True

                    if force_upload or not remote_exists or reuploaded:
                        total_size = os.path.getsize(local_file_path)
                        sent_bytes = 0

                        def _block_progress(chunk: bytes) -> None:
                            nonlocal sent_bytes
                            sent_bytes += len(chunk)
                            if progress_callback:
                                progress_callback(min(sent_bytes, total_size), total_size)

                        with open(local_file_path, "rb") as file_obj:
                            self._ftps.storbinary(
                                f"STOR {remote_path}",
                                file_obj,
                                blocksize=1024 * 256,
                                callback=_block_progress,
                            )
                        if progress_callback:
                            progress_callback(total_size, total_size)
                        self._remote_file_names.add(file_name)
                        uploaded = True

                    size_match, hash_match = self.verify_remote_file(
                        local_file_path,
                        remote_path,
                        verify_hash=verify_hash,
                    )
                    verified = size_match and hash_match
                    if not verified:
                        raise RuntimeError(
                            f"FTPS verify failed: remote={remote_path} size_match={size_match} hash_match={hash_match}"
                        )
                    reason = "reuploaded" if reuploaded else ("uploaded" if uploaded else "already_exists")
                    return (
                        uploaded,
                        f"{reason}; remote={remote_path}; size_match={size_match}; hash_match={hash_match}; verified={verified}",
                    )
                except Exception as exc:
                    last_exc = exc
                    if attempt >= attempts or not self._is_recoverable_error(exc):
                        raise
                    try:
                        self._reconnect_current_channel_dir_locked()
                    except Exception as reconnect_exc:
                        last_exc = reconnect_exc
                        if attempt >= attempts:
                            raise reconnect_exc
            if last_exc:
                raise last_exc
            raise RuntimeError("FTPS upload failed for unknown reason.")

    def verify_remote_file(
        self,
        local_file_path: str,
        remote_file_path: str,
        verify_hash: bool = False,
        local_hash: str = "",
    ) -> tuple[bool, bool]:
        if not self._ftps:
            raise RuntimeError("FTPS не подключен.")

        local_size = os.path.getsize(local_file_path)
        remote_size = self._ftps.size(remote_file_path) or 0
        size_match = int(remote_size) == int(local_size)

        hash_match = True
        if verify_hash:
            local_hash_value = local_hash.strip() if local_hash else self._sha256_local(local_file_path)
            remote_hash = self._sha256_remote(remote_file_path)
            hash_match = local_hash_value == remote_hash
        return size_match, hash_match

    def check_remote_file_status(
        self,
        local_file_path: str,
        verify_hash: bool = False,
        local_hash: str = "",
    ) -> tuple[bool, str]:
        if not self.enabled:
            return False, "FTPS выключен."
        if not self._ftps:
            return False, "FTPS не подключен."
        if not self._remote_channel_dir:
            return False, "Не подготовлен удаленный каталог."
        file_name = os.path.basename(local_file_path)
        if not file_name:
            return False, "Пустое имя файла."
        remote_path = posixpath.join(self._remote_channel_dir, file_name)
        if not self._remote_exists(remote_path):
            return False, f"remote_missing; remote={remote_path}"
        size_match, hash_match = self.verify_remote_file(
            local_file_path,
            remote_path,
            verify_hash=verify_hash,
            local_hash=local_hash,
        )
        verified = size_match and hash_match
        if verified:
            self._remote_file_names.add(file_name)
        return (
            verified,
            f"remote={remote_path}; size_match={size_match}; hash_match={hash_match}; verified={verified}",
        )

    def _remote_exists(self, remote_file_path: str) -> bool:
        if not self._ftps:
            raise RuntimeError("FTPS не подключен.")
        try:
            size = self._ftps.size(remote_file_path)
            return size is not None
        except error_perm as exc:
            # 550 commonly means missing file / denied; for existence check treat as missing.
            if "550" in str(exc):
                return False
            raise

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
