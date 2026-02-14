import asyncio
import hashlib
import os
import posixpath
import socket
import ssl
import threading
import unicodedata
from concurrent.futures import Future, TimeoutError as FutureTimeoutError
from pathlib import PurePosixPath
from typing import Callable, Optional

import aioftp
from aioftp.errors import StatusCodeError

from core.models import Settings

FTPS_TIMEOUT_SECONDS = 30
FTPS_LONG_TIMEOUT_SECONDS = 180


class FTPSSync:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._client: Optional[aioftp.Client] = None
        self._remote_channel_dir = ""
        self._remote_file_names: set[str] = set()
        self._io_lock = threading.Lock()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._loop_thread: Optional[threading.Thread] = None

    @property
    def enabled(self) -> bool:
        return bool(self.settings.use_ftps)

    @property
    def remote_channel_dir(self) -> str:
        return self._remote_channel_dir

    @property
    def name(self) -> str:
        return "FTPS"

    def _start_loop(self) -> None:
        if self._loop and self._loop_thread and self._loop_thread.is_alive():
            return

        loop = asyncio.new_event_loop()

        def _runner() -> None:
            asyncio.set_event_loop(loop)
            loop.run_forever()

        thread = threading.Thread(target=_runner, name="ftps-aioftp-loop", daemon=True)
        thread.start()
        self._loop = loop
        self._loop_thread = thread

    def _stop_loop(self) -> None:
        if not self._loop:
            return
        try:
            self._loop.call_soon_threadsafe(self._loop.stop)
        except Exception:
            pass
        if self._loop_thread:
            self._loop_thread.join(timeout=2)
        self._loop = None
        self._loop_thread = None

    def _run(self, coro, timeout: int = FTPS_LONG_TIMEOUT_SECONDS):
        if not self._loop:
            raise RuntimeError("FTPS loop is not started.")
        fut: Future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        try:
            return fut.result(timeout=timeout)
        except FutureTimeoutError as exc:
            fut.cancel()
            raise TimeoutError(f"FTPS operation timed out after {timeout}s") from exc

    def _build_ssl_context(self) -> ssl.SSLContext:
        if self.settings.ftps_verify_tls:
            return ssl.create_default_context()
        return ssl._create_unverified_context()

    def connect(self) -> None:
        if not self.enabled:
            return

        if not self.settings.ftps_host or not self.settings.ftps_username:
            raise ValueError("FTPS включен, но не заполнены host/username.")
        if self.settings.ftps_port <= 0 or self.settings.ftps_port > 65535:
            raise ValueError("FTPS_PORT должен быть в диапазоне 1..65535.")
        if not self.settings.ftps_passive_mode:
            raise ValueError("FTPS active mode не поддерживается aioftp. Включите пассивный режим.")
        if self.settings.ftps_security_mode not in {"explicit", "implicit"}:
            raise ValueError("FTPS security mode должен быть explicit или implicit.")

        self._start_loop()
        self._run(self._connect_async())

    async def _connect_async(self) -> None:
        if self.settings.ftps_security_mode == "implicit":
            # aioftp handles implicit FTPS through preconfigured SSL context.
            ssl_opt: ssl.SSLContext | bool | None = self._build_ssl_context()
        else:
            ssl_opt = None

        encoding = (self.settings.ftps_encoding or "auto").strip().lower()
        if encoding == "auto":
            encoding = "utf-8"

        client = aioftp.Client(
            socket_timeout=FTPS_TIMEOUT_SECONDS,
            connection_timeout=FTPS_TIMEOUT_SECONDS,
            encoding=encoding,
            ssl=ssl_opt,
        )
        await client.connect(host=self.settings.ftps_host, port=self.settings.ftps_port)
        if self.settings.ftps_security_mode == "explicit":
            await client.upgrade_to_tls(self._build_ssl_context())
        await client.login(user=self.settings.ftps_username, password=self.settings.ftps_password or "")
        self._client = client

    def close(self) -> None:
        if self._loop and self._client is not None:
            try:
                self._run(self._close_async(), timeout=10)
            except Exception:
                pass
        self._client = None
        self._stop_loop()

    async def _close_async(self) -> None:
        if not self._client:
            return
        try:
            await self._client.quit()
        except Exception:
            try:
                await self._client.close()
            except Exception:
                pass

    def validate_connection(self) -> tuple[bool, str]:
        if not self.enabled:
            return True, "FTPS выключен."
        try:
            self.connect()
            base_remote = self.ensure_remote_dir((self.settings.ftps_remote_dir or "/").strip() or "/")
            tls_mode = "verify=on" if self.settings.ftps_verify_tls else "verify=off"
            transfer_mode = "passive"
            secure_mode = self.settings.ftps_security_mode
            encoding = (self.settings.ftps_encoding or "auto").strip().lower()
            if encoding == "auto":
                encoding = "utf-8"
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
        if not self._client:
            raise RuntimeError("FTPS is not connected.")

        with self._io_lock:
            base_remote = self.ensure_remote_dir((self.settings.ftps_remote_dir or "/").strip() or "/")
            channel_remote = (
                posixpath.join(base_remote.rstrip("/"), channel_folder)
                if base_remote != "/"
                else f"/{channel_folder}"
            )
            channel_remote = self.ensure_remote_dir(channel_remote)
            self._remote_channel_dir = channel_remote
            self._remote_file_names = self._run(
                self._list_names_async(channel_remote),
                timeout=FTPS_LONG_TIMEOUT_SECONDS,
            )

    async def _list_names_async(self, remote_dir: str) -> set[str]:
        if not self._client:
            return set()
        names: set[str] = set()
        try:
            async for path, _ in self._client.list(PurePosixPath(remote_dir)):
                names.add(path.name)
        except Exception:
            return set()
        return names

    @staticmethod
    def _is_recoverable_error(exc: Exception) -> bool:
        if isinstance(exc, (ssl.SSLEOFError, BrokenPipeError, TimeoutError, socket.timeout, ConnectionError)):
            return True
        if isinstance(exc, OSError) and getattr(exc, "errno", None) in {32, 54, 104, 110}:
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
                self._run(self._ensure_remote_dir_async(candidate), timeout=FTPS_LONG_TIMEOUT_SECONDS)
                return candidate
            except Exception as exc:
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

    @staticmethod
    def _normalize_name(value: str) -> str:
        return unicodedata.normalize("NFC", (value or "").strip())

    async def _ensure_remote_dir_async(self, path: str) -> None:
        if not self._client:
            raise RuntimeError("FTPS is not connected.")
        if path in {"", ".", "./"}:
            return
        is_abs = path.startswith("/")
        parts = [p for p in path.split("/") if p]
        current = "/" if is_abs else ""
        for part in parts:
            current = posixpath.join(current, part) if current else part
            target = PurePosixPath(current)
            exists = await self._client.exists(target)
            if not exists:
                await self._client.make_directory(target, parents=True)

    def upload_file_if_needed(
        self,
        local_file_path: str,
        progress_callback: Optional[Callable[[int, int], None]] = None,
        verify_hash: bool = False,
        force_upload: bool = False,
    ) -> tuple[bool, str]:
        if not self.enabled:
            return False, "FTPS выключен."
        if not self._client:
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
                    remote_path = posixpath.join(self._remote_channel_dir, file_name)
                    uploaded, reason = self._run(
                        self._upload_file_if_needed_async(
                            local_file_path,
                            remote_path,
                            progress_callback,
                            verify_hash,
                            force_upload,
                        ),
                        timeout=max(FTPS_LONG_TIMEOUT_SECONDS, 240),
                    )
                    return uploaded, reason
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

    async def _upload_file_if_needed_async(
        self,
        local_file_path: str,
        remote_path: str,
        progress_callback: Optional[Callable[[int, int], None]],
        verify_hash: bool,
        force_upload: bool,
    ) -> tuple[bool, str]:
        if not self._client:
            raise RuntimeError("FTPS не подключен.")

        file_name = os.path.basename(local_file_path)
        uploaded = False
        reuploaded = False

        remote_exists = False
        if file_name in self._remote_file_names:
            remote_exists = True
        else:
            remote_exists = await self._remote_exists_async(remote_path)

        if remote_exists and not force_upload:
            size_match, hash_match = await self._verify_remote_file_async(
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
            with open(local_file_path, "rb") as file_obj:
                async with self._client.upload_stream(PurePosixPath(remote_path), offset=0) as stream:
                    while True:
                        chunk = file_obj.read(1024 * 256)
                        if not chunk:
                            break
                        await stream.write(chunk)
                        sent_bytes += len(chunk)
                        if progress_callback:
                            progress_callback(min(sent_bytes, total_size), total_size)
            if progress_callback:
                progress_callback(total_size, total_size)
            self._remote_file_names.add(file_name)
            uploaded = True

        size_match, hash_match = await self._verify_remote_file_async(
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

    def verify_remote_file(
        self,
        local_file_path: str,
        remote_file_path: str,
        verify_hash: bool = False,
        local_hash: str = "",
    ) -> tuple[bool, bool]:
        return self._run(
            self._verify_remote_file_async(
                local_file_path,
                remote_file_path,
                verify_hash=verify_hash,
                local_hash=local_hash,
            ),
            timeout=max(FTPS_LONG_TIMEOUT_SECONDS, 240),
        )

    async def _verify_remote_file_async(
        self,
        local_file_path: str,
        remote_file_path: str,
        verify_hash: bool = False,
        local_hash: str = "",
    ) -> tuple[bool, bool]:
        if not self._client:
            raise RuntimeError("FTPS не подключен.")

        local_size = os.path.getsize(local_file_path)
        remote_size = await self._remote_size_async(remote_file_path)
        size_match = int(remote_size) == int(local_size)

        hash_match = True
        if verify_hash:
            local_hash_value = local_hash.strip() if local_hash else self._sha256_local(local_file_path)
            remote_hash, streamed_size = await self._sha256_remote_async(remote_file_path)
            hash_match = local_hash_value == remote_hash
            # Some FTPS servers return incomplete/incorrect size in STAT/LIST fallback.
            # When hash is verified from streamed remote content, prefer streamed size.
            if int(remote_size) <= 0:
                size_match = int(streamed_size) == int(local_size)
            elif hash_match and int(streamed_size) == int(local_size):
                size_match = True
        return size_match, hash_match

    def check_remote_file_status(
        self,
        local_file_path: str,
        verify_hash: bool = False,
        local_hash: str = "",
    ) -> tuple[bool, str]:
        if not self.enabled:
            return False, "FTPS выключен."
        if not self._client:
            return False, "FTPS не подключен."
        if not self._remote_channel_dir:
            return False, "Не подготовлен удаленный каталог."
        file_name = os.path.basename(local_file_path)
        if not file_name:
            return False, "Пустое имя файла."
        remote_name = file_name
        remote_path = posixpath.join(self._remote_channel_dir, remote_name)

        exists = self._run(self._remote_exists_async(remote_path), timeout=FTPS_LONG_TIMEOUT_SECONDS)
        if not exists:
            # Fallback for FTPS servers with inconsistent path/name encoding behavior.
            remote_name = self._find_remote_name_by_listing(file_name) or file_name
            remote_path = posixpath.join(self._remote_channel_dir, remote_name)
            exists = self._run(self._remote_exists_async(remote_path), timeout=FTPS_LONG_TIMEOUT_SECONDS)
        if not exists:
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

    def _find_remote_name_by_listing(self, requested_file_name: str) -> str:
        if not self._remote_channel_dir:
            return ""
        try:
            listed = self._run(
                self._list_names_async(self._remote_channel_dir),
                timeout=FTPS_LONG_TIMEOUT_SECONDS,
            )
        except Exception:
            return ""
        if not listed:
            return ""
        requested_norm = self._normalize_name(requested_file_name)
        lower_requested = requested_norm.casefold()
        for name in listed:
            if self._normalize_name(name) == requested_norm:
                return name
        for name in listed:
            if self._normalize_name(name).casefold() == lower_requested:
                return name
        return ""

    async def _remote_exists_async(self, remote_file_path: str) -> bool:
        if not self._client:
            raise RuntimeError("FTPS не подключен.")
        try:
            return await self._client.exists(PurePosixPath(remote_file_path))
        except StatusCodeError as exc:
            text = str(exc).lower()
            if "550" in text:
                return False
            raise

    async def _remote_size_async(self, remote_file_path: str) -> int:
        if not self._client:
            raise RuntimeError("FTPS не подключен.")
        stat = await self._client.stat(PurePosixPath(remote_file_path))
        size_raw = stat.get("size") or stat.get("st_size") or 0
        return int(size_raw)

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

    async def _sha256_remote_async(self, remote_path: str) -> tuple[str, int]:
        if not self._client:
            raise RuntimeError("FTPS не подключен.")
        digest = hashlib.sha256()
        streamed_size = 0
        async with self._client.download_stream(PurePosixPath(remote_path), offset=0) as stream:
            async for chunk in stream.iter_by_block(1024 * 512):
                digest.update(chunk)
                streamed_size += len(chunk)
        return digest.hexdigest(), int(streamed_size)
