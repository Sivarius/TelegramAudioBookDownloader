import asyncio
import ftplib
import hashlib
import os
import posixpath
import re
import socket
import ssl
import threading
import unicodedata
from concurrent.futures import Future, TimeoutError as FutureTimeoutError
from pathlib import PurePosixPath
from typing import Callable, Optional

import aioftp
from aioftp.errors import StatusCodeError

from core.ftps_manifest import FTPSManifestStore
from core.models import Settings

FTPS_TIMEOUT_SECONDS = 30
FTPS_LONG_TIMEOUT_SECONDS = 180
FTPS_LIST_TIMEOUT_SECONDS = 10
FTPS_MANIFEST_FILE = ".teleparser_manifest.json"


class FTPSSync:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._client: Optional[aioftp.Client] = None
        self._base_remote_dir = ""
        self._remote_channel_dir = ""
        self._remote_file_names: set[str] = set()
        self._io_lock = threading.Lock()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._loop_thread: Optional[threading.Thread] = None
        self._ftplib_client: Optional[ftplib.FTP_TLS] = None
        self._passive_forced = False
        self._effective_passive_mode = True
        self._manifest_store = FTPSManifestStore(
            get_ftplib_client=self._get_ftplib_client,
            close_ftplib_client=self._close_ftplib_client,
            normalize_name=self._normalize_name,
            sha256_local=self._sha256_local,
            verify_hash_enabled=lambda: bool(self.settings.ftps_verify_hash),
            manifest_file_name=FTPS_MANIFEST_FILE,
        )

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
        self._passive_forced = False
        self._effective_passive_mode = bool(self.settings.ftps_passive_mode)
        if not self._effective_passive_mode:
            # aioftp works in passive data mode. Force it to avoid hard failures.
            self._effective_passive_mode = True
            self._passive_forced = True
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
        self._close_ftplib_client()
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
            transfer_mode = "passive(auto)" if self._passive_forced else "passive"
            secure_mode = self.settings.ftps_security_mode
            encoding = (self.settings.ftps_encoding or "auto").strip().lower()
            if encoding == "auto":
                encoding = "utf-8"
            forced_note = (
                " Active mode недоступен в текущем FTPS клиенте, режим был автоматически переключен на passive."
                if self._passive_forced
                else ""
            )
            return (
                True,
                "FTPS: подключение установлено "
                f"({tls_mode}; mode={transfer_mode}; security={secure_mode}; encoding={encoding}; "
                f"hash_verify={'on' if self.settings.ftps_verify_hash else 'off'}). "
                f"Каталог доступен: {base_remote}.{forced_note}",
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
            self._base_remote_dir = base_remote
            channel_remote = (
                posixpath.join(base_remote.rstrip("/"), channel_folder)
                if base_remote != "/"
                else f"/{channel_folder}"
            )
            channel_remote = self.ensure_remote_dir(channel_remote)
            self._remote_channel_dir = channel_remote
            self._manifest_store.reset(channel_remote)
            names: set[str] = set()
            fallback_rows, _ = self._list_dir_detailed_ftplib_sync(channel_remote, limit=10000)
            names = {
                str(item.get("name", "")).strip()
                for item in fallback_rows
                if str(item.get("name", "")).strip()
            }
            if not names:
                try:
                    names = self._run(
                        self._list_names_async(channel_remote),
                        timeout=FTPS_LIST_TIMEOUT_SECONDS,
                    )
                except Exception:
                    names = set()
            self._remote_file_names = names
            self._load_manifest_locked()

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

    async def _list_dir_entries_async(self, remote_dir: str) -> list[tuple[str, str]]:
        if not self._client:
            return []
        entries: list[tuple[str, str]] = []
        try:
            async for path, info in self._client.list(PurePosixPath(remote_dir)):
                item_type = str((info or {}).get("type", "")).strip().lower()
                entries.append((path.name, item_type))
        except Exception:
            return []
        return entries

    async def _list_dir_detailed_async(self, remote_dir: str) -> list[dict]:
        if not self._client:
            return []
        entries: list[dict] = []
        try:
            async for path, info in self._client.list(PurePosixPath(remote_dir)):
                meta = dict(info or {})
                entries.append(
                    {
                        "name": str(path.name),
                        "path": str(path),
                        "type": str(meta.get("type", "")),
                        "size": str(meta.get("size", meta.get("st_size", ""))),
                        "modify": str(meta.get("modify", "")),
                    }
                )
        except Exception:
            return []
        return entries

    def _connect_ftplib_explicit(self) -> ftplib.FTP_TLS:
        if self.settings.ftps_security_mode != "explicit":
            raise RuntimeError("ftplib fallback supports explicit FTPS mode only.")
        context = self._build_ssl_context()
        encoding = (self.settings.ftps_encoding or "auto").strip().lower()
        if encoding == "auto":
            encoding = "utf-8"
        client = ftplib.FTP_TLS(timeout=FTPS_TIMEOUT_SECONDS, context=context, encoding=encoding)
        client.connect(host=self.settings.ftps_host, port=self.settings.ftps_port, timeout=FTPS_TIMEOUT_SECONDS)
        client.auth()
        client.login(user=self.settings.ftps_username, passwd=self.settings.ftps_password or "")
        client.prot_p()
        client.set_pasv(True)
        return client

    def _close_ftplib_client(self) -> None:
        if not self._ftplib_client:
            return
        try:
            self._ftplib_client.quit()
        except Exception:
            try:
                self._ftplib_client.close()
            except Exception:
                pass
        self._ftplib_client = None

    def _get_ftplib_client(self) -> ftplib.FTP_TLS:
        if self._ftplib_client:
            try:
                self._ftplib_client.voidcmd("NOOP")
                return self._ftplib_client
            except Exception:
                self._close_ftplib_client()
        self._ftplib_client = self._connect_ftplib_explicit()
        return self._ftplib_client

    def _manifest_remote_path(self) -> str:
        return self._manifest_store.manifest_remote_path()

    def _load_manifest_locked(self) -> None:
        self._manifest_store.load()

    def _flush_manifest_locked(self) -> None:
        self._manifest_store.flush()

    def _manifest_match_locked(
        self,
        local_file_path: str,
        verify_hash: bool = False,
        local_hash: str = "",
    ) -> tuple[bool, str]:
        return self._manifest_store.match(
            local_file_path,
            verify_hash=verify_hash,
            local_hash=local_hash,
        )

    def _manifest_upsert_locked(
        self,
        local_file_path: str,
        remote_path: str,
        local_hash: str = "",
    ) -> None:
        self._manifest_store.upsert(local_file_path, remote_path, local_hash=local_hash)

    def _record_verified_manifest_locked(
        self,
        local_file_path: str,
        remote_path: str,
        local_hash: str = "",
    ) -> None:
        self._manifest_store.record_verified(local_file_path, remote_path, local_hash=local_hash)

    def _list_dir_detailed_ftplib_sync(self, remote_dir: str, limit: int = 500) -> tuple[list[dict], str]:
        try:
            client = self._connect_ftplib_explicit()
        except Exception:
            return [], ""
        try:
            rows: list[dict] = []
            try:
                for name, facts in client.mlsd(remote_dir):
                    rows.append(
                        {
                            "name": str(name),
                            "path": posixpath.join(remote_dir.rstrip("/"), str(name))
                            if remote_dir not in {"", ".", "/"}
                            else (f"/{name}" if remote_dir == "/" else str(name)),
                            "type": str((facts or {}).get("type", "")),
                            "size": str((facts or {}).get("size", "")),
                            "modify": str((facts or {}).get("modify", "")),
                        }
                    )
                    if len(rows) >= max(1, int(limit)):
                        break
                return rows, "ftplib:mlsd"
            except Exception:
                pass

            try:
                names = client.nlst(remote_dir)
            except Exception:
                names = []
            for raw in names:
                value = str(raw).strip()
                if not value:
                    continue
                name = posixpath.basename(value.rstrip("/"))
                if name in {".", "..", ""}:
                    continue
                path = value
                if not path.startswith("/"):
                    if remote_dir in {"", ".", "/"}:
                        path = f"/{name}" if remote_dir == "/" else name
                    else:
                        path = posixpath.join(remote_dir.rstrip("/"), name)
                rows.append(
                    {
                        "name": name,
                        "path": path,
                        "type": "",
                        "size": "",
                        "modify": "",
                    }
                )
                if len(rows) >= max(1, int(limit)):
                    break
            return rows, ("ftplib:nlst" if rows else "")
        finally:
            try:
                client.quit()
            except Exception:
                try:
                    client.close()
                except Exception:
                    pass

    def list_remote_entries_ftplib(self, remote_dir: str, limit: int = 500) -> list[dict]:
        normalized = self._normalize_remote_dir(remote_dir)
        if not normalized:
            normalized = "/"
        rows, source = self._list_dir_detailed_ftplib_sync(normalized, limit=max(1, int(limit)))
        if not rows:
            return []
        rows.sort(key=lambda item: (str(item.get("type", "")) != "dir", str(item.get("name", "")).casefold()))
        for row in rows:
            row["list_source"] = f"{normalized} [{source or 'ftplib'}]"
        return rows

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

    @staticmethod
    def _extract_chapter_token(file_name: str) -> str:
        raw = (file_name or "").strip()
        if not raw:
            return ""
        leading = re.match(r"^(\d{1,6})", raw)
        if leading:
            return leading.group(1).lstrip("0") or "0"
        any_digits = re.search(r"(\d{1,6})", raw)
        if any_digits:
            return any_digits.group(1).lstrip("0") or "0"
        return ""

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

        remote_name = self._resolve_remote_name_from_listing(file_name) or file_name
        remote_path = posixpath.join(self._remote_channel_dir, remote_name)

        remote_exists = False
        if remote_name in self._remote_file_names:
            remote_exists = True
        else:
            remote_exists = await self._remote_exists_async(remote_path)

        if remote_exists and not force_upload:
            manifest_ok, manifest_remote_path = self._manifest_match_locked(
                local_file_path,
                verify_hash=verify_hash,
                local_hash="",
            )
            if manifest_ok:
                manifest_path = manifest_remote_path or remote_path
                try:
                    if await self._remote_exists_async(manifest_path):
                        self._remote_file_names.add(file_name)
                        return (
                            False,
                            f"already_exists; remote={manifest_path}; size_match=True; hash_match=True; verified=True; matched_by=manifest",
                        )
                except Exception:
                    pass

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
        local_hash_dbg = ""
        remote_hash_dbg = ""
        if verify_hash and size_match and not hash_match:
            # Local hash could be stale (cached earlier) while file was replaced.
            forced_local_hash = self._sha256_local(local_file_path)
            size_match, hash_match = await self._verify_remote_file_async(
                local_file_path,
                remote_path,
                verify_hash=True,
                local_hash=forced_local_hash,
            )
            local_hash_dbg = forced_local_hash
            try:
                remote_hash_dbg, _ = await self._sha256_remote_async(remote_path)
            except Exception:
                remote_hash_dbg = ""
        verified = size_match and hash_match
        if not verified:
            raise RuntimeError(
                "FTPS verify failed: "
                f"remote={remote_path} size_match={size_match} hash_match={hash_match} "
                f"local_hash={local_hash_dbg[:12] if local_hash_dbg else ''} "
                f"remote_hash={remote_hash_dbg[:12] if remote_hash_dbg else ''}"
            )
        reason = "reuploaded" if reuploaded else ("uploaded" if uploaded else "already_exists")
        local_hash_final = ""
        if self.settings.ftps_verify_hash and verify_hash:
            local_hash_final = self._sha256_local(local_file_path)
        self._record_verified_manifest_locked(local_file_path, remote_path, local_hash_final)
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

    def _verify_remote_with_diagnostics(
        self,
        local_file_path: str,
        remote_path: str,
        verify_hash: bool = False,
        local_hash: str = "",
    ) -> tuple[bool, bool, str, str]:
        size_match, hash_match = self.verify_remote_file(
            local_file_path,
            remote_path,
            verify_hash=verify_hash,
            local_hash=local_hash,
        )
        local_hash_dbg = local_hash.strip() if local_hash else ""
        remote_hash_dbg = ""
        if verify_hash and size_match and not hash_match:
            forced_local_hash = self._sha256_local(local_file_path)
            size_match, hash_match = self.verify_remote_file(
                local_file_path,
                remote_path,
                verify_hash=True,
                local_hash=forced_local_hash,
            )
            local_hash_dbg = forced_local_hash
            try:
                remote_hash_dbg, _ = self._run(
                    self._sha256_remote_async(remote_path),
                    timeout=max(FTPS_LONG_TIMEOUT_SECONDS, 240),
                )
            except Exception:
                remote_hash_dbg = ""
        return size_match, hash_match, local_hash_dbg, remote_hash_dbg

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
        should_verify_hash = bool(verify_hash and self.settings.ftps_verify_hash)
        if should_verify_hash:
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
        remote_path_hint: str = "",
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

        def _remember_verified(remote_verified_path: str, cache_name: str = "") -> None:
            remembered_name = (cache_name or "").strip() or os.path.basename(remote_verified_path)
            if remembered_name:
                self._remote_file_names.add(remembered_name)
            hash_for_manifest = ""
            if self.settings.ftps_verify_hash and verify_hash:
                hash_for_manifest = (local_hash or "").strip().lower() or self._sha256_local(local_file_path)
            self._record_verified_manifest_locked(local_file_path, remote_verified_path, hash_for_manifest)

        def _verify_candidate(remote_path: str, matched_by: str) -> tuple[bool, str]:
            size_match, hash_match, local_hash_dbg, remote_hash_dbg = self._verify_remote_with_diagnostics(
                local_file_path=local_file_path,
                remote_path=remote_path,
                verify_hash=verify_hash,
                local_hash=local_hash,
            )
            verified = size_match and hash_match
            if verified:
                _remember_verified(remote_path, os.path.basename(remote_path))
                return (
                    True,
                    f"remote={remote_path}; size_match={size_match}; hash_match={hash_match}; "
                    f"verified=True; matched_by={matched_by}",
                )
            return (
                False,
                f"remote_present_mismatch; remote={remote_path}; "
                f"size_match={size_match}; hash_match={hash_match}; "
                f"local_hash={local_hash_dbg[:12] if local_hash_dbg else ''}; "
                f"remote_hash={remote_hash_dbg[:12] if remote_hash_dbg else ''}; "
                f"matched_by={matched_by}",
            )

        manifest_ok, manifest_remote_path = self._manifest_match_locked(
            local_file_path,
            verify_hash=verify_hash,
            local_hash=local_hash,
        )
        if manifest_ok:
            remote_candidate = manifest_remote_path or ""
            hinted_candidate = self._normalize_remote_dir(remote_path_hint)
            if hinted_candidate:
                remote_candidate = hinted_candidate
            if not remote_candidate:
                remote_candidate = posixpath.join(self._remote_channel_dir, file_name)
            try:
                if self._check_remote_file_status_ftplib(
                    local_file_path,
                    remote_candidate,
                    verify_hash=False,
                    local_hash=local_hash,
                )[0]:
                    _remember_verified(remote_candidate, file_name)
                    return (
                        True,
                        f"remote={remote_candidate}; size_match=True; hash_match=True; verified=True; matched_by=manifest",
                    )
            except Exception:
                pass

        hinted_path = self._normalize_remote_dir(remote_path_hint)
        if hinted_path:
            try:
                verified, info = _verify_candidate(hinted_path, "db_cache")
                return verified, info
            except Exception:
                ok_fb, info_fb = self._check_remote_file_status_ftplib(
                    local_file_path,
                    hinted_path,
                    verify_hash=verify_hash,
                    local_hash=local_hash,
                )
                if ok_fb:
                    _remember_verified(hinted_path, file_name)
                    return True, f"{info_fb}; matched_by=db_cache_ftplib_fallback"
                if "remote_present_mismatch" in info_fb:
                    return False, f"{info_fb}; matched_by=db_cache_ftplib_fallback"

        remote_name = self._resolve_remote_name_from_listing(file_name)
        if not remote_name:
            # Listing can be unavailable on some FTPS servers even when direct file
            # stat/stream works. Try exact path before declaring remote_missing.
            direct_path = posixpath.join(self._remote_channel_dir, file_name)
            try:
                verified, info = _verify_candidate(direct_path, "direct_path")
                return verified, info
            except Exception:
                pass

            candidates = self._fuzzy_remote_candidates(file_name)
            present_mismatch_info = ""
            for candidate_name in candidates[:15]:
                candidate_path = posixpath.join(self._remote_channel_dir, candidate_name)
                try:
                    verified, info = _verify_candidate(candidate_path, "fuzzy")
                except Exception:
                    continue
                if verified:
                    return verified, info
                if not present_mismatch_info:
                    present_mismatch_info = info
            for sibling_dir in self._sibling_channel_dirs():
                sibling_candidates = self._fuzzy_remote_candidates_in_dir(sibling_dir, file_name)
                for candidate_name in sibling_candidates[:8]:
                    candidate_path = posixpath.join(sibling_dir, candidate_name)
                    try:
                        verified, info = _verify_candidate(candidate_path, "sibling_dir_fuzzy")
                    except Exception:
                        continue
                    if verified:
                        return verified, info
                    if not present_mismatch_info:
                        present_mismatch_info = info
            remote_path = posixpath.join(self._remote_channel_dir, file_name)
            if present_mismatch_info:
                return False, present_mismatch_info
            if candidates:
                sample_path = posixpath.join(self._remote_channel_dir, candidates[0])
                ok_fb, info_fb = self._check_remote_file_status_ftplib(
                    local_file_path,
                    sample_path,
                    verify_hash=verify_hash,
                    local_hash=local_hash,
                )
                if ok_fb:
                    _remember_verified(sample_path, candidates[0])
                    return True, f"{info_fb}; matched_by=fuzzy_ftplib_fallback"
                if "remote_present_mismatch" in info_fb:
                    return False, f"{info_fb}; matched_by=fuzzy_ftplib_fallback"
                return (
                    False,
                    f"remote_missing; remote={remote_path}; fuzzy_candidates={len(candidates)}; sample={sample_path}",
                )
            return False, f"remote_missing; remote={remote_path}"
        remote_path = posixpath.join(self._remote_channel_dir, remote_name)
        try:
            verified, info = _verify_candidate(remote_path, "final")
            return verified, info
        except Exception as verify_exc:
            ok_fb, info_fb = self._check_remote_file_status_ftplib(
                local_file_path,
                remote_path,
                verify_hash=verify_hash,
                local_hash=local_hash,
            )
            if ok_fb:
                _remember_verified(remote_path, file_name)
                return True, f"{info_fb}; matched_by=final_ftplib_fallback"
            if "remote_present_mismatch" in info_fb:
                return False, f"{info_fb}; matched_by=final_ftplib_fallback"
            return False, f"{info_fb}; verify_exception={verify_exc}"

    def _check_remote_file_status_ftplib(
        self,
        local_file_path: str,
        remote_path: str,
        verify_hash: bool = False,
        local_hash: str = "",
    ) -> tuple[bool, str]:
        try:
            client = self._get_ftplib_client()
        except Exception as exc:
            return False, f"remote_missing; remote={remote_path}; ftplib_connect_failed={exc}"
        try:
            local_size = os.path.getsize(local_file_path)
            remote_size = -1
            try:
                size_value = client.size(remote_path)
                if size_value is not None:
                    remote_size = int(size_value)
            except Exception:
                remote_size = -1
            if remote_size < 0:
                return False, f"remote_missing; remote={remote_path}"
            size_match = int(remote_size) == int(local_size)
            hash_match = True
            should_verify_hash = bool(verify_hash and self.settings.ftps_verify_hash)
            if should_verify_hash and size_match:
                local_hash_value = local_hash.strip() if local_hash else self._sha256_local(local_file_path)
                digest = hashlib.sha256()

                def _consume(data: bytes) -> None:
                    digest.update(data)

                client.retrbinary(f"RETR {remote_path}", _consume, blocksize=1024 * 256)
                remote_hash = digest.hexdigest()
                hash_match = local_hash_value == remote_hash
            verified = size_match and hash_match
            if verified:
                return (
                    True,
                    f"remote={remote_path}; size_match={size_match}; hash_match={hash_match}; verified=True",
                )
            return (
                False,
                f"remote_present_mismatch; remote={remote_path}; size_match={size_match}; hash_match={hash_match}",
            )
        except Exception as exc:
            self._close_ftplib_client()
            text = str(exc).lower()
            if "550" in text or "not found" in text:
                return False, f"remote_missing; remote={remote_path}"
            return False, f"remote_missing; remote={remote_path}; ftplib_check_failed={exc}"

    def _find_remote_name_by_listing(self, requested_file_name: str) -> str:
        if not self._remote_channel_dir:
            return ""
        try:
            listed = self._run(
                self._list_names_async(self._remote_channel_dir),
                timeout=FTPS_LIST_TIMEOUT_SECONDS,
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

    def _get_remote_listing_names(self) -> set[str]:
        if self._remote_file_names:
            return set(self._remote_file_names)
        listed: set[str] = set()
        if not self._remote_channel_dir:
            return listed
        try:
            listed = self._run(
                self._list_names_async(self._remote_channel_dir),
                timeout=FTPS_LIST_TIMEOUT_SECONDS,
            )
        except Exception:
            listed = set()
        if not listed:
            fallback_rows, _ = self._list_dir_detailed_ftplib_sync(self._remote_channel_dir, limit=1000)
            listed = {str(item.get("name", "")).strip() for item in fallback_rows if str(item.get("name", "")).strip()}
        if listed:
            self._remote_file_names.update(listed)
        return set(self._remote_file_names)

    def _fuzzy_remote_candidates(self, requested_file_name: str) -> list[str]:
        listed = self._get_remote_listing_names()
        if not listed:
            return []
        req_norm = self._normalize_name(requested_file_name)
        req_lower = req_norm.casefold()
        req_ext = os.path.splitext(req_lower)[1]
        chapter_token = self._extract_chapter_token(requested_file_name)
        ranked: list[tuple[int, str]] = []
        for name in listed:
            norm = self._normalize_name(name)
            lower = norm.casefold()
            if req_ext and not lower.endswith(req_ext):
                continue
            score = 0
            if chapter_token:
                token_re = rf"(^|[^0-9])0*{re.escape(chapter_token)}([^0-9]|$)"
                if re.search(token_re, lower):
                    score += 10
                if lower.startswith(chapter_token) or lower.startswith(chapter_token.zfill(4)):
                    score += 6
            if lower[:16] == req_lower[:16]:
                score += 4
            if score > 0:
                ranked.append((score, name))
        ranked.sort(key=lambda item: (-item[0], self._normalize_name(item[1]).casefold()))
        return [name for _, name in ranked]

    def _fuzzy_remote_candidates_in_dir(self, remote_dir: str, requested_file_name: str) -> list[str]:
        if not remote_dir:
            return []
        listed: set[str] = set()
        try:
            listed = self._run(
                self._list_names_async(remote_dir),
                timeout=FTPS_LIST_TIMEOUT_SECONDS,
            )
        except Exception:
            listed = set()
        if not listed:
            fallback_rows, _ = self._list_dir_detailed_ftplib_sync(remote_dir, limit=1000)
            listed = {str(item.get("name", "")).strip() for item in fallback_rows if str(item.get("name", "")).strip()}
        if not listed:
            return []
        req_norm = self._normalize_name(requested_file_name)
        req_lower = req_norm.casefold()
        req_ext = os.path.splitext(req_lower)[1]
        chapter_token = self._extract_chapter_token(requested_file_name)
        ranked: list[tuple[int, str]] = []
        for name in listed:
            norm = self._normalize_name(name)
            lower = norm.casefold()
            if req_ext and not lower.endswith(req_ext):
                continue
            score = 0
            if chapter_token:
                token_re = rf"(^|[^0-9])0*{re.escape(chapter_token)}([^0-9]|$)"
                if re.search(token_re, lower):
                    score += 10
                if lower.startswith(chapter_token) or lower.startswith(chapter_token.zfill(4)):
                    score += 6
            if lower[:16] == req_lower[:16]:
                score += 4
            if score > 0:
                ranked.append((score, name))
        ranked.sort(key=lambda item: (-item[0], self._normalize_name(item[1]).casefold()))
        return [name for _, name in ranked]

    def _sibling_channel_dirs(self) -> list[str]:
        base_dir = (self._base_remote_dir or "").strip()
        if not base_dir:
            return []
        entries: list[tuple[str, str]] = []
        try:
            entries = self._run(
                self._list_dir_entries_async(base_dir),
                timeout=FTPS_LIST_TIMEOUT_SECONDS,
            )
        except Exception:
            entries = []
        if not entries:
            fallback_rows, source = self._list_dir_detailed_ftplib_sync(base_dir, limit=500)
            if source:
                entries = [
                    (
                        str(item.get("name", "")),
                        str(item.get("type", "")) if str(item.get("type", "")).strip() else "dir",
                    )
                    for item in fallback_rows
                    if str(item.get("name", "")).strip()
                ]
        dirs: list[str] = []
        for name, item_type in entries:
            if item_type != "dir":
                continue
            if base_dir == "/":
                dirs.append(f"/{name}")
            else:
                dirs.append(posixpath.join(base_dir.rstrip("/"), name))
        current = (self._remote_channel_dir or "").rstrip("/")
        return [item for item in dirs if item.rstrip("/") != current]

    def list_remote_entries(self, remote_dir: str, limit: int = 200) -> list[dict]:
        if not self.enabled:
            return []
        if not self._client:
            raise RuntimeError("FTPS не подключен.")
        normalized = self._normalize_remote_dir(remote_dir)
        if not normalized:
            normalized = "/"
        candidates: list[str] = []
        for item in (normalized, normalized.lstrip("/"), f"/{normalized.lstrip('/')}"):
            value = self._normalize_remote_dir(item)
            if not value:
                continue
            if value not in candidates:
                candidates.append(value)
        if "." not in candidates:
            candidates.append(".")

        rows: list[dict] = []
        used = ""
        with self._io_lock:
            for candidate in candidates:
                try:
                    current = self._run(
                        self._list_dir_detailed_async(candidate),
                        timeout=FTPS_LIST_TIMEOUT_SECONDS,
                    )
                except Exception:
                    current = []
                source = "aioftp:list"
                if not current:
                    current, fb_source = self._list_dir_detailed_ftplib_sync(candidate, limit=max(200, limit))
                    if fb_source:
                        source = fb_source
                if current:
                    rows = current
                    used = f"{candidate} [{source}]"
                    break
            if not rows:
                try:
                    rows = self._run(
                        self._list_dir_detailed_async(candidates[0]),
                        timeout=FTPS_LIST_TIMEOUT_SECONDS,
                    )
                except Exception:
                    rows = []
                source = "aioftp:list"
                if not rows:
                    rows, fb_source = self._list_dir_detailed_ftplib_sync(candidates[0], limit=max(200, limit))
                    if fb_source:
                        source = fb_source
                used = f"{candidates[0]} [{source}]"
        rows = rows[: max(1, int(limit))]
        rows.sort(key=lambda item: (str(item.get("type", "")) != "dir", str(item.get("name", "")).casefold()))
        for row in rows:
            row["list_source"] = used
        return rows

    def _resolve_remote_name_from_listing(self, requested_file_name: str) -> str:
        requested_norm = self._normalize_name(requested_file_name)
        lower_requested = requested_norm.casefold()

        for name in self._remote_file_names:
            if self._normalize_name(name) == requested_norm:
                return name
        for name in self._remote_file_names:
            if self._normalize_name(name).casefold() == lower_requested:
                return name

        listed_name = self._find_remote_name_by_listing(requested_file_name)
        if listed_name:
            self._remote_file_names.add(listed_name)
        return listed_name

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
        try:
            stat = await self._client.stat(PurePosixPath(remote_file_path))
            size_raw = stat.get("size") or stat.get("st_size") or 0
            return int(size_raw)
        except Exception as exc:
            if not self._is_recoverable_stat_error(exc):
                raise
            return await asyncio.to_thread(self._remote_size_ftplib_sync, remote_file_path)

    def _remote_size_ftplib_sync(self, remote_file_path: str) -> int:
        try:
            client = self._get_ftplib_client()
        except Exception as exc:
            raise RuntimeError(f"ftplib connect failed: {exc}") from exc
        try:
            size_value = client.size(remote_file_path)
            if size_value is None:
                return 0
            return int(size_value)
        except Exception:
            self._close_ftplib_client()
            raise

    @staticmethod
    def _is_recoverable_stat_error(exc: Exception) -> bool:
        text = str(exc).lower()
        if isinstance(exc, IndexError):
            return True
        if isinstance(exc, (TimeoutError, socket.timeout, ConnectionError)):
            return True
        return (
            "mlst" in text
            or "mlsd" in text
            or "list index out of range" in text
            or "timed out" in text
        )

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
