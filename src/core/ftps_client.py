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
from pathlib import PurePosixPath
from typing import Callable, Optional

import aioftp
from aioftp.errors import StatusCodeError

from core.ftps_connection import (
    build_ssl_context as build_ssl_context_impl,
    close as close_impl,
    close_async as close_async_impl,
    connect as connect_impl,
    connect_async as connect_async_impl,
    run_coro as run_coro_impl,
    start_loop as start_loop_impl,
    stop_loop as stop_loop_impl,
    validate_connection as validate_connection_impl,
)
from core.ftps_ftplib import (
    close_ftplib_client as close_ftplib_client_impl,
    connect_ftplib_explicit as connect_ftplib_explicit_impl,
    get_ftplib_client as get_ftplib_client_impl,
)
from core.ftps_manifest import FTPSManifestStore
from core.ftps_listing import (
    list_dir_detailed_ftplib_sync as list_dir_detailed_ftplib_sync_impl,
    list_remote_entries as list_remote_entries_impl,
    list_remote_entries_ftplib as list_remote_entries_ftplib_impl,
)
from core.ftps_name_resolver import (
    find_remote_name_by_listing,
    fuzzy_remote_candidates,
    fuzzy_remote_candidates_in_dir,
    get_remote_listing_names,
    resolve_remote_name_from_listing,
    sibling_channel_dirs,
)
from core.ftps_status_checker import (
    check_remote_file_status as check_remote_file_status_impl,
    check_remote_file_status_ftplib as check_remote_file_status_ftplib_impl,
)
from core.ftps_uploader import (
    upload_file_if_needed as upload_file_if_needed_impl,
    upload_file_if_needed_async as upload_file_if_needed_async_impl,
)
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

    @staticmethod
    def _list_timeout_seconds() -> int:
        return FTPS_LIST_TIMEOUT_SECONDS

    @staticmethod
    def _long_timeout_seconds() -> int:
        return FTPS_LONG_TIMEOUT_SECONDS

    @staticmethod
    def _timeout_seconds() -> int:
        return FTPS_TIMEOUT_SECONDS

    def _start_loop(self) -> None:
        start_loop_impl(self)

    def _stop_loop(self) -> None:
        stop_loop_impl(self)

    def _run(self, coro, timeout: int = FTPS_LONG_TIMEOUT_SECONDS):
        return run_coro_impl(self, coro, timeout=timeout)

    def _build_ssl_context(self) -> ssl.SSLContext:
        return build_ssl_context_impl(self)

    def connect(self) -> None:
        connect_impl(self)

    async def _connect_async(self) -> None:
        await connect_async_impl(self)

    def close(self) -> None:
        close_impl(self)

    async def _close_async(self) -> None:
        await close_async_impl(self)

    def validate_connection(self) -> tuple[bool, str]:
        return validate_connection_impl(self)

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
        return connect_ftplib_explicit_impl(self)

    def _close_ftplib_client(self) -> None:
        close_ftplib_client_impl(self)

    def _get_ftplib_client(self) -> ftplib.FTP_TLS:
        return get_ftplib_client_impl(self)

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
        return list_dir_detailed_ftplib_sync_impl(self, remote_dir, limit=limit)

    def list_remote_entries_ftplib(self, remote_dir: str, limit: int = 500) -> list[dict]:
        return list_remote_entries_ftplib_impl(self, remote_dir, limit=limit)

    @staticmethod
    def _is_recoverable_error(exc: Exception) -> bool:
        if isinstance(exc, (ssl.SSLEOFError, BrokenPipeError, TimeoutError, socket.timeout, ConnectionError)):
            return True
        if isinstance(exc, OSError) and getattr(exc, "errno", None) in {32, 54, 104, 110}:
            return True
        text = str(exc).lower()
        return (
            "timed out" in text
            or "broken pipe" in text
            or "eof occurred" in text
            or "ftps не подключен" in text
            or "not connected" in text
        )

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
        return upload_file_if_needed_impl(
            self,
            local_file_path=local_file_path,
            progress_callback=progress_callback,
            verify_hash=verify_hash,
            force_upload=force_upload,
        )

    async def _upload_file_if_needed_async(
        self,
        local_file_path: str,
        remote_path: str,
        progress_callback: Optional[Callable[[int, int], None]],
        verify_hash: bool,
        force_upload: bool,
    ) -> tuple[bool, str]:
        return await upload_file_if_needed_async_impl(
            self,
            local_file_path=local_file_path,
            remote_path=remote_path,
            progress_callback=progress_callback,
            verify_hash=verify_hash,
            force_upload=force_upload,
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
        return check_remote_file_status_impl(
            self,
            local_file_path=local_file_path,
            verify_hash=verify_hash,
            local_hash=local_hash,
            remote_path_hint=remote_path_hint,
        )

    def _check_remote_file_status_ftplib(
        self,
        local_file_path: str,
        remote_path: str,
        verify_hash: bool = False,
        local_hash: str = "",
    ) -> tuple[bool, str]:
        return check_remote_file_status_ftplib_impl(
            self,
            local_file_path=local_file_path,
            remote_path=remote_path,
            verify_hash=verify_hash,
            local_hash=local_hash,
        )

    def _find_remote_name_by_listing(self, requested_file_name: str) -> str:
        return find_remote_name_by_listing(self, requested_file_name)

    def _get_remote_listing_names(self) -> set[str]:
        return get_remote_listing_names(self)

    def _fuzzy_remote_candidates(self, requested_file_name: str) -> list[str]:
        return fuzzy_remote_candidates(self, requested_file_name)

    def _fuzzy_remote_candidates_in_dir(self, remote_dir: str, requested_file_name: str) -> list[str]:
        return fuzzy_remote_candidates_in_dir(self, remote_dir, requested_file_name)

    def _sibling_channel_dirs(self) -> list[str]:
        return sibling_channel_dirs(self)

    def list_remote_entries(self, remote_dir: str, limit: int = 200) -> list[dict]:
        return list_remote_entries_impl(self, remote_dir, limit=limit)

    def _resolve_remote_name_from_listing(self, requested_file_name: str) -> str:
        return resolve_remote_name_from_listing(self, requested_file_name)

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
