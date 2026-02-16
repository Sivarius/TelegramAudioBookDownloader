import json
import os
import posixpath
import time
from io import BytesIO
from typing import Callable


class FTPSManifestStore:
    def __init__(
        self,
        *,
        get_ftplib_client: Callable[[], object],
        close_ftplib_client: Callable[[], None],
        normalize_name: Callable[[str], str],
        sha256_local: Callable[[str], str],
        verify_hash_enabled: Callable[[], bool],
        manifest_file_name: str,
    ) -> None:
        self._get_ftplib_client = get_ftplib_client
        self._close_ftplib_client = close_ftplib_client
        self._normalize_name = normalize_name
        self._sha256_local = sha256_local
        self._verify_hash_enabled = verify_hash_enabled
        self._manifest_file_name = manifest_file_name

        self.entries: dict[str, dict] = {}
        self.loaded = False
        self.dirty = False
        self.remote_channel_dir = ""

    def reset(self, remote_channel_dir: str) -> None:
        self.remote_channel_dir = remote_channel_dir or ""
        self.entries = {}
        self.loaded = False
        self.dirty = False

    def manifest_remote_path(self) -> str:
        if not self.remote_channel_dir:
            return ""
        return posixpath.join(self.remote_channel_dir, self._manifest_file_name)

    def load(self) -> None:
        if self.loaded:
            return
        self.entries = {}
        self.dirty = False
        manifest_path = self.manifest_remote_path()
        if not manifest_path:
            self.loaded = True
            return
        try:
            client = self._get_ftplib_client()
            buffer = BytesIO()
            client.retrbinary(f"RETR {manifest_path}", buffer.write, blocksize=1024 * 64)
            payload = buffer.getvalue().decode("utf-8", errors="ignore").strip()
            if payload:
                raw = json.loads(payload)
                if isinstance(raw, dict):
                    files = raw.get("files", raw)
                    if isinstance(files, dict):
                        for name, item in files.items():
                            if not isinstance(item, dict):
                                continue
                            key = self._normalize_name(str(name))
                            self.entries[key] = {
                                "size": int(item.get("size", 0) or 0),
                                "sha256": str(item.get("sha256", "") or "").strip().lower(),
                                "remote_path": str(item.get("remote_path", "") or "").strip(),
                                "updated_at": str(item.get("updated_at", "") or "").strip(),
                            }
        except Exception as exc:
            text = str(exc).lower()
            if "550" not in text and "not found" not in text:
                self._close_ftplib_client()
        finally:
            self.loaded = True

    def flush(self) -> None:
        if not self.loaded:
            self.load()
        if not self.dirty:
            return
        manifest_path = self.manifest_remote_path()
        if not manifest_path:
            return
        temp_path = f"{manifest_path}.tmp"
        data = {"version": 1, "files": self.entries}
        payload = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        client = self._get_ftplib_client()
        try:
            client.storbinary(f"STOR {temp_path}", BytesIO(payload), blocksize=1024 * 64)
            try:
                client.delete(manifest_path)
            except Exception:
                pass
            try:
                client.rename(temp_path, manifest_path)
            except Exception:
                client.storbinary(f"STOR {manifest_path}", BytesIO(payload), blocksize=1024 * 64)
                try:
                    client.delete(temp_path)
                except Exception:
                    pass
            self.dirty = False
        except Exception:
            self._close_ftplib_client()
            raise

    def match(self, local_file_path: str, *, verify_hash: bool = False, local_hash: str = "") -> tuple[bool, str]:
        if not self.loaded:
            self.load()
        file_name = self._normalize_name(os.path.basename(local_file_path))
        entry = self.entries.get(file_name)
        if not entry:
            return False, ""
        local_size = int(os.path.getsize(local_file_path))
        size_match = int(entry.get("size", 0) or 0) == local_size
        if not size_match:
            return False, "manifest_size_mismatch"
        hash_match = True
        should_verify_hash = bool(verify_hash and self._verify_hash_enabled())
        if should_verify_hash:
            local_hash_value = (local_hash or "").strip().lower() or self._sha256_local(local_file_path)
            remote_hash_value = str(entry.get("sha256", "") or "").strip().lower()
            hash_match = bool(remote_hash_value) and remote_hash_value == local_hash_value
        if not hash_match:
            return False, "manifest_hash_mismatch"
        remote_path = str(entry.get("remote_path", "") or "").strip()
        return True, remote_path

    def upsert(self, local_file_path: str, remote_path: str, *, local_hash: str = "") -> None:
        if not self.loaded:
            self.load()
        file_name = self._normalize_name(os.path.basename(local_file_path))
        if not file_name:
            return
        hash_value = (local_hash or "").strip().lower()
        if self._verify_hash_enabled() and not hash_value:
            hash_value = self._sha256_local(local_file_path)
        self.entries[file_name] = {
            "size": int(os.path.getsize(local_file_path)),
            "sha256": hash_value,
            "remote_path": (remote_path or "").strip(),
            "updated_at": str(int(time.time())),
        }
        self.dirty = True

    def record_verified(self, local_file_path: str, remote_path: str, *, local_hash: str = "") -> None:
        try:
            self.upsert(local_file_path, remote_path, local_hash=local_hash)
            self.flush()
        except Exception:
            # Manifest is an optimization layer; do not break main flow.
            pass
