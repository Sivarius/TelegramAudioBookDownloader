import os
import posixpath
from pathlib import PurePosixPath
from typing import Callable, Optional


def upload_file_if_needed(
    sync,
    local_file_path: str,
    progress_callback: Optional[Callable[[int, int], None]] = None,
    verify_hash: bool = False,
    force_upload: bool = False,
) -> tuple[bool, str]:
    if not sync.enabled:
        return False, "FTPS выключен."
    if not sync._client:
        return False, "FTPS не подключен."
    if not sync._remote_channel_dir:
        return False, "Не подготовлен удаленный каталог."

    file_name = os.path.basename(local_file_path)
    if not file_name:
        return False, "Пустое имя файла."

    with sync._io_lock:
        attempts = 3
        last_exc: Optional[Exception] = None
        for attempt in range(1, attempts + 1):
            try:
                remote_path = posixpath.join(sync._remote_channel_dir, file_name)
                uploaded, reason = sync._run(
                    upload_file_if_needed_async(
                        sync,
                        local_file_path,
                        remote_path,
                        progress_callback,
                        verify_hash,
                        force_upload,
                    ),
                    timeout=max(sync._long_timeout_seconds(), 240),
                )
                return uploaded, reason
            except Exception as exc:
                last_exc = exc
                if attempt >= attempts or not sync._is_recoverable_error(exc):
                    raise
                try:
                    sync._reconnect_current_channel_dir_locked()
                except Exception as reconnect_exc:
                    last_exc = reconnect_exc
                    if attempt >= attempts:
                        raise reconnect_exc
        if last_exc:
            raise last_exc
        raise RuntimeError("FTPS upload failed for unknown reason.")


async def upload_file_if_needed_async(
    sync,
    local_file_path: str,
    remote_path: str,
    progress_callback: Optional[Callable[[int, int], None]],
    verify_hash: bool,
    force_upload: bool,
) -> tuple[bool, str]:
    if not sync._client:
        raise RuntimeError("FTPS не подключен.")

    file_name = os.path.basename(local_file_path)
    uploaded = False
    reuploaded = False

    remote_name = sync._resolve_remote_name_from_listing(file_name) or file_name
    remote_path = posixpath.join(sync._remote_channel_dir, remote_name)

    remote_exists = False
    if remote_name in sync._remote_file_names:
        remote_exists = True
    else:
        remote_exists = await sync._remote_exists_async(remote_path)

    if remote_exists and not force_upload:
        manifest_ok, manifest_remote_path = sync._manifest_match_locked(
            local_file_path,
            verify_hash=verify_hash,
            local_hash="",
        )
        if manifest_ok:
            manifest_path = manifest_remote_path or remote_path
            try:
                if await sync._remote_exists_async(manifest_path):
                    sync._remote_file_names.add(file_name)
                    return (
                        False,
                        f"already_exists; remote={manifest_path}; size_match=True; hash_match=True; verified=True; matched_by=manifest",
                    )
            except Exception:
                pass

        size_match, hash_match = await sync._verify_remote_file_async(
            local_file_path,
            remote_path,
            verify_hash=verify_hash,
        )
        verified = size_match and hash_match
        if verified:
            sync._remote_file_names.add(file_name)
            return (
                False,
                f"already_exists; remote={remote_path}; size_match={size_match}; hash_match={hash_match}; verified={verified}",
            )
        reuploaded = True

    if force_upload or not remote_exists or reuploaded:
        total_size = os.path.getsize(local_file_path)
        sent_bytes = 0
        with open(local_file_path, "rb") as file_obj:
            async with sync._client.upload_stream(PurePosixPath(remote_path), offset=0) as stream:
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
        sync._remote_file_names.add(file_name)
        uploaded = True

    size_match, hash_match = await sync._verify_remote_file_async(
        local_file_path,
        remote_path,
        verify_hash=verify_hash,
    )
    local_hash_dbg = ""
    remote_hash_dbg = ""
    if verify_hash and size_match and not hash_match:
        forced_local_hash = sync._sha256_local(local_file_path)
        size_match, hash_match = await sync._verify_remote_file_async(
            local_file_path,
            remote_path,
            verify_hash=True,
            local_hash=forced_local_hash,
        )
        local_hash_dbg = forced_local_hash
        try:
            remote_hash_dbg, _ = await sync._sha256_remote_async(remote_path)
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
    if sync.settings.ftps_verify_hash and verify_hash:
        local_hash_final = sync._sha256_local(local_file_path)
    sync._record_verified_manifest_locked(local_file_path, remote_path, local_hash_final)
    return (
        uploaded,
        f"{reason}; remote={remote_path}; size_match={size_match}; hash_match={hash_match}; verified={verified}",
    )
