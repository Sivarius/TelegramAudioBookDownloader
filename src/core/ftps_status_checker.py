import hashlib
import os
import posixpath


def check_remote_file_status_ftplib(
    sync,
    local_file_path: str,
    remote_path: str,
    verify_hash: bool = False,
    local_hash: str = "",
) -> tuple[bool, str]:
    try:
        client = sync._get_ftplib_client()
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
        should_verify_hash = bool(verify_hash and sync.settings.ftps_verify_hash)
        if should_verify_hash and size_match:
            local_hash_value = local_hash.strip() if local_hash else sync._sha256_local(local_file_path)
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
        sync._close_ftplib_client()
        text = str(exc).lower()
        if "550" in text or "not found" in text:
            return False, f"remote_missing; remote={remote_path}"
        return False, f"remote_missing; remote={remote_path}; ftplib_check_failed={exc}"


def check_remote_file_status(
    sync,
    local_file_path: str,
    verify_hash: bool = False,
    local_hash: str = "",
    remote_path_hint: str = "",
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

    def _remember_verified(remote_verified_path: str, cache_name: str = "") -> None:
        remembered_name = (cache_name or "").strip() or os.path.basename(remote_verified_path)
        if remembered_name:
            sync._remote_file_names.add(remembered_name)
        hash_for_manifest = ""
        if sync.settings.ftps_verify_hash and verify_hash:
            hash_for_manifest = (local_hash or "").strip().lower() or sync._sha256_local(local_file_path)
        sync._record_verified_manifest_locked(local_file_path, remote_verified_path, hash_for_manifest)

    def _verify_candidate(remote_path: str, matched_by: str) -> tuple[bool, str]:
        size_match, hash_match, local_hash_dbg, remote_hash_dbg = sync._verify_remote_with_diagnostics(
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

    manifest_ok, manifest_remote_path = sync._manifest_match_locked(
        local_file_path,
        verify_hash=verify_hash,
        local_hash=local_hash,
    )
    if manifest_ok:
        remote_candidate = manifest_remote_path or ""
        hinted_candidate = sync._normalize_remote_dir(remote_path_hint)
        if hinted_candidate:
            remote_candidate = hinted_candidate
        if not remote_candidate:
            remote_candidate = posixpath.join(sync._remote_channel_dir, file_name)
        try:
            if check_remote_file_status_ftplib(
                sync,
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

    hinted_path = sync._normalize_remote_dir(remote_path_hint)
    if hinted_path:
        try:
            verified, info = _verify_candidate(hinted_path, "db_cache")
            return verified, info
        except Exception:
            ok_fb, info_fb = check_remote_file_status_ftplib(
                sync,
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

    remote_name = sync._resolve_remote_name_from_listing(file_name)
    if not remote_name:
        direct_path = posixpath.join(sync._remote_channel_dir, file_name)
        try:
            verified, info = _verify_candidate(direct_path, "direct_path")
            return verified, info
        except Exception:
            pass

        candidates = sync._fuzzy_remote_candidates(file_name)
        present_mismatch_info = ""
        for candidate_name in candidates[:15]:
            candidate_path = posixpath.join(sync._remote_channel_dir, candidate_name)
            try:
                verified, info = _verify_candidate(candidate_path, "fuzzy")
            except Exception:
                continue
            if verified:
                return verified, info
            if not present_mismatch_info:
                present_mismatch_info = info
        for sibling_dir in sync._sibling_channel_dirs():
            sibling_candidates = sync._fuzzy_remote_candidates_in_dir(sibling_dir, file_name)
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
        remote_path = posixpath.join(sync._remote_channel_dir, file_name)
        if present_mismatch_info:
            return False, present_mismatch_info
        if candidates:
            sample_path = posixpath.join(sync._remote_channel_dir, candidates[0])
            ok_fb, info_fb = check_remote_file_status_ftplib(
                sync,
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
    remote_path = posixpath.join(sync._remote_channel_dir, remote_name)
    try:
        verified, info = _verify_candidate(remote_path, "final")
        return verified, info
    except Exception as verify_exc:
        ok_fb, info_fb = check_remote_file_status_ftplib(
            sync,
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
