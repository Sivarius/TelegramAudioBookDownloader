import os
import posixpath
import re


def find_remote_name_by_listing(sync, requested_file_name: str) -> str:
    if not sync._remote_channel_dir:
        return ""
    try:
        listed = sync._run(
            sync._list_names_async(sync._remote_channel_dir),
            timeout=sync._list_timeout_seconds(),
        )
    except Exception:
        return ""
    if not listed:
        return ""
    requested_norm = sync._normalize_name(requested_file_name)
    lower_requested = requested_norm.casefold()
    for name in listed:
        if sync._normalize_name(name) == requested_norm:
            return name
    for name in listed:
        if sync._normalize_name(name).casefold() == lower_requested:
            return name
    return ""


def get_remote_listing_names(sync) -> set[str]:
    if sync._remote_file_names:
        return set(sync._remote_file_names)
    listed: set[str] = set()
    if not sync._remote_channel_dir:
        return listed
    try:
        listed = sync._run(
            sync._list_names_async(sync._remote_channel_dir),
            timeout=sync._list_timeout_seconds(),
        )
    except Exception:
        listed = set()
    if not listed:
        fallback_rows, _ = sync._list_dir_detailed_ftplib_sync(sync._remote_channel_dir, limit=1000)
        listed = {str(item.get("name", "")).strip() for item in fallback_rows if str(item.get("name", "")).strip()}
    if listed:
        sync._remote_file_names.update(listed)
    return set(sync._remote_file_names)


def fuzzy_remote_candidates(sync, requested_file_name: str) -> list[str]:
    listed = get_remote_listing_names(sync)
    if not listed:
        return []
    req_norm = sync._normalize_name(requested_file_name)
    req_lower = req_norm.casefold()
    req_ext = os.path.splitext(req_lower)[1]
    chapter_token = sync._extract_chapter_token(requested_file_name)
    ranked: list[tuple[int, str]] = []
    for name in listed:
        norm = sync._normalize_name(name)
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
    ranked.sort(key=lambda item: (-item[0], sync._normalize_name(item[1]).casefold()))
    return [name for _, name in ranked]


def fuzzy_remote_candidates_in_dir(sync, remote_dir: str, requested_file_name: str) -> list[str]:
    if not remote_dir:
        return []
    listed: set[str] = set()
    try:
        listed = sync._run(
            sync._list_names_async(remote_dir),
            timeout=sync._list_timeout_seconds(),
        )
    except Exception:
        listed = set()
    if not listed:
        fallback_rows, _ = sync._list_dir_detailed_ftplib_sync(remote_dir, limit=1000)
        listed = {str(item.get("name", "")).strip() for item in fallback_rows if str(item.get("name", "")).strip()}
    if not listed:
        return []
    req_norm = sync._normalize_name(requested_file_name)
    req_lower = req_norm.casefold()
    req_ext = os.path.splitext(req_lower)[1]
    chapter_token = sync._extract_chapter_token(requested_file_name)
    ranked: list[tuple[int, str]] = []
    for name in listed:
        norm = sync._normalize_name(name)
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
    ranked.sort(key=lambda item: (-item[0], sync._normalize_name(item[1]).casefold()))
    return [name for _, name in ranked]


def sibling_channel_dirs(sync) -> list[str]:
    base_dir = (sync._base_remote_dir or "").strip()
    if not base_dir:
        return []
    entries: list[tuple[str, str]] = []
    try:
        entries = sync._run(
            sync._list_dir_entries_async(base_dir),
            timeout=sync._list_timeout_seconds(),
        )
    except Exception:
        entries = []
    if not entries:
        fallback_rows, source = sync._list_dir_detailed_ftplib_sync(base_dir, limit=500)
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
    current = (sync._remote_channel_dir or "").rstrip("/")
    return [item for item in dirs if item.rstrip("/") != current]


def resolve_remote_name_from_listing(sync, requested_file_name: str) -> str:
    requested_norm = sync._normalize_name(requested_file_name)
    lower_requested = requested_norm.casefold()

    for name in sync._remote_file_names:
        if sync._normalize_name(name) == requested_norm:
            return name
    for name in sync._remote_file_names:
        if sync._normalize_name(name).casefold() == lower_requested:
            return name

    listed_name = find_remote_name_by_listing(sync, requested_file_name)
    if listed_name:
        sync._remote_file_names.add(listed_name)
    return listed_name
