import posixpath


def list_dir_detailed_ftplib_sync(sync, remote_dir: str, limit: int = 500) -> tuple[list[dict], str]:
    try:
        client = sync._connect_ftplib_explicit()
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


def list_remote_entries_ftplib(sync, remote_dir: str, limit: int = 500) -> list[dict]:
    normalized = sync._normalize_remote_dir(remote_dir)
    if not normalized:
        normalized = "/"
    rows, source = list_dir_detailed_ftplib_sync(sync, normalized, limit=max(1, int(limit)))
    if not rows:
        return []
    rows.sort(key=lambda item: (str(item.get("type", "")) != "dir", str(item.get("name", "")).casefold()))
    for row in rows:
        row["list_source"] = f"{normalized} [{source or 'ftplib'}]"
    return rows


def list_remote_entries(sync, remote_dir: str, limit: int = 200) -> list[dict]:
    if not sync.enabled:
        return []
    if not sync._client:
        raise RuntimeError("FTPS не подключен.")
    normalized = sync._normalize_remote_dir(remote_dir)
    if not normalized:
        normalized = "/"
    candidates: list[str] = []
    for item in (normalized, normalized.lstrip("/"), f"/{normalized.lstrip('/')}"):
        value = sync._normalize_remote_dir(item)
        if not value:
            continue
        if value not in candidates:
            candidates.append(value)
    if "." not in candidates:
        candidates.append(".")

    rows: list[dict] = []
    used = ""
    with sync._io_lock:
        for candidate in candidates:
            try:
                current = sync._run(
                    sync._list_dir_detailed_async(candidate),
                    timeout=sync._list_timeout_seconds(),
                )
            except Exception:
                current = []
            source = "aioftp:list"
            if not current:
                current, fb_source = list_dir_detailed_ftplib_sync(sync, candidate, limit=max(200, limit))
                if fb_source:
                    source = fb_source
            if current:
                rows = current
                used = f"{candidate} [{source}]"
                break
        if not rows:
            try:
                rows = sync._run(
                    sync._list_dir_detailed_async(candidates[0]),
                    timeout=sync._list_timeout_seconds(),
                )
            except Exception:
                rows = []
            source = "aioftp:list"
            if not rows:
                rows, fb_source = list_dir_detailed_ftplib_sync(sync, candidates[0], limit=max(200, limit))
                if fb_source:
                    source = fb_source
            used = f"{candidates[0]} [{source}]"
    rows = rows[: max(1, int(limit))]
    rows.sort(key=lambda item: (str(item.get("type", "")) != "dir", str(item.get("name", "")).casefold()))
    for row in rows:
        row["list_source"] = used
    return rows
