import asyncio
import logging
import time
from datetime import datetime
from typing import Callable, Optional

from core.downloader import run_downloader, run_remote_uploader


def build_status_hook(
    set_status: Callable[..., None],
    worker_status: dict,
) -> Callable[[dict], None]:
    def _update_progress_item(
        message_id: str,
        *,
        percent: Optional[int] = None,
        received: Optional[int] = None,
        total: Optional[int] = None,
        state: Optional[str] = None,
        file_path: Optional[str] = None,
        speed_bps: Optional[float] = None,
        eta_sec: Optional[float] = None,
        error: Optional[str] = None,
    ) -> None:
        current = dict(worker_status.get("file_progresses", {}))
        item = dict(current.get(message_id, {}))
        if percent is not None:
            item["percent"] = int(percent)
        if received is not None:
            item["received"] = int(received)
        if total is not None:
            item["total"] = int(total)
        if state is not None:
            item["state"] = state
        if file_path:
            item["file_path"] = file_path
        if speed_bps is not None:
            item["speed_bps"] = float(speed_bps)
        if eta_sec is not None:
            item["eta_sec"] = float(eta_sec)
        if error is not None:
            item["error"] = str(error)
        item["updated_at"] = datetime.now().strftime("%H:%M:%S")
        current[message_id] = item
        if len(current) > 40:
            keys = list(current.keys())
            for key in keys[:-40]:
                current.pop(key, None)
        set_status(file_progresses=current)

    def _update_upload_progress_item(
        transfer_id: str,
        *,
        percent: Optional[int] = None,
        received: Optional[int] = None,
        total: Optional[int] = None,
        state: Optional[str] = None,
        file_path: Optional[str] = None,
        speed_bps: Optional[float] = None,
        eta_sec: Optional[float] = None,
        error: Optional[str] = None,
    ) -> None:
        current = dict(worker_status.get("upload_file_progresses", {}))
        item = dict(current.get(transfer_id, {}))
        if percent is not None:
            item["percent"] = int(percent)
        if received is not None:
            item["received"] = int(received)
        if total is not None:
            item["total"] = int(total)
        if state is not None:
            item["state"] = state
        if file_path:
            item["file_path"] = file_path
        if speed_bps is not None:
            item["speed_bps"] = float(speed_bps)
        if eta_sec is not None:
            item["eta_sec"] = float(eta_sec)
        if error is not None:
            item["error"] = str(error)
        item["updated_at"] = datetime.now().strftime("%H:%M:%S")
        current[transfer_id] = item
        if len(current) > 40:
            keys = list(current.keys())
            for key in keys[:-40]:
                current.pop(key, None)
        set_status(upload_file_progresses=current)

    def _status_hook(payload: dict) -> None:
        progress_runtime = getattr(_status_hook, "_progress_runtime", {})
        upload_runtime = getattr(_status_hook, "_upload_runtime", {})
        setattr(_status_hook, "_progress_runtime", progress_runtime)
        setattr(_status_hook, "_upload_runtime", upload_runtime)
        event = payload.get("event", "")
        message_id = str(payload.get("message_id", ""))
        is_upload_transfer = message_id.startswith("upload:")
        if event == "downloading":
            _update_progress_item(message_id, percent=0, received=0, total=0, state="downloading")
            progress_runtime[message_id] = {
                "last_received": 0,
                "last_ts": time.time(),
                "speed_bps": 0.0,
            }
            set_status(
                current_message_id=message_id,
                message=f"Скачивание message_id={message_id}",
                progress_percent=0,
                progress_received=0,
                progress_total=0,
            )
        elif event == "progress":
            received = int(payload.get("received", 0))
            total = int(payload.get("total", 0))
            now = time.time()
            state = progress_runtime.get(
                message_id, {"last_received": 0, "last_ts": now, "speed_bps": 0.0}
            )
            delta_bytes = received - int(state.get("last_received", 0))
            delta_time = now - float(state.get("last_ts", now))
            speed_bps = float(state.get("speed_bps", 0.0))
            if delta_bytes > 0 and delta_time > 0.2:
                speed_bps = delta_bytes / delta_time
            eta_sec = 0.0
            if speed_bps > 1 and total > received:
                eta_sec = (total - received) / speed_bps
            progress_runtime[message_id] = {
                "last_received": received,
                "last_ts": now,
                "speed_bps": speed_bps,
            }
            _update_progress_item(
                message_id,
                percent=int(payload.get("percent", 0)),
                received=received,
                total=total,
                state="downloading",
                speed_bps=speed_bps,
                eta_sec=eta_sec,
            )
            set_status(
                current_message_id=message_id,
                progress_percent=int(payload.get("percent", 0)),
                progress_received=received,
                progress_total=total,
            )
        elif event == "downloaded":
            _update_progress_item(
                message_id,
                percent=100,
                state="done",
                file_path=str(payload.get("file_path", "")),
                eta_sec=0.0,
            )
            progress_runtime.pop(message_id, None)
            set_status(
                downloaded=int(worker_status.get("downloaded", 0)) + 1,
                current_message_id=message_id,
                last_file=str(payload.get("file_path", "")),
                progress_percent=100,
                message=f"Скачан message_id={message_id}",
            )
        elif event == "failed":
            _update_progress_item(message_id, state="failed")
            progress_runtime.pop(message_id, None)
            set_status(
                failed=int(worker_status.get("failed", 0)) + 1,
                current_message_id=message_id,
                message=f"Ошибка message_id={message_id}",
            )
        elif event == "skipped":
            set_status(skipped=int(worker_status.get("skipped", 0)) + 1)
        elif event == "throttled":
            set_status(
                current_message_id=message_id,
                current_concurrency=int(payload.get("concurrency", worker_status.get("current_concurrency", 1))),
                message=(
                    f"FloodWait {payload.get('seconds', '?')}s; "
                    f"понижаю параллельность до {payload.get('concurrency', 1)}"
                ),
            )
        elif event == "sftp_ready":
            set_status(message=str(payload.get("message", "Удаленный протокол готов.")))
        elif event == "sftp_uploaded":
            if not is_upload_transfer:
                _update_progress_item(message_id, state="sftp_uploaded")
            _update_upload_progress_item(message_id, state="uploaded", percent=100, eta_sec=0.0)
            upload_runtime.pop(message_id, None)
            transport = str(payload.get("transport", "SFTP"))
            set_status(
                sftp_uploaded=int(worker_status.get("sftp_uploaded", 0)) + 1,
                message=f"{transport} загружен: message_id={message_id}",
            )
        elif event == "sftp_skipped":
            if not is_upload_transfer:
                _update_progress_item(message_id, state="sftp_skipped")
            _update_upload_progress_item(message_id, state="skipped", eta_sec=0.0)
            upload_runtime.pop(message_id, None)
            transport = str(payload.get("transport", "SFTP"))
            set_status(
                sftp_skipped=int(worker_status.get("sftp_skipped", 0)) + 1,
                message=f"{transport} пропуск: message_id={message_id}",
            )
        elif event == "sftp_failed":
            reason = str(payload.get("reason", "")).strip()
            if not is_upload_transfer:
                _update_progress_item(message_id, state="sftp_failed", error=reason)
            _update_upload_progress_item(message_id, state="failed", error=reason)
            upload_runtime.pop(message_id, None)
            transport = str(payload.get("transport", "SFTP"))
            if reason:
                logging.error("%s failed message_id=%s reason=%s", transport, message_id, reason)
            set_status(
                sftp_failed=int(worker_status.get("sftp_failed", 0)) + 1,
                message=(f"{transport} ошибка: message_id={message_id}" + (f" ({reason})" if reason else "")),
            )
        elif event == "uploading":
            transfer_id = message_id or str(payload.get("file_path", "upload"))
            _update_upload_progress_item(
                transfer_id,
                percent=0,
                received=0,
                total=0,
                state="uploading",
                file_path=str(payload.get("file_path", "")),
                speed_bps=0.0,
                eta_sec=0.0,
            )
            upload_runtime[transfer_id] = {
                "last_received": 0,
                "last_ts": time.time(),
                "speed_bps": 0.0,
                "started_ts": time.time(),
            }
            set_status(message=f"Upload: {transfer_id}")
        elif event == "upload_progress":
            transfer_id = message_id or str(payload.get("file_path", "upload"))
            received = int(payload.get("received", 0))
            total = int(payload.get("total", 0))
            now = time.time()
            state = upload_runtime.get(
                transfer_id,
                {"last_received": 0, "last_ts": now, "speed_bps": 0.0, "started_ts": now},
            )
            delta_bytes = received - int(state.get("last_received", 0))
            delta_time = now - float(state.get("last_ts", now))
            started_ts = float(state.get("started_ts", now))
            speed_bps_prev = float(state.get("speed_bps", 0.0))
            speed_bps_inst = 0.0
            if delta_bytes > 0 and delta_time > 0:
                speed_bps_inst = delta_bytes / delta_time
            avg_time = max(0.001, now - started_ts)
            speed_bps_avg = float(received) / avg_time if received > 0 else 0.0
            speed_bps = max(speed_bps_inst, speed_bps_avg, speed_bps_prev)
            eta_sec = 0.0
            if speed_bps > 1 and total > received:
                eta_sec = (total - received) / speed_bps
            upload_runtime[transfer_id] = {
                "last_received": received,
                "last_ts": now,
                "speed_bps": speed_bps,
                "started_ts": started_ts,
            }
            _update_upload_progress_item(
                transfer_id,
                percent=int(payload.get("percent", 0)),
                received=received,
                total=total,
                state="uploading",
                speed_bps=speed_bps,
                eta_sec=eta_sec,
            )
            set_status(
                upload_progress_percent=int(payload.get("percent", 0)),
                upload_progress_received=received,
                upload_progress_total=total,
                upload_progress_speed_bps=speed_bps,
                upload_progress_eta_sec=eta_sec,
            )
        elif event == "upload_started":
            set_status(
                current_concurrency=int(payload.get("concurrency", worker_status.get("current_concurrency", 1))),
                message=str(payload.get("message", "Upload started.")),
            )
        elif event == "upload_done":
            transfer_id = message_id or ""
            if transfer_id:
                final_speed = 0.0
                state = upload_runtime.get(transfer_id, {})
                if state:
                    started_ts = float(state.get("started_ts", 0.0))
                    last_ts = float(state.get("last_ts", 0.0))
                    last_received = int(state.get("last_received", 0))
                    if started_ts > 0 and last_ts > started_ts and last_received > 0:
                        final_speed = float(last_received) / (last_ts - started_ts)
                _update_upload_progress_item(
                    transfer_id,
                    state="done",
                    percent=100,
                    eta_sec=0.0,
                    speed_bps=final_speed if final_speed > 0 else None,
                )
                upload_runtime.pop(transfer_id, None)
                if final_speed > 0:
                    set_status(upload_progress_speed_bps=final_speed)
            set_status(upload_progress_eta_sec=0.0)
        elif event == "local_cleaned":
            transport = str(payload.get("transport", "SFTP"))
            set_status(message=f"Локальный файл удален после {transport}: message_id={message_id}")

    return _status_hook


def run_worker_target(
    settings,
    db_path,
    allowed_message_ids,
    *,
    upload_only: bool,
    live_mode: bool,
    worker_stop_event,
    set_status: Callable[..., None],
    status_hook: Callable[[dict], None],
    set_worker_runtime: Callable[[Optional[asyncio.AbstractEventLoop], Optional[asyncio.Task]], None],
) -> None:
    loop: Optional[asyncio.AbstractEventLoop] = None
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        main_task = loop.create_task(
            run_remote_uploader(
                settings=settings,
                db_path=db_path,
                status_hook=status_hook,
                stop_requested=lambda: worker_stop_event.is_set(),
            )
            if upload_only
            else run_downloader(
                settings=settings,
                db_path=db_path,
                allowed_message_ids=allowed_message_ids,
                status_hook=status_hook,
                stop_requested=lambda: worker_stop_event.is_set(),
                live_mode=live_mode,
            )
        )
        set_worker_runtime(loop, main_task)
        loop.run_until_complete(main_task)
    except asyncio.CancelledError:
        set_status(message="Загрузка прервана пользователем.", running=False, mode="idle")
    except Exception as exc:
        logging.exception("Downloader crashed")
        set_status(message=f"Ошибка загрузчика: {exc}", running=False, mode="idle")
    finally:
        set_worker_runtime(None, None)
        if loop is not None:
            try:
                pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
                for task in pending:
                    task.cancel()
                if pending:
                    loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            except Exception:
                logging.exception("Failed to finalize worker event loop")
            finally:
                loop.close()
        set_status(running=False, mode="idle")
