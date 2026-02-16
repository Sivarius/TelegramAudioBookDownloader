import asyncio
import logging
import os
import threading
import time

from flask import request


def register_action_routes(app, deps: dict) -> None:
    @app.post("/authorize")
    def authorize():
        form = deps["form_from_request"]()
        runtime_state = deps["runtime_state"]

        try:
            settings = deps["build_settings"](form)
        except ValueError as exc:
            return deps["render"](form, str(exc))

        runtime_state["remember_me"] = bool(form["remember_me"])
        runtime_state["session_name"] = settings.session_name
        deps["store_remember_me"](runtime_state["remember_me"])
        deps["store_enable_periodic_checks"](bool(form["enable_periodic_checks"]))
        if not runtime_state["remember_me"]:
            deps["delete_session_files"](settings.session_name)

        proxy_ok, proxy_message = asyncio.run(deps["validate_proxy"](settings))
        if settings.use_mtproxy:
            deps["set_proxy_status"](True, proxy_ok, proxy_message)
            if not proxy_ok:
                return deps["render"](form, proxy_message)
        else:
            deps["set_proxy_status"](False, True, "MTProxy не используется.")

        sftp_ok, sftp_message = asyncio.run(deps["validate_sftp"](settings))
        if settings.use_sftp:
            deps["set_sftp_status"](True, sftp_ok, sftp_message)
            if not sftp_ok:
                return deps["render"](form, sftp_message)
        else:
            deps["set_sftp_status"](False, True, "SFTP не используется.")

        ftps_ok, ftps_message = asyncio.run(deps["validate_ftps"](settings))
        if settings.use_ftps:
            deps["set_ftps_status"](True, ftps_ok, ftps_message)
            if not ftps_ok:
                return deps["render"](form, ftps_message)
        else:
            deps["set_ftps_status"](False, True, "FTPS не используется.")

        deps["store_settings"](settings)
        deps["upsert_current_channel_preference"](settings)

        try:
            ok, auth_message, need_code, need_password = asyncio.run(
                deps["authorize_user"](settings, form["code"], form["password"])
            )
        except Exception as exc:
            deps["set_auth_status"](False, f"Ошибка авторизации: {exc}")
            return deps["render"](form, f"Ошибка авторизации: {exc}")

        if not ok:
            deps["set_auth_status"](False, auth_message)
            return deps["render"](form, auth_message, need_code=need_code, need_password=need_password)

        deps["set_auth_status"](True, auth_message)

        preview_ok, preview_message, items = asyncio.run(deps["fetch_preview"](settings))
        deps["set_preview_cache"](items if preview_ok else [])

        proxy_status = deps["proxy_status"]
        sftp_status = deps["sftp_status"]
        ftps_status = deps["ftps_status"]
        return deps["render"](
            form,
            (
                f"{proxy_status['message']} {sftp_status['message']} {ftps_status['message']} "
                f"{auth_message} {preview_message}"
            ),
        )

    @app.post("/start")
    def start_download():
        form = deps["form_from_request"]()
        runtime_state = deps["runtime_state"]

        try:
            settings = deps["build_settings"](form)
        except ValueError as exc:
            return deps["render"](form, str(exc))

        runtime_state["remember_me"] = bool(form["remember_me"])
        runtime_state["session_name"] = settings.session_name
        deps["store_remember_me"](runtime_state["remember_me"])
        deps["store_enable_periodic_checks"](bool(form["enable_periodic_checks"]))

        proxy_ok, proxy_message = asyncio.run(deps["validate_proxy"](settings))
        if settings.use_mtproxy:
            deps["set_proxy_status"](True, proxy_ok, proxy_message)
            if not proxy_ok:
                return deps["render"](form, proxy_message)
        else:
            deps["set_proxy_status"](False, True, "MTProxy не используется.")

        sftp_ok, sftp_message = asyncio.run(deps["validate_sftp"](settings))
        if settings.use_sftp:
            deps["set_sftp_status"](True, sftp_ok, sftp_message)
            if not sftp_ok:
                return deps["render"](form, sftp_message)
        else:
            deps["set_sftp_status"](False, True, "SFTP не используется.")

        ftps_ok, ftps_message = asyncio.run(deps["validate_ftps"](settings))
        if settings.use_ftps:
            deps["set_ftps_status"](True, ftps_ok, ftps_message)
            if not ftps_ok:
                return deps["render"](form, ftps_message)
        else:
            deps["set_ftps_status"](False, True, "FTPS не используется.")

        deps["store_settings"](settings)
        deps["upsert_current_channel_preference"](settings)

        channel_ok, channel_message = asyncio.run(deps["validate_channel"](settings))
        if not channel_ok:
            return deps["render"](form, channel_message)

        preview_cache = deps["get_preview_cache"]()
        if not preview_cache:
            preview_ok, _, items = asyncio.run(deps["fetch_preview"](settings))
            deps["set_preview_cache"](items if preview_ok else [])
            preview_cache = deps["get_preview_cache"]()

        allowed_message_ids = deps["pick_range_ids"](preview_cache, form["from_index"], form["to_index"])
        if allowed_message_ids is not None and len(allowed_message_ids) == 0:
            return deps["render"](form, "В выбранном диапазоне нет аудио.")

        started, start_message = deps["start_worker"](
            settings, allowed_message_ids, live_mode=True, source="manual"
        )
        if started:
            proxy_status = deps["proxy_status"]
            sftp_status = deps["sftp_status"]
            ftps_status = deps["ftps_status"]
            return deps["render"](
                form,
                (
                    f"{proxy_status['message']} {sftp_status['message']} {ftps_status['message']} "
                    f"{channel_message} {start_message}"
                ),
            )
        return deps["render"](form, start_message)

    @app.post("/start_upload")
    def start_upload():
        form = deps["form_from_request"]()
        runtime_state = deps["runtime_state"]

        try:
            settings = deps["build_settings"](form, require_channel=True)
        except ValueError as exc:
            return deps["render"](form, str(exc))

        runtime_state["remember_me"] = bool(form["remember_me"])
        runtime_state["session_name"] = settings.session_name
        deps["store_remember_me"](runtime_state["remember_me"])
        deps["store_enable_periodic_checks"](bool(form["enable_periodic_checks"]))
        deps["store_settings"](settings)
        deps["upsert_current_channel_preference"](settings)

        sftp_ok, sftp_message = asyncio.run(deps["validate_sftp"](settings))
        if settings.use_sftp:
            deps["set_sftp_status"](True, sftp_ok, sftp_message)
            if not sftp_ok:
                return deps["render"](form, sftp_message)
        else:
            deps["set_sftp_status"](False, True, "SFTP не используется.")

        ftps_ok, ftps_message = asyncio.run(deps["validate_ftps"](settings))
        if settings.use_ftps:
            deps["set_ftps_status"](True, ftps_ok, ftps_message)
            if not ftps_ok:
                return deps["render"](form, ftps_message)
        else:
            deps["set_ftps_status"](False, True, "FTPS не используется.")

        if not settings.use_sftp and not settings.use_ftps:
            return deps["render"](form, "Для upload включите SFTP или FTPS.")

        started, start_message = deps["start_worker"](
            settings,
            allowed_message_ids=None,
            live_mode=False,
            source="manual-upload",
            upload_only=True,
        )
        if started:
            sftp_status = deps["sftp_status"]
            ftps_status = deps["ftps_status"]
            return deps["render"](form, f"{sftp_status['message']} {ftps_status['message']} {start_message}")
        return deps["render"](form, start_message)

    @app.post("/stop_server")
    def stop_server():
        deps["worker_stop_event"].set()
        with deps["worker_lock"]:
            worker_loop = deps["get_worker_loop"]()
            worker_main_task = deps["get_worker_main_task"]()
            if worker_loop and worker_main_task and not worker_main_task.done():
                try:
                    worker_loop.call_soon_threadsafe(worker_main_task.cancel)
                except Exception:
                    logging.exception("Failed to cancel worker task during server stop")
        deps["monitor_stop_event"].set()
        deps["set_status"](message="Остановка сервера...", running=False, mode="idle")

        worker_thread = deps["get_worker_thread"]()
        if worker_thread and worker_thread.is_alive():
            worker_thread.join(timeout=10)
        monitor_thread = deps["get_monitor_thread"]()
        if monitor_thread and monitor_thread.is_alive():
            monitor_thread.join(timeout=5)

        runtime_state = deps["runtime_state"]
        if not runtime_state["remember_me"]:
            deps["delete_session_files"](runtime_state["session_name"])

        shutdown_func = request.environ.get("werkzeug.server.shutdown")
        if shutdown_func is not None:
            shutdown_func()
            return "Сервер остановлен."

        def _force_exit() -> None:
            time.sleep(0.5)
            os._exit(0)

        threading.Thread(target=_force_exit, daemon=True).start()
        return "Сервер остановлен (fallback)."

    @app.post("/stop_download")
    def stop_download():
        form = deps["form_from_request"]()
        worker_thread = deps["get_worker_thread"]()
        if not (worker_thread and worker_thread.is_alive()):
            deps["set_status"](running=False, mode="idle", message="Активная загрузка не выполняется.")
            return deps["render"](form, "Активная загрузка не выполняется.")

        deps["worker_stop_event"].set()
        with deps["worker_lock"]:
            worker_loop = deps["get_worker_loop"]()
            worker_main_task = deps["get_worker_main_task"]()
            if worker_loop and worker_main_task and not worker_main_task.done():
                try:
                    worker_loop.call_soon_threadsafe(worker_main_task.cancel)
                except Exception:
                    logging.exception("Failed to cancel worker task")
        deps["set_status"](message="Запрошена остановка текущей загрузки...")
        worker_thread.join(timeout=15)

        worker_thread = deps["get_worker_thread"]()
        if worker_thread and worker_thread.is_alive():
            return deps["render"](form, "Остановка запрошена, ожидается завершение текущих задач.")

        deps["set_status"](running=False, mode="idle", message="Текущая задача остановлена.")
        return deps["render"](form, "Текущая задача остановлена.")
