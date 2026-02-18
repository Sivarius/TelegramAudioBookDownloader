import asyncio
import json
from dataclasses import replace

from flask import Response, jsonify, request, stream_with_context

from core.db import AppDatabase


def register_basic_routes(app, deps: dict) -> None:
    @app.get("/")
    def index():
        return deps["render"](deps["load_saved_form"]())

    @app.get("/status")
    def status():
        return jsonify(deps["status_payload"]())

    @app.get("/status_stream")
    def status_stream():
        @stream_with_context
        def _event_stream():
            last_seen = -1
            while True:
                status_cond = deps["status_cond"]
                with status_cond:
                    if last_seen == -1:
                        payload = deps["status_payload"]()
                        last_seen = deps["get_status_version"]()
                    else:
                        status_cond.wait(timeout=25)
                        payload = deps["status_payload"]()
                        last_seen = deps["get_status_version"]()
                yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"

        return Response(_event_stream(), mimetype="text/event-stream")

    @app.get("/debug_logs")
    def debug_logs():
        debug_log_lock = deps["debug_log_lock"]
        debug_log_buffer = deps["debug_log_buffer"]
        with debug_log_lock:
            lines = list(debug_log_buffer)
        return jsonify({"lines": lines, "count": len(lines)})

    @app.post("/debug_mode")
    def debug_mode():
        enabled = request.form.get("enabled", "0") == "1"
        deps["set_debug_mode"](enabled)
        return jsonify({"ok": True, "enabled": enabled})

    @app.post("/preview")
    def refresh_preview():
        form = deps["form_from_request"]()
        deps["store_enable_periodic_checks"](bool(form["enable_periodic_checks"]))

        try:
            settings = deps["build_settings"](form)
        except ValueError as exc:
            return deps["render"](form, str(exc))
        deps["store_settings"](settings)

        deps["upsert_current_channel_preference"](settings)
        preview_ok, preview_message, items = asyncio.run(deps["fetch_preview"](settings))
        deps["set_preview_cache"](items if preview_ok else [])

        return deps["render"](
            form,
            f"{deps['proxy_status']['message']} {deps['sftp_status']['message']} {deps['ftps_status']['message']} {preview_message}",
        )

    @app.post("/preview_ftps")
    def refresh_ftps_preview():
        form = deps["form_from_request"]()
        deps["store_enable_periodic_checks"](bool(form["enable_periodic_checks"]))
        try:
            settings = deps["build_settings"](form, require_channel=False)
        except ValueError as exc:
            return deps["render"](form, str(exc))
        deps["store_settings"](settings)

        ok, message, items = asyncio.run(deps["fetch_ftps_preview"](settings))
        deps["set_ftps_remote_preview_cache"](items if ok else [])
        deps["set_ftps_remote_preview_meta"](message)
        return deps["render"](form, message)

    @app.post("/check_sftp")
    def check_sftp():
        form = deps["form_from_request"]()
        try:
            settings = deps["build_settings"](form, require_channel=False)
        except ValueError as exc:
            return deps["render"](form, str(exc))
        deps["store_settings"](settings)

        sftp_ok, sftp_message = asyncio.run(deps["validate_sftp"](settings))
        if settings.use_sftp:
            deps["set_sftp_status"](True, sftp_ok, sftp_message)
        else:
            deps["set_sftp_status"](False, True, "SFTP не используется.")
        return deps["render"](form, deps["sftp_status"]["message"])

    @app.post("/check_ftps")
    def check_ftps():
        form = deps["form_from_request"]()
        try:
            settings = deps["build_settings"](form, require_channel=False)
        except ValueError as exc:
            return deps["render"](form, str(exc))
        deps["store_settings"](settings)

        ftps_ok, ftps_message = asyncio.run(deps["validate_ftps"](settings))
        if settings.use_ftps:
            deps["set_ftps_status"](True, ftps_ok, ftps_message)
            if ftps_ok and (settings.channel or "").strip() and (settings.channel or "").strip() != "_":
                audit_ok, audit_message = asyncio.run(deps["audit_ftps_selected_channel"](settings))
                ftps_message = f"{ftps_message} {audit_message}".strip()
                if audit_ok:
                    preview_ok, _, items = asyncio.run(deps["fetch_preview"](settings))
                    if preview_ok:
                        deps["set_preview_cache"](items)
        else:
            deps["set_ftps_status"](False, True, "FTPS не используется.")
        return deps["render"](form, ftps_message)

    @app.post("/warmup_ftps_manifest")
    def warmup_ftps_manifest():
        form = deps["form_from_request"]()
        try:
            settings = deps["build_settings"](form, require_channel=False)
        except ValueError as exc:
            return deps["render"](form, str(exc))
        deps["store_settings"](settings)

        if not settings.use_ftps:
            deps["set_ftps_status"](False, True, "FTPS не используется.")
            return deps["render"](form, "Прогрев manifest: FTPS не используется.")

        ftps_ok, ftps_message = asyncio.run(deps["validate_ftps"](settings))
        deps["set_ftps_status"](True, ftps_ok, ftps_message)
        if not ftps_ok:
            return deps["render"](form, f"Прогрев manifest: {ftps_message}")

        if not (settings.channel or "").strip() or (settings.channel or "").strip() == "_":
            return deps["render"](form, "Прогрев manifest: укажите CHANNEL_ID.")

        warmup_settings = replace(settings, ftps_verify_hash=False)
        audit_ok, audit_message = asyncio.run(deps["audit_ftps_selected_channel"](warmup_settings))
        result_message = (
            "Прогрев manifest завершен. "
            "Проверка выполнялась без SHA-256 (только быстрый режим). "
            f"{audit_message}"
        )
        if audit_ok:
            preview_ok, _, items = asyncio.run(deps["fetch_preview"](settings))
            if preview_ok:
                deps["set_preview_cache"](items)
        return deps["render"](form, result_message)

    @app.post("/channels_status")
    def channels_status():
        form = deps["form_from_request"]()
        deps["store_enable_periodic_checks"](bool(form["enable_periodic_checks"]))
        refresh = request.form.get("refresh", "0") == "1"

        if refresh:
            try:
                settings = deps["build_settings"](form, require_channel=False)
            except ValueError as exc:
                return jsonify({"ok": False, "message": str(exc), "items": []}), 400
            ok, message, items = asyncio.run(deps["collect_saved_channels_status"](settings))
        else:
            ok, message, items = deps["collect_saved_channels_cached"]()
        return jsonify({"ok": ok, "message": message, "items": items})

    @app.post("/channels_delete")
    def channels_delete():
        channel_ref = request.form.get("channel_ref", "").strip()
        if not channel_ref:
            return jsonify({"ok": False, "message": "channel_ref is required"}), 400
        db = AppDatabase(deps["db_path"])
        try:
            db.delete_channel_data(channel_ref)
        finally:
            db.close()
        return jsonify({"ok": True, "message": f"Канал удален из истории: {channel_ref}"})

    @app.post("/channels_preferences_update")
    def channels_preferences_update():
        form = deps["form_from_request"]()
        deps["store_enable_periodic_checks"](bool(form["enable_periodic_checks"]))
        try:
            settings = deps["build_settings"](form, require_channel=False)
        except ValueError as exc:
            return jsonify({"ok": False, "message": str(exc)}), 400

        channel_ref = request.form.get("channel_ref", "").strip()
        channel_id = deps["safe_int"](request.form.get("channel_id", "0"), 0)
        channel_title = request.form.get("channel_title", "").strip()
        check_new = request.form.get("check_new") == "1"
        auto_download = request.form.get("auto_download") == "1"
        auto_sftp = request.form.get("auto_sftp") == "1"
        auto_ftps = request.form.get("auto_ftps") == "1"
        cleanup_local = request.form.get("cleanup_local") == "1"

        if not channel_ref:
            return jsonify({"ok": False, "message": "channel_ref is required"}), 400

        db = AppDatabase(deps["db_path"])
        try:
            db.upsert_channel_preferences(
                channel_ref=channel_ref,
                channel_id=channel_id,
                channel_title=channel_title or channel_ref,
                check_new=check_new,
                auto_download=auto_download,
                auto_sftp=auto_sftp,
                auto_ftps=auto_ftps,
                cleanup_local=cleanup_local,
            )
        finally:
            db.close()

        settings = replace(
            settings,
            cleanup_local_after_sftp=cleanup_local,
            cleanup_local_after_ftps=cleanup_local,
        )
        deps["store_settings"](settings)
        return jsonify({"ok": True, "message": "Настройки канала сохранены."})

    @app.post("/suggest_new_range")
    def suggest_new_range():
        form = deps["form_from_request"]()
        try:
            settings = deps["build_settings"](form, require_channel=False)
        except ValueError as exc:
            return jsonify({"ok": False, "message": str(exc)}), 400
        if not (settings.channel or "").strip():
            return jsonify({"ok": False, "message": "Укажите CHANNEL_ID."}), 200

        preview_cache = deps["get_preview_cache"]()
        if not preview_cache:
            ok_preview, _, items = asyncio.run(deps["fetch_preview"](settings))
            preview_cache = items if ok_preview else []
            deps["set_preview_cache"](preview_cache)

        if not preview_cache:
            return jsonify({"ok": False, "message": "Нет данных предпросмотра. Нажмите Обновить предпросмотр."}), 200

        last_downloaded_id = 0
        db = AppDatabase(deps["db_path"])
        try:
            state = db.get_channel_state_by_ref(settings.channel)
            last_downloaded_id = int(state.get("last_message_id") or 0)
            if last_downloaded_id <= 0:
                channel_id = int(db.get_channel_id_by_ref(settings.channel) or 0)
                if channel_id > 0:
                    last_downloaded_id = int(db.get_last_downloaded_message_id(channel_id) or 0)
        finally:
            db.close()

        if last_downloaded_id <= 0:
            try:
                last_downloaded_id = int(asyncio.run(deps["resolve_last_downloaded_message_id"](settings)) or 0)
            except Exception as exc:
                return jsonify({"ok": False, "message": f"Не удалось вычислить диапазон новых: {exc}"}), 200

        from_index = 0
        to_index = int(len(preview_cache))
        for item in preview_cache:
            if int(item.get("message_id", 0)) > int(last_downloaded_id):
                from_index = int(item.get("index", 0))
                break

        if from_index <= 0:
            return jsonify(
                {
                    "ok": True,
                    "message": "Новых глав не найдено.",
                    "from_index": "",
                    "to_index": "",
                    "has_new": False,
                    "last_downloaded_id": int(last_downloaded_id),
                }
            )

        return jsonify(
            {
                "ok": True,
                "message": "Диапазон новых глав определен.",
                "from_index": str(from_index),
                "to_index": str(to_index),
                "has_new": True,
                "last_downloaded_id": int(last_downloaded_id),
            }
        )
