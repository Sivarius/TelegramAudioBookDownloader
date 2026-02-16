from flask import request

from web_action_service import (
    handle_authorize,
    handle_start_download,
    handle_start_upload,
    handle_stop_download,
    handle_stop_server,
)


def register_action_routes(app, deps: dict) -> None:
    @app.post("/authorize")
    def authorize():
        form = deps["form_from_request"]()
        return handle_authorize(deps, form)

    @app.post("/start")
    def start_download():
        form = deps["form_from_request"]()
        return handle_start_download(deps, form)

    @app.post("/start_upload")
    def start_upload():
        form = deps["form_from_request"]()
        return handle_start_upload(deps, form)

    @app.post("/stop_server")
    def stop_server():
        shutdown_func = request.environ.get("werkzeug.server.shutdown")
        return handle_stop_server(deps, shutdown_func)

    @app.post("/stop_download")
    def stop_download():
        form = deps["form_from_request"]()
        return handle_stop_download(deps, form)
