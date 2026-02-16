from web.app import (
    OPEN_BROWSER,
    HOST,
    PORT,
    _start_monitor_thread,
    app,
)


if __name__ == "__main__":
    import threading
    import webbrowser

    _start_monitor_thread()
    if OPEN_BROWSER:
        threading.Timer(1.0, lambda: webbrowser.open(f"http://{HOST}:{PORT}")).start()
    app.run(host=HOST, port=PORT, debug=False, use_reloader=False)
