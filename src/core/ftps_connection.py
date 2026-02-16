import asyncio
import socket
import ssl
import threading
from concurrent.futures import Future, TimeoutError as FutureTimeoutError

import aioftp


def start_loop(sync) -> None:
    if sync._loop and sync._loop_thread and sync._loop_thread.is_alive():
        return

    loop = asyncio.new_event_loop()

    def _runner() -> None:
        asyncio.set_event_loop(loop)
        loop.run_forever()

    thread = threading.Thread(target=_runner, name="ftps-aioftp-loop", daemon=True)
    thread.start()
    sync._loop = loop
    sync._loop_thread = thread


def stop_loop(sync) -> None:
    if not sync._loop:
        return
    try:
        sync._loop.call_soon_threadsafe(sync._loop.stop)
    except Exception:
        pass
    if sync._loop_thread:
        sync._loop_thread.join(timeout=2)
    sync._loop = None
    sync._loop_thread = None


def run_coro(sync, coro, timeout: int) -> object:
    if not sync._loop:
        raise RuntimeError("FTPS loop is not started.")
    fut: Future = asyncio.run_coroutine_threadsafe(coro, sync._loop)
    try:
        return fut.result(timeout=timeout)
    except FutureTimeoutError as exc:
        fut.cancel()
        raise TimeoutError(f"FTPS operation timed out after {timeout}s") from exc


def build_ssl_context(sync) -> ssl.SSLContext:
    if sync.settings.ftps_verify_tls:
        return ssl.create_default_context()
    return ssl._create_unverified_context()


def connect(sync) -> None:
    if not sync.enabled:
        return

    if not sync.settings.ftps_host or not sync.settings.ftps_username:
        raise ValueError("FTPS включен, но не заполнены host/username.")
    if sync.settings.ftps_port <= 0 or sync.settings.ftps_port > 65535:
        raise ValueError("FTPS_PORT должен быть в диапазоне 1..65535.")
    sync._passive_forced = False
    sync._effective_passive_mode = bool(sync.settings.ftps_passive_mode)
    if not sync._effective_passive_mode:
        sync._effective_passive_mode = True
        sync._passive_forced = True
    if sync.settings.ftps_security_mode not in {"explicit", "implicit"}:
        raise ValueError("FTPS security mode должен быть explicit или implicit.")

    start_loop(sync)
    sync._run(connect_async(sync))


async def connect_async(sync) -> None:
    if sync.settings.ftps_security_mode == "implicit":
        ssl_opt: ssl.SSLContext | bool | None = build_ssl_context(sync)
    else:
        ssl_opt = None

    encoding = (sync.settings.ftps_encoding or "auto").strip().lower()
    if encoding == "auto":
        encoding = "utf-8"

    client = aioftp.Client(
        socket_timeout=sync._timeout_seconds(),
        connection_timeout=sync._timeout_seconds(),
        encoding=encoding,
        ssl=ssl_opt,
    )
    await client.connect(host=sync.settings.ftps_host, port=sync.settings.ftps_port)
    if sync.settings.ftps_security_mode == "explicit":
        await client.upgrade_to_tls(build_ssl_context(sync))
    await client.login(user=sync.settings.ftps_username, password=sync.settings.ftps_password or "")
    sync._client = client


def close(sync) -> None:
    if sync._loop and sync._client is not None:
        try:
            sync._run(close_async(sync), timeout=10)
        except Exception:
            pass
    sync._close_ftplib_client()
    sync._client = None
    stop_loop(sync)


async def close_async(sync) -> None:
    if not sync._client:
        return
    try:
        await sync._client.quit()
    except Exception:
        try:
            await sync._client.close()
        except Exception:
            pass


def validate_connection(sync) -> tuple[bool, str]:
    if not sync.enabled:
        return True, "FTPS выключен."
    try:
        connect(sync)
        base_remote = sync.ensure_remote_dir((sync.settings.ftps_remote_dir or "/").strip() or "/")
        tls_mode = "verify=on" if sync.settings.ftps_verify_tls else "verify=off"
        transfer_mode = "passive(auto)" if sync._passive_forced else "passive"
        secure_mode = sync.settings.ftps_security_mode
        encoding = (sync.settings.ftps_encoding or "auto").strip().lower()
        if encoding == "auto":
            encoding = "utf-8"
        forced_note = (
            " Active mode недоступен в текущем FTPS клиенте, режим был автоматически переключен на passive."
            if sync._passive_forced
            else ""
        )
        return (
            True,
            "FTPS: подключение установлено "
            f"({tls_mode}; mode={transfer_mode}; security={secure_mode}; encoding={encoding}; "
            f"hash_verify={'on' if sync.settings.ftps_verify_hash else 'off'}). "
            f"Каталог доступен: {base_remote}.{forced_note}",
        )
    except ssl.SSLCertVerificationError as exc:
        return (
            False,
            "FTPS: ошибка TLS-сертификата. "
            "Проверьте цепочку сертификатов на сервере или отключите проверку TLS в настройках FTPS. "
            f"({exc})",
        )
    except (TimeoutError, socket.timeout) as exc:
        return (
            False,
            "FTPS: таймаут чтения/ответа сервера. "
            "Проверьте доступность хоста/порта, настройки firewall/NAT и режим FTPS на сервере. "
            f"(timeout={sync._timeout_seconds()}s; {exc})",
        )
    except Exception as exc:
        return False, f"FTPS: ошибка подключения ({exc})"
    finally:
        close(sync)
