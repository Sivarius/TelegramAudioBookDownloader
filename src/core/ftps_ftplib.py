import ftplib


def connect_ftplib_explicit(sync) -> ftplib.FTP_TLS:
    if sync.settings.ftps_security_mode != "explicit":
        raise RuntimeError("ftplib fallback supports explicit FTPS mode only.")
    context = sync._build_ssl_context()
    encoding = (sync.settings.ftps_encoding or "auto").strip().lower()
    if encoding == "auto":
        encoding = "utf-8"
    client = ftplib.FTP_TLS(timeout=sync._timeout_seconds(), context=context, encoding=encoding)
    client.connect(host=sync.settings.ftps_host, port=sync.settings.ftps_port, timeout=sync._timeout_seconds())
    client.auth()
    client.login(user=sync.settings.ftps_username, passwd=sync.settings.ftps_password or "")
    client.prot_p()
    client.set_pasv(True)
    return client


def close_ftplib_client(sync) -> None:
    if not sync._ftplib_client:
        return
    try:
        sync._ftplib_client.quit()
    except Exception:
        try:
            sync._ftplib_client.close()
        except Exception:
            pass
    sync._ftplib_client = None


def get_ftplib_client(sync) -> ftplib.FTP_TLS:
    if sync._ftplib_client:
        try:
            sync._ftplib_client.voidcmd("NOOP")
            return sync._ftplib_client
        except Exception:
            close_ftplib_client(sync)
    sync._ftplib_client = connect_ftplib_explicit(sync)
    return sync._ftplib_client
