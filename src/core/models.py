from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    api_id: int
    api_hash: str
    phone: str
    channel: str
    download_dir: Path
    session_name: str
    startup_scan_limit: int
    download_concurrency: int
    use_mtproxy: bool
    mtproxy_link: str
    use_sftp: bool
    sftp_host: str
    sftp_port: int
    sftp_username: str
    sftp_password: str
    sftp_remote_dir: str
    cleanup_local_after_sftp: bool
    use_ftps: bool
    ftps_host: str
    ftps_port: int
    ftps_username: str
    ftps_password: str
    ftps_remote_dir: str
    ftps_encoding: str
    ftps_verify_tls: bool
    ftps_passive_mode: bool
    ftps_security_mode: str
    ftps_upload_concurrency: int
    cleanup_local_after_ftps: bool
