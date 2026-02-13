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
