from typing import Optional

from telethon import TelegramClient, connection, utils
from telethon.tl.custom.message import Message

from core.config import parse_mtproxy_link
from core.models import Settings


def create_telegram_client(settings: Settings) -> TelegramClient:
    if settings.use_mtproxy:
        host, port, secret = parse_mtproxy_link(settings.mtproxy_link)
        return TelegramClient(
            settings.session_name,
            settings.api_id,
            settings.api_hash,
            connection=connection.ConnectionTcpMTProxyRandomizedIntermediate,
            proxy=(host, port, secret),
        )

    return TelegramClient(settings.session_name, settings.api_id, settings.api_hash)


async def resolve_channel_entity(client: TelegramClient, channel_ref: str):
    raw = channel_ref.strip()
    if not raw:
        raise ValueError("CHANNEL_ID is empty.")

    try:
        return await client.get_entity(raw)
    except Exception:
        pass

    numeric_id: Optional[int] = None
    try:
        numeric_id = int(raw)
    except ValueError:
        numeric_id = None

    if numeric_id is not None:
        try:
            return await client.get_entity(numeric_id)
        except Exception:
            pass

        try:
            real_id, peer_type = utils.resolve_id(numeric_id)
            return await client.get_entity(peer_type(real_id))
        except Exception:
            pass

        async for dialog in client.iter_dialogs():
            try:
                if utils.get_peer_id(dialog.entity) == numeric_id:
                    return dialog.entity
            except Exception:
                continue

    raise ValueError(
        f"Cannot resolve channel '{channel_ref}'. Try @username or ensure this chat is present in account dialogs."
    )


def is_audio_message(message: Message) -> bool:
    if message.audio:
        return True

    if message.document and message.document.mime_type:
        return message.document.mime_type.startswith("audio/")

    return False
