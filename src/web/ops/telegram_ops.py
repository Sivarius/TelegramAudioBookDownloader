from core.db import AppDatabase
from core.models import Settings
from core.telegram_client import (
    create_telegram_client,
    entity_channel_id_variants,
    is_audio_message,
    resolve_channel_entity,
)


async def fetch_preview(
    settings: Settings,
    db_path,
    limit: int = 300,
) -> tuple[bool, str, list[dict]]:
    client = create_telegram_client(settings)
    await client.connect()

    try:
        if not await client.is_user_authorized():
            return False, "Сначала выполните авторизацию.", []

        channel = await resolve_channel_entity(client, settings.channel)
        alt_ids = entity_channel_id_variants(channel)
        db = AppDatabase(db_path)

        items: list[dict] = []
        index = 1
        try:
            async for message in client.iter_messages(channel, limit=limit, reverse=True):
                if not is_audio_message(message):
                    continue

                file_name = ""
                if message.file and getattr(message.file, "name", None):
                    file_name = str(message.file.name)

                title = file_name or (message.message or "audio")
                date_text = message.date.strftime("%Y-%m-%d %H:%M") if message.date else ""
                message_id = int(message.id)
                is_downloaded = any(
                    db.already_downloaded(cid, message_id) for cid in alt_ids
                )
                is_remote_uploaded = any(
                    db.is_remote_uploaded(cid, message_id) for cid in alt_ids
                )
                items.append(
                    {
                        "index": index,
                        "message_id": message.id,
                        "title": title.replace("\n", " ")[:80],
                        "date": date_text,
                        "downloaded": is_downloaded,
                        "remote_uploaded": is_remote_uploaded,
                    }
                )
                index += 1
        finally:
            db.close()

        return True, f"Найдено аудио: {len(items)}", items
    except Exception as exc:
        return False, f"Ошибка предпросмотра: {exc}", []
    finally:
        await client.disconnect()


async def resolve_last_downloaded_message_id(settings: Settings, db_path) -> int:
    client = create_telegram_client(settings)
    await client.connect()
    try:
        channel = await resolve_channel_entity(client, settings.channel)
        alt_ids = entity_channel_id_variants(channel)
        db = AppDatabase(db_path)
        try:
            return max(db.get_last_downloaded_message_id(cid) for cid in alt_ids)
        finally:
            db.close()
    finally:
        await client.disconnect()


async def resolve_effective_last_downloaded_message_id(settings: Settings, db_path) -> int:
    db = AppDatabase(db_path)
    try:
        state = db.get_channel_state_by_ref(settings.channel)
        last_downloaded_id = int(state.get("last_message_id") or 0)
        if last_downloaded_id > 0:
            return last_downloaded_id
        channel_id = int(db.get_channel_id_by_ref(settings.channel) or 0)
        if channel_id > 0:
            last_downloaded_id = int(db.get_last_downloaded_message_id(channel_id) or 0)
            if last_downloaded_id > 0:
                return last_downloaded_id
    finally:
        db.close()
    return int(await resolve_last_downloaded_message_id(settings, db_path))


def pick_range_ids(items: list[dict], from_index_raw: str, to_index_raw: str, safe_int) -> set[int] | None:
    if not from_index_raw and not to_index_raw:
        return None

    if not items:
        return set()

    from_index = safe_int(from_index_raw, 1)
    to_index = safe_int(to_index_raw, len(items))

    if from_index <= 0:
        from_index = 1
    if to_index <= 0:
        to_index = len(items)
    if from_index > to_index:
        from_index, to_index = to_index, from_index

    selected = {int(i["message_id"]) for i in items if from_index <= int(i["index"]) <= to_index}
    return selected
