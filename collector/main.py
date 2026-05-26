"""
collector/main.py — сбор данных по списку Telegram-групп (Telethon)

Запуск:
    python collector/main.py

Переменные окружения (из .env):
    TG_API_ID, TG_API_HASH, TG_STRING_SESSION
    DATABASE_URL
    GROUPS_CSV             (по умолчанию ./data/groups.csv)
    MESSAGES_LIMIT         (по умолчанию 500)
    DELAY_BETWEEN_GROUPS   (по умолчанию 3 секунды)
"""

import asyncio
import os
import csv
import re
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

import asyncpg
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import (
    FloodWaitError, ChannelPrivateError, UsernameNotOccupiedError,
    UsernameInvalidError, InviteHashExpiredError, InviteHashInvalidError,
)
from telethon.tl.types import Channel, Chat as TLChat, MessageMediaPhoto, MessageMediaDocument

from db.database import get_pool, init_schema

load_dotenv()

# ── Настройки ──────────────────────────────────────────────────────────────
API_ID          = int(os.environ["TG_API_ID"])
API_HASH        = os.environ["TG_API_HASH"]
STRING_SESSION  = os.environ["TG_STRING_SESSION"]
GROUPS_CSV      = os.getenv("GROUPS_CSV", "./data/groups.csv")
MESSAGES_LIMIT  = int(os.getenv("MESSAGES_LIMIT", "500"))
DELAY           = float(os.getenv("DELAY_BETWEEN_GROUPS", "3"))

STOPWORDS = {
    "и", "в", "на", "с", "по", "для", "из", "это", "что", "как", "или",
    "не", "но", "а", "то", "же", "бы", "к", "о", "от", "до", "за",
    "the", "a", "an", "is", "in", "of", "to", "and", "for", "at", "be",
    "this", "that", "it", "was", "are", "with", "i", "you", "he", "she",
}


# ── Вспомогательные функции ────────────────────────────────────────────────

def load_links(csv_path: str) -> list[str]:
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV не найден: {csv_path}")
    links = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            link = (
                row.get("link") or row.get("url") or
                row.get("username") or list(row.values())[0]
            ).strip()
            if link:
                links.append(link)
    return links


def extract_username_or_invite(link: str):
    link = link.strip()
    if link.startswith("@"):
        return link[1:], None
    m = re.search(r"t\.me/\+([A-Za-z0-9_-]+)", link)
    if m:
        return None, m.group(1)
    m = re.search(r"t\.me/joinchat/([A-Za-z0-9_-]+)", link)
    if m:
        return None, m.group(1)
    m = re.search(r"t\.me/([A-Za-z0-9_]+)", link)
    if m:
        return m.group(1), None
    if re.match(r"^[A-Za-z0-9_]{3,}$", link):
        return link, None
    return None, None


def get_media_type(msg) -> str | None:
    if msg.media is None:
        return None
    name = type(msg.media).__name__
    mapping = {
        "MessageMediaPhoto": "photo",
        "MessageMediaDocument": "document",
        "MessageMediaWebPage": "webpage",
        "MessageMediaPoll": "poll",
        "MessageMediaGeo": "geo",
        "MessageMediaContact": "contact",
    }
    return mapping.get(name, name.lower().replace("messagemedia", ""))


def extract_top_words(messages, top_n=30):
    words = []
    for msg in messages:
        text = getattr(msg, "text", None) or getattr(msg, "message", None)
        if not text:
            continue
        tokens = re.findall(r"[а-яёa-z]{3,}", text.lower())
        words.extend(t for t in tokens if t not in STOPWORDS)
    return Counter(words).most_common(top_n)


def to_utc(dt):
    """Приводит naive datetime к UTC-aware."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def peak_hour(messages) -> int | None:
    hours = [m.date.hour for m in messages if m.date]
    return Counter(hours).most_common(1)[0][0] if hours else None


# ── Основная логика сбора ──────────────────────────────────────────────────

async def collect_group(client: TelegramClient, pool: asyncpg.Pool, input_link: str):
    print(f"\n🔍 {input_link}")
    username, invite_hash = extract_username_or_invite(input_link)

    async with pool.acquire() as conn:
        group_row = await conn.fetchrow(
            "SELECT id, status FROM groups WHERE input_link = $1", input_link
        )
        if group_row and group_row["status"] == "done":
            print("   ⏭  уже собрано, пропускаем")
            return
        if not group_row:
            group_db_id = await conn.fetchval(
                "INSERT INTO groups(input_link, status) VALUES($1, 'pending') RETURNING id",
                input_link,
            )
        else:
            group_db_id = group_row["id"]

    try:
        # Получаем entity
        if username:
            entity = await client.get_entity(username)
        elif invite_hash:
            entity = await client.get_entity(f"https://t.me/joinchat/{invite_hash}")
        else:
            raise ValueError("Не удалось распознать ссылку")

        # Полная информация
        full = await client.get_entity(entity)
        is_channel = isinstance(full, Channel)

        title       = getattr(full, "title", None)
        uname       = getattr(full, "username", None)
        tg_id       = full.id
        members     = getattr(full, "participants_count", None)
        is_public   = bool(uname)
        created_at  = getattr(full, "date", None)

        if is_channel:
            group_type = "channel" if getattr(full, "broadcast", False) else "supergroup"
        else:
            group_type = "group"

        # Описание
        try:
            from telethon.tl.functions.channels import GetFullChannelRequest
            from telethon.tl.functions.messages import GetFullChatRequest
            if is_channel:
                full_info = await client(GetFullChannelRequest(full))
                description = full_info.full_chat.about
                members = full_info.full_chat.participants_count
            else:
                full_info = await client(GetFullChatRequest(full.id))
                description = full_info.full_chat.about
        except Exception:
            description = None

        # Собираем сообщения
        now = datetime.now(timezone.utc)
        week_ago  = now - timedelta(days=7)
        month_ago = now - timedelta(days=30)

        messages = []
        async for msg in client.iter_messages(entity, limit=MESSAGES_LIMIT):
            messages.append(msg)

        msg_7d  = sum(1 for m in messages if m.date and m.date.replace(tzinfo=timezone.utc) >= week_ago)
        msg_30d = sum(1 for m in messages if m.date and m.date.replace(tzinfo=timezone.utc) >= month_ago)

        views_list    = [m.views    or 0 for m in messages if m.views    is not None]
        forwards_list = [m.forwards or 0 for m in messages if m.forwards is not None]
        replies_list  = [m.replies.replies if m.replies else 0 for m in messages]

        avg_views    = sum(views_list)    / len(views_list)    if views_list    else 0
        avg_forwards = sum(forwards_list) / len(forwards_list) if forwards_list else 0
        avg_replies  = sum(replies_list)  / len(replies_list)  if replies_list  else 0

        media_msgs     = [m for m in messages if m.media]
        media_share    = len(media_msgs) / len(messages) * 100 if messages else 0
        unique_authors = len({
            m.sender_id for m in messages
            if m.sender_id and m.date and m.date.replace(tzinfo=timezone.utc) >= month_ago
        })
        ph    = peak_hour(messages)
        top_w = extract_top_words(messages)

        async with pool.acquire() as conn:
            await conn.execute("""
                UPDATE groups SET
                    username      = $1, tg_id        = $2, title        = $3,
                    description   = $4, members_count= $5, group_type   = $6,
                    is_public     = $7, created_at_tg= $8,
                    collected_at  = NOW(), status     = 'done', error_message = NULL
                WHERE id = $9
            """, uname, tg_id, title, description, members,
                group_type, is_public, to_utc(created_at), group_db_id)

            await conn.execute("""
                INSERT INTO group_stats
                    (group_id, messages_total, messages_last_7d, messages_last_30d,
                     avg_views_per_post, avg_forwards_per_post, avg_replies_per_post,
                     unique_authors_30d, media_share_percent, peak_hour)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
            """, group_db_id, len(messages), msg_7d, msg_30d,
                avg_views, avg_forwards, avg_replies,
                unique_authors, media_share, ph)

            for msg in messages:
                text = getattr(msg, "text", None) or getattr(msg, "message", None)
                media_type = get_media_type(msg)
                await conn.execute("""
                    INSERT INTO messages_sample
                        (group_id, message_id, sent_at, author_id, text,
                         views, forwards, replies, has_media, media_type)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                    ON CONFLICT (group_id, message_id) DO NOTHING
                """, group_db_id, msg.id,
                    to_utc(msg.date),
                    msg.sender_id, text, msg.views, msg.forwards,
                    msg.replies.replies if msg.replies else None,
                    bool(msg.media), media_type)

            await conn.execute("DELETE FROM top_words WHERE group_id = $1", group_db_id)
            for word, count in top_w:
                await conn.execute(
                    "INSERT INTO top_words(group_id, word, count) VALUES($1,$2,$3)",
                    group_db_id, word, count)

        print(f"   ✅ {title} | {members} участников | {len(messages)} сообщений")

    except FloodWaitError as e:
        print(f"   ⏳ FloodWait {e.seconds}s — ждём...")
        await asyncio.sleep(e.seconds + 5)
        await collect_group(client, pool, input_link)

    except (ChannelPrivateError, UsernameNotOccupiedError,
            UsernameInvalidError, InviteHashExpiredError, InviteHashInvalidError) as e:
        status = "private" if isinstance(e, ChannelPrivateError) else "error"
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE groups SET status=$1, error_message=$2 WHERE id=$3",
                status, str(e), group_db_id)
        print(f"   ⚠️  {type(e).__name__}")

    except Exception as e:
        import traceback
        traceback.print_exc()
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE groups SET status='error', error_message=$1 WHERE id=$2",
                str(e), group_db_id)
        print(f"   ❌ Ошибка: {e}")


# ── Точка входа ────────────────────────────────────────────────────────────

async def main():
    links = load_links(GROUPS_CSV)
    print(f"📋 Загружено {len(links)} ссылок")

    pool = await get_pool()
    async with pool.acquire() as conn:
        await init_schema(conn)

    client = TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH)
    await client.start()
    print("✅ Telegram подключён")

    try:
        for i, link in enumerate(links, 1):
            print(f"[{i}/{len(links)}]", end=" ")
            await collect_group(client, pool, link)
            await asyncio.sleep(DELAY)
    finally:
        await client.disconnect()
        await pool.close()

    print("\n🎉 Готово!")


if __name__ == "__main__":
    asyncio.run(main())
