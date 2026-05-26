"""
collector/main.py — сбор данных по списку Telegram-групп

Запуск:
    python collector/main.py

Переменные окружения (из .env):
    TG_API_ID, TG_API_HASH, TG_STRING_SESSION
    DATABASE_URL
    GROUPS_CSV         (по умолчанию ./data/groups.csv)
    MESSAGES_LIMIT     (по умолчанию 500)
    DELAY_BETWEEN_GROUPS (по умолчанию 3 секунды)
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
from pyrogram import Client
from pyrogram.errors import (
    FloodWait, ChannelPrivate, UsernameNotOccupied,
    UsernameInvalid, InviteHashExpired, ChatAdminRequired,
)
from pyrogram.types import Chat, Message

from db.database import get_pool, init_schema

load_dotenv()

# ── Настройки ──────────────────────────────────────────────────────────────
API_ID           = int(os.environ["TG_API_ID"])
API_HASH         = os.environ["TG_API_HASH"]
STRING_SESSION   = os.environ["TG_STRING_SESSION"]
GROUPS_CSV       = os.getenv("GROUPS_CSV", "./data/groups.csv")
MESSAGES_LIMIT   = int(os.getenv("MESSAGES_LIMIT", "500"))
DELAY            = float(os.getenv("DELAY_BETWEEN_GROUPS", "3"))

# Стоп-слова для top_words (русский + английский минимум)
STOPWORDS = {
    "и", "в", "на", "с", "по", "для", "из", "это", "что", "как", "или",
    "не", "но", "а", "то", "же", "бы", "к", "о", "от", "до", "за",
    "the", "a", "an", "is", "in", "of", "to", "and", "for", "at", "be",
    "this", "that", "it", "was", "are", "with", "i", "you", "he", "she",
}


# ── Вспомогательные функции ────────────────────────────────────────────────

def load_links(csv_path: str) -> list[str]:
    """Читает список ссылок из CSV."""
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV не найден: {csv_path}")

    links = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # поддержка колонок: link / url / username / группа
            link = (
                row.get("link") or row.get("url") or
                row.get("username") or list(row.values())[0]
            ).strip()
            if link:
                links.append(link)
    return links


def extract_username_or_invite(link: str) -> tuple[str | None, str | None]:
    """
    Возвращает (username, invite_hash) из ссылки.
    Примеры входа:
        https://t.me/durov          → ("durov", None)
        t.me/+AbCdEf               → (None, "AbCdEf")
        @channel_name              → ("channel_name", None)
        https://t.me/joinchat/xxx  → (None, "xxx")
    """
    link = link.strip()

    # @username
    if link.startswith("@"):
        return link[1:], None

    # invite: t.me/+ или t.me/joinchat/
    m = re.search(r"t\.me/\+([A-Za-z0-9_-]+)", link)
    if m:
        return None, m.group(1)

    m = re.search(r"t\.me/joinchat/([A-Za-z0-9_-]+)", link)
    if m:
        return None, m.group(1)

    # обычный username
    m = re.search(r"t\.me/([A-Za-z0-9_]+)", link)
    if m:
        return m.group(1), None

    # просто строка без схемы — считаем username
    if re.match(r"^[A-Za-z0-9_]{3,}$", link):
        return link, None

    return None, None


def extract_top_words(messages: list[Message], top_n: int = 30) -> list[tuple[str, int]]:
    """Извлекает топ-N слов из текстов сообщений."""
    words: list[str] = []
    for msg in messages:
        if not msg.text:
            continue
        tokens = re.findall(r"[а-яёa-z]{3,}", msg.text.lower())
        words.extend(t for t in tokens if t not in STOPWORDS)
    return Counter(words).most_common(top_n)


def peak_hour(messages: list[Message]) -> int | None:
    """Определяет самый активный час (UTC) по выборке."""
    if not messages:
        return None
    hours = [m.date.hour for m in messages if m.date]
    return Counter(hours).most_common(1)[0][0] if hours else None


# ── Основная логика сбора ──────────────────────────────────────────────────

async def collect_group(
    app: Client,
    pool: asyncpg.Pool,
    input_link: str,
):
    print(f"\n🔍 {input_link}")
    username, invite_hash = extract_username_or_invite(input_link)

    async with pool.acquire() as conn:
        # Вставляем запись (или берём существующую)
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
        # Получаем объект чата
        if username:
            chat: Chat = await app.get_chat(username)
        elif invite_hash:
            chat: Chat = await app.get_chat(f"+{invite_hash}")
        else:
            raise ValueError("Не удалось распознать ссылку")

        is_public = bool(chat.username)
        now = datetime.now(timezone.utc)
        week_ago  = now - timedelta(days=7)
        month_ago = now - timedelta(days=30)

        # Собираем историю сообщений
        messages: list[Message] = []
        async for msg in app.get_chat_history(chat.id, limit=MESSAGES_LIMIT):
            messages.append(msg)

        # Считаем метрики
        msg_7d  = sum(1 for m in messages if m.date and m.date >= week_ago)
        msg_30d = sum(1 for m in messages if m.date and m.date >= month_ago)

        views_list    = [m.views    or 0 for m in messages if m.views    is not None]
        forwards_list = [m.forwards or 0 for m in messages if m.forwards is not None]
        replies_list  = [m.replies.replies if m.replies else 0 for m in messages]

        avg_views    = sum(views_list)    / len(views_list)    if views_list    else 0
        avg_forwards = sum(forwards_list) / len(forwards_list) if forwards_list else 0
        avg_replies  = sum(replies_list)  / len(replies_list)  if replies_list  else 0

        media_msgs       = [m for m in messages if m.media]
        media_share      = len(media_msgs) / len(messages) * 100 if messages else 0
        unique_authors   = len({m.from_user.id for m in messages if m.from_user and m.date >= month_ago})
        ph               = peak_hour(messages)
        top_w            = extract_top_words(messages)

        async with pool.acquire() as conn:
            # Обновляем основную запись группы
            await conn.execute("""
                UPDATE groups SET
                    username        = $1,
                    tg_id           = $2,
                    title           = $3,
                    description     = $4,
                    members_count   = $5,
                    group_type      = $6,
                    is_public       = $7,
                    invite_link     = $8,
                    created_at_tg   = $9,
                    collected_at    = NOW(),
                    status          = 'done',
                    error_message   = NULL
                WHERE id = $10
            """,
                chat.username,
                chat.id,
                chat.title,
                chat.description,
                chat.members_count,
                chat.type.value,
                is_public,
                chat.invite_link,
                chat.date,
                group_db_id,
            )

            # Статистика
            await conn.execute("""
                INSERT INTO group_stats
                    (group_id, messages_total, messages_last_7d, messages_last_30d,
                     avg_views_per_post, avg_forwards_per_post, avg_replies_per_post,
                     unique_authors_30d, media_share_percent, peak_hour)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                ON CONFLICT DO NOTHING
            """,
                group_db_id, len(messages), msg_7d, msg_30d,
                avg_views, avg_forwards, avg_replies,
                unique_authors, media_share, ph,
            )

            # Сохраняем выборку сообщений
            for msg in messages:
                media_type = msg.media.value if msg.media else None
                await conn.execute("""
                    INSERT INTO messages_sample
                        (group_id, message_id, sent_at, author_id, text,
                         views, forwards, replies, has_media, media_type)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                    ON CONFLICT (group_id, message_id) DO NOTHING
                """,
                    group_db_id,
                    msg.id,
                    msg.date,
                    msg.from_user.id if msg.from_user else None,
                    msg.text or msg.caption,
                    msg.views,
                    msg.forwards,
                    msg.replies.replies if msg.replies else None,
                    bool(msg.media),
                    media_type,
                )

            # Топ слова
            await conn.execute("DELETE FROM top_words WHERE group_id = $1", group_db_id)
            for word, count in top_w:
                await conn.execute(
                    "INSERT INTO top_words(group_id, word, count) VALUES($1,$2,$3)",
                    group_db_id, word, count,
                )

        print(f"   ✅ {chat.title} | {chat.members_count} участников | {len(messages)} сообщений")

    except FloodWait as e:
        print(f"   ⏳ FloodWait {e.value}s — ждём...")
        await asyncio.sleep(e.value + 5)
        # повторяем этот же чат
        await collect_group(app, pool, input_link)

    except (ChannelPrivate, UsernameNotOccupied, UsernameInvalid, InviteHashExpired) as e:
        status = "private" if isinstance(e, ChannelPrivate) else "error"
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE groups SET status=$1, error_message=$2 WHERE id=$3",
                status, str(e), group_db_id,
            )
        print(f"   ⚠️  {type(e).__name__}: {e}")

    except Exception as e:
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE groups SET status='error', error_message=$1 WHERE id=$2",
                str(e), group_db_id,
            )
        print(f"   ❌ Ошибка: {e}")


# ── Точка входа ────────────────────────────────────────────────────────────

async def main():
    links = load_links(GROUPS_CSV)
    print(f"📋 Загружено {len(links)} ссылок")

    pool = await get_pool()

    # Инициализируем схему БД
    async with pool.acquire() as conn:
        await init_schema(conn)

    async with Client(
        name="tg_collector",
        api_id=API_ID,
        api_hash=API_HASH,
        session_string=STRING_SESSION,
    ) as app:
        for i, link in enumerate(links, 1):
            print(f"[{i}/{len(links)}]", end=" ")
            await collect_group(app, pool, link)
            await asyncio.sleep(DELAY)

    await pool.close()
    print("\n🎉 Готово!")


if __name__ == "__main__":
    asyncio.run(main())
