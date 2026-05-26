"""
db/database.py — подключение к PostgreSQL и инициализация схемы
"""

import os
import asyncpg
from pathlib import Path


async def get_connection() -> asyncpg.Connection:
    """Создаёт одиночное подключение к БД."""
    return await asyncpg.connect(os.environ["DATABASE_URL"])


async def get_pool() -> asyncpg.Pool:
    """Создаёт пул подключений (для параллельной работы)."""
    return await asyncpg.create_pool(
        os.environ["DATABASE_URL"],
        min_size=1,
        max_size=5,
    )


async def init_schema(conn: asyncpg.Connection):
    """Применяет schema.sql если таблицы ещё не созданы."""
    schema_path = Path(__file__).parent / "schema.sql"
    sql = schema_path.read_text(encoding="utf-8")
    await conn.execute(sql)
    print("✅ Schema applied")
