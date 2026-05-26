-- ===========================================
-- TG Group Analyzer — PostgreSQL Schema
-- ===========================================

-- Основная таблица групп
CREATE TABLE IF NOT EXISTS groups (
    id SERIAL PRIMARY KEY,
    input_link TEXT NOT NULL,              -- оригинальная ссылка из CSV
    username TEXT,                         -- @username (если публичная)
    tg_id BIGINT UNIQUE,                   -- внутренний ID в Telegram
    title TEXT,
    description TEXT,
    members_count INTEGER,
    group_type TEXT,                       -- group / supergroup / channel
    is_public BOOLEAN,
    invite_link TEXT,
    created_at_tg TIMESTAMP,              -- дата создания группы в Telegram
    collected_at TIMESTAMP DEFAULT NOW(),
    status TEXT DEFAULT 'pending',         -- pending / done / error / private / banned
    error_message TEXT                     -- причина ошибки если status = error
);

-- Статистика активности
CREATE TABLE IF NOT EXISTS group_stats (
    id SERIAL PRIMARY KEY,
    group_id INTEGER REFERENCES groups(id) ON DELETE CASCADE,
    messages_total INTEGER,
    messages_last_7d INTEGER,
    messages_last_30d INTEGER,
    avg_views_per_post REAL,
    avg_forwards_per_post REAL,
    avg_replies_per_post REAL,
    unique_authors_30d INTEGER,
    media_share_percent REAL,             -- % сообщений с медиа
    peak_hour INTEGER,                    -- 0–23, самый активный час UTC
    collected_at TIMESTAMP DEFAULT NOW()
);

-- Выборка сообщений (последние N на группу)
CREATE TABLE IF NOT EXISTS messages_sample (
    id SERIAL PRIMARY KEY,
    group_id INTEGER REFERENCES groups(id) ON DELETE CASCADE,
    message_id BIGINT,
    sent_at TIMESTAMP,
    author_id BIGINT,
    text TEXT,
    views INTEGER,
    forwards INTEGER,
    replies INTEGER,
    has_media BOOLEAN,
    media_type TEXT,                      -- photo / video / document / audio / sticker / ...
    UNIQUE(group_id, message_id)
);

-- Топ слов по группе (для тематики)
CREATE TABLE IF NOT EXISTS top_words (
    id SERIAL PRIMARY KEY,
    group_id INTEGER REFERENCES groups(id) ON DELETE CASCADE,
    word TEXT,
    count INTEGER,
    collected_at TIMESTAMP DEFAULT NOW()
);

-- Индексы для быстрых запросов
CREATE INDEX IF NOT EXISTS idx_groups_status ON groups(status);
CREATE INDEX IF NOT EXISTS idx_groups_tg_id ON groups(tg_id);
CREATE INDEX IF NOT EXISTS idx_messages_group ON messages_sample(group_id);
CREATE INDEX IF NOT EXISTS idx_messages_sent_at ON messages_sample(sent_at);
CREATE INDEX IF NOT EXISTS idx_top_words_group ON top_words(group_id);
