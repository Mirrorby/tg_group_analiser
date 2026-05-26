# TG Group Analyzer

Разовый сбор статистики по списку Telegram-групп.  
Данные сохраняются в PostgreSQL (Railway).

## Структура репо

```
tg-group-analyzer/
├── .github/workflows/collect.yml   # GitHub Actions (запуск вручную)
├── data/
│   ├── groups.csv                  # ← сюда вставляешь свои ссылки
│   └── results/                    # экспорты (gitignore)
├── db/
│   ├── schema.sql                  # схема PostgreSQL
│   └── database.py                 # хелпер подключения
├── collector/
│   └── main.py                     # основной скрипт сбора
├── .env.example                    # шаблон переменных окружения
├── requirements.txt
└── README.md
```

## Что собирается

| Данные | Таблица |
|--------|---------|
| Название, описание, тип, кол-во участников | `groups` |
| Сообщений за 7/30 дней, охваты, пересылки | `group_stats` |
| Выборка последних N сообщений | `messages_sample` |
| Топ слов по тематике | `top_words` |

## Быстрый старт

### 1. Клонировать и настроить

```bash
git clone https://github.com/YOUR_USERNAME/tg-group-analyzer
cd tg-group-analyzer
cp .env.example .env
# заполнить .env своими данными
```

### 2. Установить зависимости

```bash
pip install -r requirements.txt
```

### 3. Заполнить список групп

Отредактировать `data/groups.csv` — по одной ссылке на строку:

```csv
link
https://t.me/example_channel
@another_group
https://t.me/+InviteHash
```

### 4. Запустить

```bash
python collector/main.py
```

Или через GitHub Actions: `Actions → Collect TG Stats → Run workflow`

## Переменные окружения

| Переменная | Описание |
|------------|----------|
| `TG_API_ID` | С https://my.telegram.org |
| `TG_API_HASH` | С https://my.telegram.org |
| `TG_STRING_SESSION` | String Session от аккаунта |
| `DATABASE_URL` | PostgreSQL URL из Railway |
| `MESSAGES_LIMIT` | Кол-во сообщений на группу (default: 500) |
| `DELAY_BETWEEN_GROUPS` | Пауза между группами в секундах (default: 3) |

## Статусы групп в БД

| Статус | Значение |
|--------|----------|
| `pending` | Ещё не обработана |
| `done` | Успешно собрана |
| `private` | Приватная, нет доступа |
| `error` | Ошибка (см. `error_message`) |
