# Beauty Bot Phase 1 — PostgreSQL

Эта версия заменяет локальную SQLite-базу на PostgreSQL.

## Что меняется

Раньше бот использовал:

```text
DB_PATH=beauty_bot.db
```

Теперь используется:

```text
DATABASE_URL
```

В Railway Postgres уже создаёт эту переменную автоматически.

---

# Как связать Bot и Postgres в Railway

## 1. Проверь, что оба сервиса находятся в одном Railway Project

На Canvas должны быть два блока, например:

```text
Beauty Bot
Postgres
```

## 2. Открой сервис Postgres

Открой:

```text
Postgres → Variables
```

Там Railway создаёт:

```text
DATABASE_URL
PGHOST
PGPORT
PGUSER
PGPASSWORD
PGDATABASE
```

Ничего из этого не нужно копировать вручную.

## 3. Открой сервис самого бота

Открой:

```text
Beauty Bot → Variables
```

Нажми:

```text
New Variable / Add Reference Variable
```

Выбери переменную:

```text
DATABASE_URL
```

из сервиса Postgres.

Если вводишь вручную, значение должно быть:

```text
${{Postgres.DATABASE_URL}}
```

ВАЖНО: `Postgres` здесь — точное имя сервиса на Railway.
Если твой сервис называется иначе, например `PostgreSQL`, используй:

```text
${{PostgreSQL.DATABASE_URL}}
```

## 4. В Variables бота должны остаться

```text
BOT_TOKEN=...
ADMIN_IDS=...
ITEMS_THRESHOLD=2
DATABASE_URL=${{Postgres.DATABASE_URL}}
```

## 5. Что удалить

Для PostgreSQL больше не нужен:

```text
DB_PATH
```

Он этой версией кода не используется.

Railway Volume `/data` также больше не нужен именно для базы данных.

## 6. Замени файлы в GitHub

Замени:

```text
bot.py
stats.py
requirements.txt
.env.example
.gitignore
README.md
```

на файлы из этого комплекта.

После commit Railway должен автоматически сделать новый deployment.

## 7. Что произойдёт при первом запуске

`bot.py` сам выполнит `CREATE TABLE IF NOT EXISTS` и создаст в Postgres:

```text
users
starts
items
events
```

Отдельно руками создавать эти таблицы не нужно.

## 8. Проверка

После deployment в Railway Logs должно быть:

```text
Beauty Bot Phase 1 + PostgreSQL запущен
```

После этого в Telegram:

```text
/start
```

затем:

```text
/stats
```

и:

```text
/export
```

## 9. Проверка данных

После `/start` в PostgreSQL должна появиться строка в `users`,
а также записи в `starts` и `events`.

---

# Важный момент о старой SQLite-базе

Переход на PostgreSQL НЕ переносит автоматически старые записи из:

```text
beauty_bot.db
```

Новый PostgreSQL начнёт со своей базы.

Если старые тестовые пользователи нужны, их надо мигрировать отдельно.
Если это были только пробные тесты до рекламного запуска, обычно проще начать
PostgreSQL с чистой базы.

---

# Почему используется DATABASE_URL

Railway предоставляет Postgres-сервису готовые переменные:

```text
DATABASE_URL
PGHOST
PGPORT
PGUSER
PGPASSWORD
PGDATABASE
```

Для этого бота достаточно одного `DATABASE_URL`.
