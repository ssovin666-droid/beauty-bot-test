"""
Beauty Bot — Phase 1.

Цель этой версии:
- зарегистрировать пользователя при /start;
- сохранить Telegram-профиль и источник перехода;
- сохранить каждое повторное открытие по deep-link;
- дать разделы «Полка» и «Вишлист»;
- фиксировать ключевые события воронки;
- считать активацию;
- дать администратору /stats и /export.

Реальный мониторинг цен и отправка уведомлений о скидках в Phase 1 не реализованы.
"""

import asyncio
import csv
import json
import logging
import os
import re
import sqlite3
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
DB_PATH = os.getenv("DB_PATH", "beauty_bot.db").strip()
ITEMS_THRESHOLD = int(os.getenv("ITEMS_THRESHOLD", "2"))

ADMIN_IDS = {
    int(value.strip())
    for value in os.getenv("ADMIN_IDS", "").split(",")
    if value.strip().isdigit()
}

router = Router()

SOURCE_RE = re.compile(r"[^A-Za-z0-9_-]+")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_source(raw: str | None) -> str:
    """Deep-link Telegram допускает A-Z, a-z, 0-9, _ и -, до 64 символов."""
    if not raw:
        return "direct"
    value = SOURCE_RE.sub("_", raw.strip())[:64]
    return value or "direct"


def connect_db() -> sqlite3.Connection:
    path = Path(DB_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db() -> None:
    conn = connect_db()
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            language_code TEXT,
            is_premium INTEGER DEFAULT 0,

            first_source TEXT NOT NULL DEFAULT 'direct',
            last_source TEXT NOT NULL DEFAULT 'direct',

            started_at TEXT NOT NULL,
            last_started_at TEXT NOT NULL,

            notifications INTEGER NOT NULL DEFAULT 0,
            activated_at TEXT
        );

        CREATE TABLE IF NOT EXISTS starts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            source TEXT NOT NULL,
            started_at TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            section TEXT NOT NULL CHECK(section IN ('polka', 'wishlist')),
            name TEXT NOT NULL,
            normalized_name TEXT NOT NULL,
            added_at TEXT NOT NULL,
            UNIQUE(user_id, section, normalized_name),
            FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            event_name TEXT NOT NULL,
            event_data TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_starts_user_id
            ON starts(user_id);

        CREATE INDEX IF NOT EXISTS idx_starts_source
            ON starts(source);

        CREATE INDEX IF NOT EXISTS idx_items_user_section
            ON items(user_id, section);

        CREATE INDEX IF NOT EXISTS idx_events_user_id
            ON events(user_id);

        CREATE INDEX IF NOT EXISTS idx_events_name
            ON events(event_name);
        """
    )
    conn.commit()
    conn.close()


def log_event(user_id: int, event_name: str, data: dict | None = None) -> None:
    conn = connect_db()
    conn.execute(
        """
        INSERT INTO events (user_id, event_name, event_data, created_at)
        VALUES (?, ?, ?, ?)
        """,
        (
            user_id,
            event_name,
            json.dumps(data, ensure_ascii=False) if data else None,
            now_iso(),
        ),
    )
    conn.commit()
    conn.close()


def register_start(message: Message, source: str) -> None:
    user = message.from_user
    if user is None:
        return

    timestamp = now_iso()
    conn = connect_db()

    existing = conn.execute(
        "SELECT user_id FROM users WHERE user_id = ?",
        (user.id,),
    ).fetchone()

    if existing is None:
        conn.execute(
            """
            INSERT INTO users (
                user_id, username, first_name, last_name, language_code,
                is_premium, first_source, last_source,
                started_at, last_started_at, notifications
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
            """,
            (
                user.id,
                user.username,
                user.first_name,
                user.last_name,
                user.language_code,
                int(bool(getattr(user, "is_premium", False))),
                source,
                source,
                timestamp,
                timestamp,
            ),
        )
    else:
        conn.execute(
            """
            UPDATE users
            SET username = ?,
                first_name = ?,
                last_name = ?,
                language_code = ?,
                is_premium = ?,
                last_source = ?,
                last_started_at = ?
            WHERE user_id = ?
            """,
            (
                user.username,
                user.first_name,
                user.last_name,
                user.language_code,
                int(bool(getattr(user, "is_premium", False))),
                source,
                timestamp,
                user.id,
            ),
        )

    conn.execute(
        """
        INSERT INTO starts (user_id, source, started_at)
        VALUES (?, ?, ?)
        """,
        (user.id, source, timestamp),
    )

    conn.execute(
        """
        INSERT INTO events (user_id, event_name, event_data, created_at)
        VALUES (?, 'start', ?, ?)
        """,
        (
            user.id,
            json.dumps({"source": source}, ensure_ascii=False),
            timestamp,
        ),
    )

    conn.commit()
    conn.close()


def ensure_user_exists(message: Message) -> None:
    """Страховка для старых сообщений/кнопок после обновления бота."""
    user = message.from_user
    if user is None:
        return

    conn = connect_db()
    row = conn.execute(
        "SELECT user_id FROM users WHERE user_id = ?",
        (user.id,),
    ).fetchone()
    conn.close()

    if row is None:
        register_start(message, "direct")


def normalize_item_name(name: str) -> str:
    return " ".join(name.casefold().split())


def add_item(user_id: int, section: str, name: str) -> bool:
    """Возвращает True, если товар добавлен, False — если это дубликат."""
    cleaned = " ".join(name.split())
    normalized = normalize_item_name(cleaned)

    conn = connect_db()
    try:
        conn.execute(
            """
            INSERT INTO items (user_id, section, name, normalized_name, added_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (user_id, section, cleaned, normalized, now_iso()),
        )
        conn.commit()
        added = True
    except sqlite3.IntegrityError:
        added = False
    finally:
        conn.close()

    if added:
        log_event(
            user_id,
            "item_added",
            {"section": section, "name": cleaned},
        )
        maybe_activate(user_id)

    return added


def set_notifications(user_id: int, value: bool) -> None:
    conn = connect_db()
    conn.execute(
        "UPDATE users SET notifications = ? WHERE user_id = ?",
        (int(value), user_id),
    )
    conn.commit()
    conn.close()

    log_event(
        user_id,
        "notifications_on" if value else "notifications_off",
    )

    if value:
        maybe_activate(user_id)


def maybe_activate(user_id: int) -> bool:
    """
    Активация = минимум ITEMS_THRESHOLD уникальных товаров
    + включены уведомления.
    """
    conn = connect_db()

    items_count = conn.execute(
        """
        SELECT COUNT(DISTINCT normalized_name)
        FROM items
        WHERE user_id = ?
        """,
        (user_id,),
    ).fetchone()[0]

    user = conn.execute(
        """
        SELECT notifications, activated_at
        FROM users
        WHERE user_id = ?
        """,
        (user_id,),
    ).fetchone()

    activated_now = False

    if (
        user is not None
        and user["activated_at"] is None
        and user["notifications"] == 1
        and items_count >= ITEMS_THRESHOLD
    ):
        timestamp = now_iso()
        conn.execute(
            "UPDATE users SET activated_at = ? WHERE user_id = ?",
            (timestamp, user_id),
        )
        conn.execute(
            """
            INSERT INTO events (user_id, event_name, event_data, created_at)
            VALUES (?, 'activated', ?, ?)
            """,
            (
                user_id,
                json.dumps(
                    {"items_count": items_count, "threshold": ITEMS_THRESHOLD},
                    ensure_ascii=False,
                ),
                timestamp,
            ),
        )
        conn.commit()
        activated_now = True

    conn.close()
    return activated_now


def get_items(user_id: int, section: str) -> list[str]:
    conn = connect_db()
    rows = conn.execute(
        """
        SELECT name
        FROM items
        WHERE user_id = ? AND section = ?
        ORDER BY id
        """,
        (user_id, section),
    ).fetchall()
    conn.close()
    return [row["name"] for row in rows]


def get_notifications(user_id: int) -> bool:
    conn = connect_db()
    row = conn.execute(
        "SELECT notifications FROM users WHERE user_id = ?",
        (user_id,),
    ).fetchone()
    conn.close()
    return bool(row and row["notifications"])


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def build_stats_text() -> str:
    conn = connect_db()

    total_users = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    activated = conn.execute(
        "SELECT COUNT(*) FROM users WHERE activated_at IS NOT NULL"
    ).fetchone()[0]
    notifications = conn.execute(
        "SELECT COUNT(*) FROM users WHERE notifications = 1"
    ).fetchone()[0]
    with_items = conn.execute(
        "SELECT COUNT(DISTINCT user_id) FROM items"
    ).fetchone()[0]

    sources = conn.execute(
        """
        SELECT
            u.first_source AS source,
            COUNT(*) AS registrations,
            SUM(CASE WHEN u.activated_at IS NOT NULL THEN 1 ELSE 0 END) AS activated
        FROM users u
        GROUP BY u.first_source
        ORDER BY registrations DESC
        LIMIT 20
        """
    ).fetchall()

    conn.close()

    conv = (activated / total_users * 100) if total_users else 0.0

    lines = [
        "📊 Phase 1 — статистика",
        "",
        f"Регистраций: {total_users}",
        f"Добавили хотя бы 1 товар: {with_items}",
        f"Уведомления включены: {notifications}",
        f"Активированы: {activated}",
        f"Конверсия Start → Activation: {conv:.1f}%",
        "",
        "По первому источнику:",
    ]

    if not sources:
        lines.append("пока нет данных")
    else:
        for row in sources:
            source_conv = (
                row["activated"] / row["registrations"] * 100
                if row["registrations"]
                else 0
            )
            lines.append(
                f"• {row['source']}: {row['registrations']} start / "
                f"{row['activated']} act / {source_conv:.1f}%"
            )

    return "\n".join(lines)


def export_database_to_zip() -> Path:
    """Экспортирует основные таблицы в CSV и собирает их в ZIP."""
    conn = connect_db()
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    temp_dir = Path(tempfile.mkdtemp(prefix="beauty_bot_export_"))
    zip_path = temp_dir / f"beauty_bot_export_{timestamp}.zip"

    tables = ["users", "starts", "items", "events"]

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for table in tables:
            cursor = conn.execute(f"SELECT * FROM {table} ORDER BY 1")
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

            csv_path = temp_dir / f"{table}.csv"
            with csv_path.open("w", newline="", encoding="utf-8-sig") as f:
                writer = csv.writer(f)
                writer.writerow(columns)
                writer.writerows([tuple(row) for row in rows])

            zf.write(csv_path, arcname=csv_path.name)

    conn.close()
    return zip_path


class AddItem(StatesGroup):
    waiting_polka = State()
    waiting_wishlist = State()


def main_menu(user_id: int) -> InlineKeyboardMarkup:
    notif_on = get_notifications(user_id)
    notif_label = (
        "🔔 Уведомления: включены"
        if notif_on
        else "🔕 Включить уведомления"
    )

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📦 Полка", callback_data="menu_polka")],
            [InlineKeyboardButton(text="✨ Вишлист", callback_data="menu_wishlist")],
            [InlineKeyboardButton(text=notif_label, callback_data="toggle_notify")],
        ]
    )


def section_menu(section: str) -> InlineKeyboardMarkup:
    add_cb = "add_polka" if section == "polka" else "add_wishlist"

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="➕ Добавить товар", callback_data=add_cb)],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_menu")],
        ]
    )


@router.message(CommandStart())
async def cmd_start(message: Message):
    args = (message.text or "").split(maxsplit=1)
    source = normalize_source(args[1] if len(args) > 1 else "direct")

    register_start(message, source)

    await message.answer(
        "Привет! Это Beauty Bot ✨\n\n"
        "📦 «Полка» — средства, которыми ты уже пользуешься.\n"
        "✨ «Вишлист» — то, что хочешь купить, когда появится хорошая цена.\n\n"
        "Добавь товары и включи уведомления.\n\n"
        "Важно: это тестовая версия. На этом этапе бот сохраняет товары "
        "и настройки, но ещё не отправляет реальные уведомления о скидках.",
        reply_markup=main_menu(message.from_user.id),
    )


@router.message(Command("id"))
async def cmd_id(message: Message):
    await message.answer(
        f"Твой Telegram ID: {message.from_user.id}"
    )


@router.message(Command("privacy"))
async def cmd_privacy(message: Message):
    await message.answer(
        "Для работы тестовой версии бот сохраняет Telegram ID, доступные Telegram "
        "данные профиля (например, username и имя), источник перехода, действия "
        "внутри бота, добавленные названия товаров и настройку уведомлений. "
        "Телефон и email автоматически не запрашиваются."
    )


@router.message(Command("stats"))
async def cmd_stats(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("Команда доступна только администратору.")
        return

    await message.answer(build_stats_text())


@router.message(Command("export"))
async def cmd_export(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("Команда доступна только администратору.")
        return

    zip_path = export_database_to_zip()

    try:
        await message.answer_document(
            FSInputFile(zip_path),
            caption=(
                "Экспорт Phase 1: users.csv, starts.csv, items.csv, events.csv"
            ),
        )
    finally:
        try:
            temp_dir = zip_path.parent
            for file in temp_dir.iterdir():
                file.unlink(missing_ok=True)
            temp_dir.rmdir()
        except OSError:
            logger.warning("Не удалось удалить временный каталог экспорта.")


@router.callback_query(F.data == "back_to_menu")
async def back_to_menu(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    log_event(callback.from_user.id, "back_to_menu")

    await callback.message.edit_text(
        "Главное меню:",
        reply_markup=main_menu(callback.from_user.id),
    )
    await callback.answer()


@router.callback_query(F.data == "menu_polka")
async def menu_polka(callback: CallbackQuery):
    log_event(callback.from_user.id, "open_polka")

    items = get_items(callback.from_user.id, "polka")
    text = "📦 Твоя полка:\n" + (
        "\n".join(f"• {item}" for item in items)
        if items
        else "пока пусто"
    )

    await callback.message.edit_text(
        text,
        reply_markup=section_menu("polka"),
    )
    await callback.answer()


@router.callback_query(F.data == "menu_wishlist")
async def menu_wishlist(callback: CallbackQuery):
    log_event(callback.from_user.id, "open_wishlist")

    items = get_items(callback.from_user.id, "wishlist")
    text = "✨ Твой вишлист:\n" + (
        "\n".join(f"• {item}" for item in items)
        if items
        else "пока пусто"
    )

    await callback.message.edit_text(
        text,
        reply_markup=section_menu("wishlist"),
    )
    await callback.answer()


@router.callback_query(F.data == "add_polka")
async def add_polka_start(callback: CallbackQuery, state: FSMContext):
    log_event(
        callback.from_user.id,
        "add_item_click",
        {"section": "polka"},
    )

    await callback.message.edit_text(
        "Напиши название средства одним сообщением:"
    )
    await state.set_state(AddItem.waiting_polka)
    await callback.answer()


@router.callback_query(F.data == "add_wishlist")
async def add_wishlist_start(callback: CallbackQuery, state: FSMContext):
    log_event(
        callback.from_user.id,
        "add_item_click",
        {"section": "wishlist"},
    )

    await callback.message.edit_text(
        "Напиши бренд или продукт, который хочешь, одним сообщением:"
    )
    await state.set_state(AddItem.waiting_wishlist)
    await callback.answer()


async def validate_item_message(message: Message) -> str | None:
    if not message.text:
        await message.answer("Пришли название товара обычным текстом.")
        return None

    name = " ".join(message.text.split())

    if not name:
        await message.answer("Название не должно быть пустым.")
        return None

    if len(name) > 200:
        await message.answer(
            "Название слишком длинное. Напиши бренд и название товара короче."
        )
        return None

    return name


@router.message(AddItem.waiting_polka)
async def add_polka_finish(message: Message, state: FSMContext):
    name = await validate_item_message(message)
    if name is None:
        return

    added = add_item(message.from_user.id, "polka", name)
    await state.clear()

    if added:
        text = f"Добавила «{name}» на полку ✅"
    else:
        text = f"«{name}» уже есть на твоей полке."

    await message.answer(
        text,
        reply_markup=main_menu(message.from_user.id),
    )


@router.message(AddItem.waiting_wishlist)
async def add_wishlist_finish(message: Message, state: FSMContext):
    name = await validate_item_message(message)
    if name is None:
        return

    added = add_item(message.from_user.id, "wishlist", name)
    await state.clear()

    if added:
        text = f"Добавила «{name}» в вишлист ✅"
    else:
        text = f"«{name}» уже есть в твоём вишлисте."

    await message.answer(
        text,
        reply_markup=main_menu(message.from_user.id),
    )


@router.callback_query(F.data == "toggle_notify")
async def toggle_notify(callback: CallbackQuery):
    current = get_notifications(callback.from_user.id)
    new_value = not current
    set_notifications(callback.from_user.id, new_value)

    await callback.message.edit_reply_markup(
        reply_markup=main_menu(callback.from_user.id)
    )
    await callback.answer(
        "Уведомления включены"
        if new_value
        else "Уведомления выключены"
    )


async def main():
    if not BOT_TOKEN:
        raise SystemExit(
            "Не задан BOT_TOKEN. Создай .env по образцу .env.example."
        )

    init_db()

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    logger.info("Beauty Bot Phase 1 запущен")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
