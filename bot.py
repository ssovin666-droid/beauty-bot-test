"""
Beauty Bot — бот-заглушка для Фазы 1 теста (см. план запуска).

Что делает:
- /start [source] — регистрирует пользователя, запоминает источник перехода
  (передаётся как параметр в ссылке, см. README про UTM-ссылки)
- Разделы «Полка» и «Вишлист» — добавление товаров текстом
- Кнопка «Включить уведомления» — метрика активации
- Пользователь считается «активным», когда добавил ITEMS_THRESHOLD товаров
  И включил уведомления (см. README, порог можно менять)
- Все события пишутся в SQLite (beauty_bot.db) — по ним считает stats.py

Никаких реальных уведомлений о скидках бот не шлёт — это заглушка для теста
гипотезы, а не финальный продукт.
"""

import asyncio
import logging
import os
import sqlite3
from datetime import datetime

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
DB_PATH = os.environ.get("DB_PATH", "beauty_bot.db")
ITEMS_THRESHOLD = int(os.environ.get("ITEMS_THRESHOLD", "2"))  # сколько товаров нужно добавить для "активации"

router = Router()


# ---------- База данных ----------

def db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            source TEXT,
            started_at TEXT,
            notifications INTEGER DEFAULT 0,
            activated_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            section TEXT,
            name TEXT,
            added_at TEXT
        )
    """)
    return conn


def now():
    return datetime.utcnow().isoformat()


def get_or_create_user(user_id: int, source: str):
    conn = db()
    row = conn.execute("SELECT user_id FROM users WHERE user_id=?", (user_id,)).fetchone()
    if row is None:
        conn.execute(
            "INSERT INTO users (user_id, source, started_at, notifications) VALUES (?, ?, ?, 0)",
            (user_id, source or "unknown", now()),
        )
        conn.commit()
    conn.close()


def add_item(user_id: int, section: str, name: str):
    conn = db()
    conn.execute(
        "INSERT INTO items (user_id, section, name, added_at) VALUES (?, ?, ?, ?)",
        (user_id, section, name.strip(), now()),
    )
    conn.commit()
    conn.close()
    maybe_activate(user_id)


def set_notifications(user_id: int, value: int):
    conn = db()
    conn.execute("UPDATE users SET notifications=? WHERE user_id=?", (value, user_id))
    conn.commit()
    conn.close()
    if value:
        maybe_activate(user_id)


def maybe_activate(user_id: int):
    """Помечает пользователя активным, если выполнены оба условия."""
    conn = db()
    items_count = conn.execute(
        "SELECT COUNT(*) FROM items WHERE user_id=?", (user_id,)
    ).fetchone()[0]
    user = conn.execute(
        "SELECT notifications, activated_at FROM users WHERE user_id=?", (user_id,)
    ).fetchone()
    if user and user[1] is None and items_count >= ITEMS_THRESHOLD and user[0] == 1:
        conn.execute(
            "UPDATE users SET activated_at=? WHERE user_id=?", (now(), user_id)
        )
        conn.commit()
    conn.close()


def get_items(user_id: int, section: str):
    conn = db()
    rows = conn.execute(
        "SELECT name FROM items WHERE user_id=? AND section=? ORDER BY added_at",
        (user_id, section),
    ).fetchall()
    conn.close()
    return [r[0] for r in rows]


def get_notifications(user_id: int) -> bool:
    conn = db()
    row = conn.execute("SELECT notifications FROM users WHERE user_id=?", (user_id,)).fetchone()
    conn.close()
    return bool(row and row[0])


# ---------- Состояния (ожидание ввода товара) ----------

class AddItem(StatesGroup):
    waiting_polka = State()
    waiting_vishlist = State()


# ---------- Клавиатуры ----------

def main_menu(user_id: int) -> InlineKeyboardMarkup:
    notif_on = get_notifications(user_id)
    notif_label = "🔔 Уведомления: включены" if notif_on else "🔕 Включить уведомления"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📦 Полка", callback_data="menu_polka")],
        [InlineKeyboardButton(text="✨ Вишлист", callback_data="menu_vishlist")],
        [InlineKeyboardButton(text=notif_label, callback_data="toggle_notify")],
    ])


def section_menu(section: str) -> InlineKeyboardMarkup:
    add_cb = "add_polka" if section == "polka" else "add_vishlist"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить товар", callback_data=add_cb)],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_menu")],
    ])


# ---------- Хендлеры ----------

@router.message(CommandStart())
async def cmd_start(message: Message, command: CommandStart):
    # /start vish_vk_a  -> source = "vish_vk_a"
    args = message.text.split(maxsplit=1)
    source = args[1].strip() if len(args) > 1 else "direct"
    get_or_create_user(message.from_user.id, source)

    await message.answer(
        "Привет! Это Beauty Bot.\n\n"
        "«Полка» — средства, которыми ты уже пользуешься. Мы будем следить за скидками на них.\n"
        "«Вишлист» — то, что хочешь попробовать, но не по полной цене. Сообщим, когда будет скидка.\n\n"
        "Реальных уведомлений эта версия пока не шлёт — мы проверяем, интересен ли сам формат.",
        reply_markup=main_menu(message.from_user.id),
    )


@router.callback_query(F.data == "back_to_menu")
async def back_to_menu(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text(
        "Главное меню:",
        reply_markup=main_menu(callback.from_user.id),
    )
    await callback.answer()


@router.callback_query(F.data == "menu_polka")
async def menu_polka(callback: CallbackQuery):
    items = get_items(callback.from_user.id, "polka")
    text = "📦 Твоя полка:\n" + ("\n".join(f"• {i}" for i in items) if items else "пока пусто")
    await callback.message.edit_text(text, reply_markup=section_menu("polka"))
    await callback.answer()


@router.callback_query(F.data == "menu_vishlist")
async def menu_vishlist(callback: CallbackQuery):
    items = get_items(callback.from_user.id, "vishlist")
    text = "✨ Твой вишлист:\n" + ("\n".join(f"• {i}" for i in items) if items else "пока пусто")
    await callback.message.edit_text(text, reply_markup=section_menu("vishlist"))
    await callback.answer()


@router.callback_query(F.data == "add_polka")
async def add_polka_start(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("Напиши название средства одним сообщением:")
    await state.set_state(AddItem.waiting_polka)
    await callback.answer()


@router.callback_query(F.data == "add_vishlist")
async def add_vishlist_start(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("Напиши бренд или продукт, который хочешь, одним сообщением:")
    await state.set_state(AddItem.waiting_vishlist)
    await callback.answer()


@router.message(AddItem.waiting_polka)
async def add_polka_finish(message: Message, state: FSMContext):
    add_item(message.from_user.id, "polka", message.text)
    await state.clear()
    await message.answer(f"Добавила «{message.text}» на полку ✅", reply_markup=main_menu(message.from_user.id))


@router.message(AddItem.waiting_vishlist)
async def add_vishlist_finish(message: Message, state: FSMContext):
    add_item(message.from_user.id, "vishlist", message.text)
    await state.clear()
    await message.answer(f"Добавила «{message.text}» в вишлист ✅", reply_markup=main_menu(message.from_user.id))


@router.callback_query(F.data == "toggle_notify")
async def toggle_notify(callback: CallbackQuery):
    current = get_notifications(callback.from_user.id)
    set_notifications(callback.from_user.id, 0 if current else 1)
    await callback.message.edit_reply_markup(reply_markup=main_menu(callback.from_user.id))
    await callback.answer("Уведомления выключены" if current else "Уведомления включены")


@router.message(Command("stats"))
async def cmd_stats(message: Message):
    """Быстрая проверка для себя: сколько всего пользователей и активных."""
    conn = db()
    total = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    activated = conn.execute("SELECT COUNT(*) FROM users WHERE activated_at IS NOT NULL").fetchone()[0]
    conn.close()
    await message.answer(f"Всего регистраций: {total}\nАктивных: {activated}\n\nПодробный разрез по источникам — см. stats.py")


async def main():
    if not BOT_TOKEN:
        raise SystemExit("Не задан BOT_TOKEN. См. README.md — как получить токен у @BotFather.")
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    logger.info("Бот запущен")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
