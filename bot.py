"""
Beauty Bot — Phase 1, PostgreSQL version.

Назначение:
- регистрация пользователя при /start;
- сохранение Telegram-профиля и источника;
- сохранение каждого рекламного касания;
- Полка / Вишлист;
- события воронки;
- активация;
- /stats и /export для администраторов.

Эта версия использует PostgreSQL через DATABASE_URL.
"""

import asyncio
import csv
import logging
import os
import re
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from psycopg.errors import UniqueViolation
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

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
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
ITEMS_THRESHOLD = int(os.getenv("ITEMS_THRESHOLD", "2"))

ADMIN_IDS = {
    int(value.strip())
    for value in os.getenv("ADMIN_IDS", "").split(",")
    if value.strip().isdigit()
}

router = Router()
SOURCE_RE = re.compile(r"[^A-Za-z0-9_-]+")


def now_utc():
    return datetime.now(timezone.utc)


def normalize_source(raw: str | None) -> str:
    if not raw:
        return "direct"
    value = SOURCE_RE.sub("_", raw.strip())[:64]
    return value or "direct"


def connect_db():
    if not DATABASE_URL:
        raise RuntimeError(
            "Не задан DATABASE_URL. В Railway добавь Reference Variable "
            "из Postgres service."
        )
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def init_db() -> None:
    statements = [
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            language_code TEXT,
            is_premium BOOLEAN NOT NULL DEFAULT FALSE,

            first_source TEXT NOT NULL DEFAULT 'direct',
            last_source TEXT NOT NULL DEFAULT 'direct',

            started_at TIMESTAMPTZ NOT NULL,
            last_started_at TIMESTAMPTZ NOT NULL,

            notifications BOOLEAN NOT NULL DEFAULT FALSE,
            activated_at TIMESTAMPTZ
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS starts (
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
            source TEXT NOT NULL,
            started_at TIMESTAMPTZ NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS items (
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
            section TEXT NOT NULL CHECK(section IN ('polka', 'wishlist')),
            name TEXT NOT NULL,
            normalized_name TEXT NOT NULL,
            added_at TIMESTAMPTZ NOT NULL,
            UNIQUE(user_id, section, normalized_name)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS events (
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
            event_name TEXT NOT NULL,
            event_data JSONB,
            created_at TIMESTAMPTZ NOT NULL
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_starts_user_id ON starts(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_starts_source ON starts(source)",
        "CREATE INDEX IF NOT EXISTS idx_items_user_section ON items(user_id, section)",
        "CREATE INDEX IF NOT EXISTS idx_events_user_id ON events(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_events_name ON events(event_name)",
    ]

    with connect_db() as conn:
        with conn.cursor() as cur:
            for statement in statements:
                cur.execute(statement)


def log_event(user_id: int, event_name: str, data: dict | None = None) -> None:
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO events (user_id, event_name, event_data, created_at)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    user_id,
                    event_name,
                    Jsonb(data) if data is not None else None,
                    now_utc(),
                ),
            )


def register_start(message: Message, source: str) -> None:
    user = message.from_user
    if user is None:
        return

    timestamp = now_utc()

    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT user_id FROM users WHERE user_id = %s",
                (user.id,),
            )
            existing = cur.fetchone()

            if existing is None:
                cur.execute(
                    """
                    INSERT INTO users (
                        user_id, username, first_name, last_name, language_code,
                        is_premium, first_source, last_source,
                        started_at, last_started_at, notifications
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, FALSE)
                    """,
                    (
                        user.id,
                        user.username,
                        user.first_name,
                        user.last_name,
                        user.language_code,
                        bool(getattr(user, "is_premium", False)),
                        source,
                        source,
                        timestamp,
                        timestamp,
                    ),
                )
            else:
                cur.execute(
                    """
                    UPDATE users
                    SET username = %s,
                        first_name = %s,
                        last_name = %s,
                        language_code = %s,
                        is_premium = %s,
                        last_source = %s,
                        last_started_at = %s
                    WHERE user_id = %s
                    """,
                    (
                        user.username,
                        user.first_name,
                        user.last_name,
                        user.language_code,
                        bool(getattr(user, "is_premium", False)),
                        source,
                        timestamp,
                        user.id,
                    ),
                )

            cur.execute(
                """
                INSERT INTO starts (user_id, source, started_at)
                VALUES (%s, %s, %s)
                """,
                (user.id, source, timestamp),
            )

            cur.execute(
                """
                INSERT INTO events (user_id, event_name, event_data, created_at)
                VALUES (%s, 'start', %s, %s)
                """,
                (
                    user.id,
                    Jsonb({"source": source}),
                    timestamp,
                ),
            )


def normalize_item_name(name: str) -> str:
    return " ".join(name.casefold().split())


def add_item(user_id: int, section: str, name: str) -> bool:
    cleaned = " ".join(name.split())
    normalized = normalize_item_name(cleaned)

    try:
        with connect_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO items (
                        user_id, section, name, normalized_name, added_at
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (user_id, section, cleaned, normalized, now_utc()),
                )
        added = True
    except UniqueViolation:
        added = False

    if added:
        log_event(
            user_id,
            "item_added",
            {"section": section, "name": cleaned},
        )
        maybe_activate(user_id)

    return added


def set_notifications(user_id: int, value: bool) -> None:
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET notifications = %s WHERE user_id = %s",
                (value, user_id),
            )

    log_event(
        user_id,
        "notifications_on" if value else "notifications_off",
    )

    if value:
        maybe_activate(user_id)


def maybe_activate(user_id: int) -> bool:
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(DISTINCT normalized_name) AS items_count
                FROM items
                WHERE user_id = %s
                """,
                (user_id,),
            )
            items_count = cur.fetchone()["items_count"]

            cur.execute(
                """
                SELECT notifications, activated_at
                FROM users
                WHERE user_id = %s
                """,
                (user_id,),
            )
            user = cur.fetchone()

            if (
                user is not None
                and user["activated_at"] is None
                and user["notifications"] is True
                and items_count >= ITEMS_THRESHOLD
            ):
                timestamp = now_utc()

                cur.execute(
                    """
                    UPDATE users
                    SET activated_at = %s
                    WHERE user_id = %s
                    """,
                    (timestamp, user_id),
                )

                cur.execute(
                    """
                    INSERT INTO events (
                        user_id, event_name, event_data, created_at
                    )
                    VALUES (%s, 'activated', %s, %s)
                    """,
                    (
                        user_id,
                        Jsonb(
                            {
                                "items_count": items_count,
                                "threshold": ITEMS_THRESHOLD,
                            }
                        ),
                        timestamp,
                    ),
                )
                return True

    return False


def get_items(user_id: int, section: str) -> list[str]:
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT name
                FROM items
                WHERE user_id = %s AND section = %s
                ORDER BY id
                """,
                (user_id, section),
            )
            rows = cur.fetchall()

    return [row["name"] for row in rows]


def get_notifications(user_id: int) -> bool:
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT notifications FROM users WHERE user_id = %s",
                (user_id,),
            )
            row = cur.fetchone()

    return bool(row and row["notifications"])


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def build_stats_text() -> str:
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM users")
            total_users = cur.fetchone()["n"]

            cur.execute(
                """
                SELECT COUNT(*) AS n
                FROM users
                WHERE activated_at IS NOT NULL
                """
            )
            activated = cur.fetchone()["n"]

            cur.execute(
                """
                SELECT COUNT(*) AS n
                FROM users
                WHERE notifications = TRUE
                """
            )
            notifications = cur.fetchone()["n"]

            cur.execute(
                "SELECT COUNT(DISTINCT user_id) AS n FROM items"
            )
            with_items = cur.fetchone()["n"]

            cur.execute(
                """
                SELECT
                    first_source AS source,
                    COUNT(*) AS registrations,
                    COUNT(*) FILTER (
                        WHERE activated_at IS NOT NULL
                    ) AS activated
                FROM users
                GROUP BY first_source
                ORDER BY registrations DESC
                LIMIT 20
                """
            )
            sources = cur.fetchall()

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
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    temp_dir = Path(tempfile.mkdtemp(prefix="beauty_bot_export_"))
    zip_path = temp_dir / f"beauty_bot_export_{timestamp}.zip"
    tables = ["users", "starts", "items", "events"]

    with connect_db() as conn:
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for table in tables:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT * FROM {table} ORDER BY 1")
                    rows = cur.fetchall()
                    columns = [col.name for col in cur.description]

                csv_path = temp_dir / f"{table}.csv"
                with csv_path.open(
                    "w", newline="", encoding="utf-8-sig"
                ) as f:
                    writer = csv.writer(f)
                    writer.writerow(columns)
                    writer.writerows(
                        [[row[column] for column in columns] for row in rows]
                    )

                zf.write(csv_path, arcname=csv_path.name)

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
            [
                InlineKeyboardButton(
                    text="✨ Вишлист",
                    callback_data="menu_wishlist",
                )
            ],
            [
                InlineKeyboardButton(
                    text=notif_label,
                    callback_data="toggle_notify",
                )
            ],
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
    await message.answer(f"Твой Telegram ID: {message.from_user.id}")


@router.message(Command("privacy"))
async def cmd_privacy(message: Message):
    await message.answer(
        "Для работы тестовой версии бот сохраняет Telegram ID, доступные "
        "Telegram данные профиля, источник перехода, действия внутри бота, "
        "добавленные названия товаров и настройку уведомлений. "
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

    text = (
        f"Добавила «{name}» на полку ✅"
        if added
        else f"«{name}» уже есть на твоей полке."
    )

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

    text = (
        f"Добавила «{name}» в вишлист ✅"
        if added
        else f"«{name}» уже есть в твоём вишлисте."
    )

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
        raise SystemExit("Не задан BOT_TOKEN.")

    if not DATABASE_URL:
        raise SystemExit(
            "Не задан DATABASE_URL. В Railway свяжи сервис бота "
            "с Postgres через Reference Variable."
        )

    init_db()

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    logger.info("Beauty Bot Phase 1 + PostgreSQL запущен")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
