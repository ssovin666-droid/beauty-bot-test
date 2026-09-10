import asyncio
import csv
import io
import logging
import os
import re
import secrets
import tempfile
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import aiohttp
from aiohttp import web
import psycopg
from psycopg.errors import UniqueViolation
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, F, Router
from aiogram.exceptions import TelegramForbiddenError
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

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
BOT_USERNAME = os.getenv("BOT_USERNAME", "beautybot_test_bot").strip().lstrip("@")
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
ITEMS_THRESHOLD = int(os.getenv("ITEMS_THRESHOLD", "2"))
PORT = int(os.getenv("PORT", "8080"))

METRIKA_COUNTER_ID = os.getenv("METRIKA_COUNTER_ID", "").strip()
METRIKA_OAUTH_TOKEN = os.getenv("METRIKA_OAUTH_TOKEN", "").strip()
METRIKA_GOAL_ID = os.getenv("METRIKA_GOAL_ID", "bot_start").strip()

ADMIN_IDS = {
    int(x.strip())
    for x in os.getenv("ADMIN_IDS", "").split(",")
    if x.strip().isdigit()
}

router = Router()
SOURCE_RE = re.compile(r"[^A-Za-z0-9_-]+")
TOKEN_RE = re.compile(r"^r_[A-Za-z0-9_-]{10,60}$")


def now_utc():
    return datetime.now(timezone.utc)


def normalize_source(value):
    if not value:
        return "direct"
    return SOURCE_RE.sub("_", value.strip())[:64] or "direct"


def connect_db():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def init_db():
    sql = [
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
            section TEXT NOT NULL CHECK(section IN ('polka','wishlist')),
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
        """
        CREATE TABLE IF NOT EXISTS ad_clicks (
            token TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            yclid TEXT,
            campaign_id TEXT,
            ad_id TEXT,
            keyword TEXT,
            device_type TEXT,
            region_id TEXT,
            clicked_at TIMESTAMPTZ NOT NULL,
            started_at TIMESTAMPTZ,
            user_id BIGINT REFERENCES users(user_id) ON DELETE SET NULL,
            metrika_sent BOOLEAN NOT NULL DEFAULT FALSE,
            metrika_sent_at TIMESTAMPTZ,
            metrika_upload_id TEXT,
            metrika_error TEXT
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_starts_source ON starts(source)",
        "CREATE INDEX IF NOT EXISTS idx_events_name ON events(event_name)",
        "CREATE INDEX IF NOT EXISTS idx_ad_clicks_yclid ON ad_clicks(yclid)",
        "CREATE INDEX IF NOT EXISTS idx_ad_clicks_source ON ad_clicks(source)",
    ]
    with connect_db() as conn, conn.cursor() as cur:
        for statement in sql:
            cur.execute(statement)


def log_event(user_id, event_name, data=None):
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO events(user_id,event_name,event_data,created_at)
            VALUES (%s,%s,%s,%s)
            """,
            (user_id, event_name, Jsonb(data) if data else None, now_utc()),
        )


def create_click(source, request):
    token = "r_" + secrets.token_urlsafe(18)
    q = request.rel_url.query
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ad_clicks(
                token,source,yclid,campaign_id,ad_id,keyword,
                device_type,region_id,clicked_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                token,
                source,
                q.get("yclid"),
                q.get("campaign_id") or q.get("campaign"),
                q.get("ad_id") or q.get("ad"),
                q.get("keyword") or q.get("term"),
                q.get("device_type") or q.get("device"),
                q.get("region_id") or q.get("region"),
                now_utc(),
            ),
        )
    return token


def get_click(token):
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM ad_clicks WHERE token=%s", (token,))
        return cur.fetchone()


def bind_click(token, user_id):
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE ad_clicks
            SET user_id=%s, started_at=COALESCE(started_at,%s)
            WHERE token=%s
            """,
            (user_id, now_utc(), token),
        )


def mark_metrika(token, sent, upload_id=None, error=None):
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE ad_clicks
            SET metrika_sent=%s,
                metrika_sent_at=CASE WHEN %s THEN %s ELSE metrika_sent_at END,
                metrika_upload_id=COALESCE(%s,metrika_upload_id),
                metrika_error=%s
            WHERE token=%s
            """,
            (sent, sent, now_utc(), upload_id, error, token),
        )


def register_start(message, source, click_token=None):
    u = message.from_user
    ts = now_utc()
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute("SELECT user_id FROM users WHERE user_id=%s", (u.id,))
        exists = cur.fetchone()
        if exists:
            cur.execute(
                """
                UPDATE users SET
                    username=%s, first_name=%s, last_name=%s,
                    language_code=%s, is_premium=%s,
                    last_source=%s, last_started_at=%s
                WHERE user_id=%s
                """,
                (
                    u.username, u.first_name, u.last_name, u.language_code,
                    bool(getattr(u, "is_premium", False)),
                    source, ts, u.id,
                ),
            )
        else:
            cur.execute(
                """
                INSERT INTO users(
                    user_id,username,first_name,last_name,language_code,
                    is_premium,first_source,last_source,
                    started_at,last_started_at,notifications
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,FALSE)
                """,
                (
                    u.id,u.username,u.first_name,u.last_name,u.language_code,
                    bool(getattr(u, "is_premium", False)),
                    source,source,ts,ts,
                ),
            )
        cur.execute(
            "INSERT INTO starts(user_id,source,started_at) VALUES (%s,%s,%s)",
            (u.id, source, ts),
        )
        cur.execute(
            """
            INSERT INTO events(user_id,event_name,event_data,created_at)
            VALUES (%s,'start',%s,%s)
            """,
            (u.id, Jsonb({"source": source, "click_token": click_token}), ts),
        )


async def send_metrika_conversion(click_row):
    if not click_row or click_row.get("metrika_sent"):
        return
    token = click_row["token"]
    yclid = click_row.get("yclid")

    if not yclid:
        logger.info("No yclid for %s; Metrika upload skipped", token)
        return

    if not METRIKA_COUNTER_ID or not METRIKA_OAUTH_TOKEN:
        logger.info("Metrika is not fully configured yet")
        return

    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["Yclid", "Target", "DateTime"])
    w.writerow([yclid, METRIKA_GOAL_ID, int(time.time()) - 2])
    payload = out.getvalue().encode("utf-8")

    url = (
        "https://api-metrika.yandex.net/management/v1/counter/"
        f"{quote(METRIKA_COUNTER_ID)}/offline_conversions/upload"
    )
    headers = {"Authorization": f"OAuth {METRIKA_OAUTH_TOKEN}"}
    form = aiohttp.FormData()
    form.add_field(
        "file",
        payload,
        filename="offline-conversions.csv",
        content_type="text/csv",
    )

    try:
        timeout = aiohttp.ClientTimeout(total=20)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, headers=headers, data=form) as resp:
                body = await resp.text()
                if 200 <= resp.status < 300:
                    upload_id = None
                    try:
                        data = await resp.json(content_type=None)
                        upload_id = str(data.get("uploading", {}).get("id") or "") or None
                    except Exception:
                        pass
                    mark_metrika(token, True, upload_id=upload_id)
                    logger.info("bot_start sent to Metrika, upload_id=%s", upload_id)
                else:
                    mark_metrika(token, False, error=f"HTTP {resp.status}: {body[:500]}")
                    logger.error("Metrika error HTTP %s: %s", resp.status, body[:500])
    except Exception as e:
        mark_metrika(token, False, error=f"{type(e).__name__}: {e}")
        logger.exception("Metrika upload failed")


async def root_handler(request):
    return web.Response(text="Beauty Bot bridge is running", content_type="text/plain")


async def health_handler(request):
    return web.json_response({"ok": True, "service": "beauty-bot-test"})


async def go_handler(request):
    source = normalize_source(request.match_info.get("source"))
    token = create_click(source, request)
    tg_url = f"https://t.me/{BOT_USERNAME}?start={token}"
    logger.info(
        "Bridge click source=%s yclid=%s token=%s",
        source, request.rel_url.query.get("yclid"), token
    )
    raise web.HTTPFound(location=tg_url)


async def start_web_server():
    app = web.Application()
    app.router.add_get("/", root_handler)
    app.router.add_get("/health", health_handler)
    app.router.add_get("/go/{source}", go_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info("HTTP bridge listening on %s", PORT)
    return runner


def normalize_item(name):
    return " ".join(name.casefold().split())


def add_item(user_id, section, name):
    cleaned = " ".join(name.split())
    try:
        with connect_db() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO items(user_id,section,name,normalized_name,added_at)
                VALUES (%s,%s,%s,%s,%s)
                """,
                (user_id, section, cleaned, normalize_item(cleaned), now_utc()),
            )
        added = True
    except UniqueViolation:
        added = False
    if added:
        log_event(user_id, "item_added", {"section": section, "name": cleaned})
        maybe_activate(user_id)
    return added


def get_items(user_id, section):
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT name FROM items WHERE user_id=%s AND section=%s ORDER BY id",
            (user_id, section),
        )
        return [r["name"] for r in cur.fetchall()]


def get_notifications(user_id):
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute("SELECT notifications FROM users WHERE user_id=%s", (user_id,))
        row = cur.fetchone()
        return bool(row and row["notifications"])


def set_notifications(user_id, value):
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE users SET notifications=%s WHERE user_id=%s",
            (value, user_id),
        )
    log_event(user_id, "notifications_on" if value else "notifications_off")
    if value:
        maybe_activate(user_id)


def maybe_activate(user_id):
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(DISTINCT normalized_name) AS n FROM items WHERE user_id=%s",
            (user_id,),
        )
        count = cur.fetchone()["n"]
        cur.execute(
            "SELECT notifications,activated_at FROM users WHERE user_id=%s",
            (user_id,),
        )
        u = cur.fetchone()
        if u and u["activated_at"] is None and u["notifications"] and count >= ITEMS_THRESHOLD:
            ts = now_utc()
            cur.execute("UPDATE users SET activated_at=%s WHERE user_id=%s", (ts,user_id))
            cur.execute(
                """
                INSERT INTO events(user_id,event_name,event_data,created_at)
                VALUES (%s,'activated',%s,%s)
                """,
                (user_id, Jsonb({"items_count": count}), ts),
            )
            return True
    return False


def is_admin(user_id):
    return user_id in ADMIN_IDS


def stats_text():
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS n FROM ad_clicks")
        bridge_clicks = cur.fetchone()["n"]
        cur.execute("SELECT COUNT(*) AS n FROM ad_clicks WHERE started_at IS NOT NULL")
        bridge_starts = cur.fetchone()["n"]
        cur.execute("SELECT COUNT(*) AS n FROM ad_clicks WHERE metrika_sent=TRUE")
        sent = cur.fetchone()["n"]
        cur.execute("SELECT COUNT(*) AS n FROM users")
        users = cur.fetchone()["n"]
        cur.execute("SELECT COUNT(DISTINCT user_id) AS n FROM items")
        item_users = cur.fetchone()["n"]
        cur.execute("SELECT COUNT(*) AS n FROM users WHERE notifications=TRUE")
        notif = cur.fetchone()["n"]
        cur.execute("SELECT COUNT(*) AS n FROM users WHERE activated_at IS NOT NULL")
        act = cur.fetchone()["n"]

    c2s = bridge_starts / bridge_clicks * 100 if bridge_clicks else 0
    s2a = act / users * 100 if users else 0
    return (
        "📊 Phase 1\n\n"
        f"Переходов через Railway: {bridge_clicks}\n"
        f"Дошли до Telegram Start: {bridge_starts}\n"
        f"Bridge Click → Start: {c2s:.1f}%\n"
        f"bot_start отправлено в Метрику: {sent}\n\n"
        f"Пользователей: {users}\n"
        f"Добавили ≥1 товар: {item_users}\n"
        f"Уведомления включены: {notif}\n"
        f"Activation: {act}\n"
        f"Start → Activation: {s2a:.1f}%"
    )


class AddItem(StatesGroup):
    waiting_polka = State()
    waiting_wishlist = State()


def main_menu(user_id):
    on = get_notifications(user_id)
    label = "🔔 Уведомления: включены" if on else "🔕 Включить уведомления"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📦 Полка", callback_data="menu_polka")],
        [InlineKeyboardButton(text="✨ Вишлист", callback_data="menu_wishlist")],
        [InlineKeyboardButton(text=label, callback_data="toggle_notify")],
    ])


def section_menu(section):
    cb = "add_polka" if section == "polka" else "add_wishlist"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить товар", callback_data=cb)],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_menu")],
    ])


@router.message(CommandStart())
async def cmd_start(message: Message):
    parts = (message.text or "").split(maxsplit=1)
    payload = parts[1].strip() if len(parts) > 1 else "direct"

    source = normalize_source(payload)
    click_token = None
    click = None

    if TOKEN_RE.match(payload):
        click = get_click(payload)
        if click:
            source = click["source"]
            click_token = payload

    register_start(message, source, click_token)

    if click_token:
        bind_click(click_token, message.from_user.id)
        click = get_click(click_token)

    try:
        await message.answer(
            "Привет! Это Beauty Bot ✨\n\n"
            "📦 «Полка» — средства, которыми ты уже пользуешься.\n"
            "✨ «Вишлист» — то, что хочешь купить, когда появится хорошая цена.\n\n"
            "Добавь товары и включи уведомления.\n\n"
            "Важно: это тестовая версия.",
            reply_markup=main_menu(message.from_user.id),
        )
    except TelegramForbiddenError:
        logger.warning("User %s blocked the bot", message.from_user.id)
        return

    if click and click.get("yclid"):
        asyncio.create_task(send_metrika_conversion(click))


@router.message(Command("id"))
async def cmd_id(message: Message):
    await message.answer(f"Твой Telegram ID: {message.from_user.id}")


@router.message(Command("stats"))
async def cmd_stats(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("Команда доступна только администратору.")
        return
    await message.answer(stats_text())


@router.message(Command("metrika_status"))
async def cmd_metrika_status(message: Message):
    if not is_admin(message.from_user.id):
        return
    await message.answer(
        "Метрика:\n"
        f"Counter ID: {'✅' if METRIKA_COUNTER_ID else '❌'}\n"
        f"OAuth: {'✅' if METRIKA_OAUTH_TOKEN else '❌'}\n"
        f"Goal: {METRIKA_GOAL_ID}\n"
        f"Port: {PORT}"
    )


@router.callback_query(F.data == "back_to_menu")
async def back_to_menu(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    log_event(callback.from_user.id, "back_to_menu")
    await callback.message.edit_text("Главное меню:", reply_markup=main_menu(callback.from_user.id))
    await callback.answer()


@router.callback_query(F.data == "menu_polka")
async def menu_polka(callback: CallbackQuery):
    log_event(callback.from_user.id, "open_polka")
    items = get_items(callback.from_user.id, "polka")
    text = "📦 Твоя полка:\n" + ("\n".join(f"• {x}" for x in items) if items else "пока пусто")
    await callback.message.edit_text(text, reply_markup=section_menu("polka"))
    await callback.answer()


@router.callback_query(F.data == "menu_wishlist")
async def menu_wishlist(callback: CallbackQuery):
    log_event(callback.from_user.id, "open_wishlist")
    items = get_items(callback.from_user.id, "wishlist")
    text = "✨ Твой вишлист:\n" + ("\n".join(f"• {x}" for x in items) if items else "пока пусто")
    await callback.message.edit_text(text, reply_markup=section_menu("wishlist"))
    await callback.answer()


@router.callback_query(F.data == "add_polka")
async def add_polka_start(callback: CallbackQuery, state: FSMContext):
    log_event(callback.from_user.id, "add_item_click", {"section":"polka"})
    await callback.message.edit_text("Напиши название средства одним сообщением:")
    await state.set_state(AddItem.waiting_polka)
    await callback.answer()


@router.callback_query(F.data == "add_wishlist")
async def add_wishlist_start(callback: CallbackQuery, state: FSMContext):
    log_event(callback.from_user.id, "add_item_click", {"section":"wishlist"})
    await callback.message.edit_text("Напиши бренд или продукт одним сообщением:")
    await state.set_state(AddItem.waiting_wishlist)
    await callback.answer()


async def validate_item(message):
    if not message.text:
        await message.answer("Пришли название товара обычным текстом.")
        return None
    name = " ".join(message.text.split())
    if not name:
        return None
    if len(name) > 200:
        await message.answer("Название слишком длинное.")
        return None
    return name


@router.message(AddItem.waiting_polka)
async def add_polka_finish(message: Message, state: FSMContext):
    name = await validate_item(message)
    if not name:
        return
    added = add_item(message.from_user.id, "polka", name)
    await state.clear()
    text = f"Добавила «{name}» на полку ✅" if added else f"«{name}» уже есть на полке."
    await message.answer(text, reply_markup=main_menu(message.from_user.id))


@router.message(AddItem.waiting_wishlist)
async def add_wishlist_finish(message: Message, state: FSMContext):
    name = await validate_item(message)
    if not name:
        return
    added = add_item(message.from_user.id, "wishlist", name)
    await state.clear()
    text = f"Добавила «{name}» в вишлист ✅" if added else f"«{name}» уже есть в вишлисте."
    await message.answer(text, reply_markup=main_menu(message.from_user.id))


@router.callback_query(F.data == "toggle_notify")
async def toggle_notify(callback: CallbackQuery):
    current = get_notifications(callback.from_user.id)
    set_notifications(callback.from_user.id, not current)
    await callback.message.edit_reply_markup(reply_markup=main_menu(callback.from_user.id))
    await callback.answer("Уведомления выключены" if current else "Уведомления включены")


async def main():
    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN is not set")
    if not DATABASE_URL:
        raise SystemExit("DATABASE_URL is not set")

    init_db()
    web_runner = await start_web_server()

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    logger.info("Beauty Bot + Postgres + Yandex bridge started")

    try:
        await dp.start_polling(bot)
    finally:
        await web_runner.cleanup()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
