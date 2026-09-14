import asyncio
import csv
import io
import json
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
METRIKA_STATUS_POLL_SECONDS = int(os.getenv("METRIKA_STATUS_POLL_SECONDS", "600"))

ADMIN_IDS = {
    int(x.strip())
    for x in os.getenv("ADMIN_IDS", "").split(",")
    if x.strip().isdigit()
}

router = Router()
SOURCE_RE = re.compile(r"[^A-Za-z0-9_-]+")
TOKEN_RE = re.compile(r"^r_[A-Za-z0-9_-]{10,60}$")
TERMINAL_METRIKA_STATUSES = {"PROCESSED", "LINKAGE_FAILURE"}
BACKGROUND_TASKS = set()


def spawn_task(coro):
    """Keep a strong reference to background tasks until they finish."""
    task = asyncio.create_task(coro)
    BACKGROUND_TASKS.add(task)
    task.add_done_callback(BACKGROUND_TASKS.discard)
    return task


def clean_log_value(value, limit=1000):
    if value is None:
        return None
    return str(value).replace("\n", " ").replace("\r", " ")[:limit]


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
        # Migrations for installations where ad_clicks already existed.
        "ALTER TABLE ad_clicks ADD COLUMN IF NOT EXISTS metrika_attempted_at TIMESTAMPTZ",
        "ALTER TABLE ad_clicks ADD COLUMN IF NOT EXISTS metrika_http_status INTEGER",
        "ALTER TABLE ad_clicks ADD COLUMN IF NOT EXISTS metrika_upload_status TEXT",
        "ALTER TABLE ad_clicks ADD COLUMN IF NOT EXISTS metrika_last_checked_at TIMESTAMPTZ",
        "ALTER TABLE ad_clicks ADD COLUMN IF NOT EXISTS metrika_processed_at TIMESTAMPTZ",
        "ALTER TABLE ad_clicks ADD COLUMN IF NOT EXISTS metrika_response TEXT",
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
    yclid = q.get("yclid") or q.get("Yclid") or q.get("YCLID")
    campaign_id = q.get("campaign_id") or q.get("campaign")
    ad_id = q.get("ad_id") or q.get("ad")
    keyword = q.get("keyword") or q.get("term")
    device_type = q.get("device_type") or q.get("device")
    region_id = q.get("region_id") or q.get("region")

    logger.info(
        "BRIDGE | CLICK_RECEIVED | source=%s | yclid=%s | campaign_id=%s | "
        "ad_id=%s | device_type=%s | region_id=%s | token=%s",
        source,
        clean_log_value(yclid, 300),
        clean_log_value(campaign_id, 100),
        clean_log_value(ad_id, 100),
        clean_log_value(device_type, 100),
        clean_log_value(region_id, 100),
        token,
    )

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
                yclid,
                campaign_id,
                ad_id,
                keyword,
                device_type,
                region_id,
                now_utc(),
            ),
        )

    logger.info(
        "BRIDGE | CLICK_SAVED | token=%s | source=%s | has_yclid=%s",
        token,
        source,
        bool(yclid),
    )
    return token


def get_click(token):
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM ad_clicks WHERE token=%s", (token,))
        return cur.fetchone()


def get_recent_unbound_clicks(seconds=180, limit=5):
    """Recent bridge clicks that have not yet been bound to a Telegram Start."""
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT token, source, yclid, clicked_at
            FROM ad_clicks
            WHERE started_at IS NULL
              AND clicked_at >= NOW() - (%s * INTERVAL '1 second')
            ORDER BY clicked_at DESC
            LIMIT %s
            """,
            (seconds, limit),
        )
        return cur.fetchall()


def log_recent_unbound_clicks(context, user_id=None):
    try:
        rows = get_recent_unbound_clicks()
        if not rows:
            logger.warning(
                "ATTRIBUTION | RECENT_UNBOUND | context=%s | user_id=%s | count=0",
                context,
                user_id,
            )
            return
        summary = "; ".join(
            f"token={r['token']},source={r['source']},has_yclid={bool(r.get('yclid'))},clicked_at={r['clicked_at']}"
            for r in rows
        )
        logger.warning(
            "ATTRIBUTION | RECENT_UNBOUND | context=%s | user_id=%s | count=%s | %s",
            context,
            user_id,
            len(rows),
            summary,
        )
    except Exception:
        logger.exception(
            "ATTRIBUTION | RECENT_UNBOUND_CHECK_FAILED | context=%s | user_id=%s",
            context,
            user_id,
        )


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
        return cur.rowcount


def mark_metrika_attempt(token):
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE ad_clicks
            SET metrika_attempted_at=%s,
                metrika_error=NULL
            WHERE token=%s
            """,
            (now_utc(), token),
        )


def mark_metrika_http(token, http_status, response_body=None, error=None):
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE ad_clicks
            SET metrika_http_status=%s,
                metrika_response=%s,
                metrika_error=%s
            WHERE token=%s
            """,
            (http_status, clean_log_value(response_body, 4000), error, token),
        )


def mark_metrika_accepted(token, upload_id, upload_status, response_body=None):
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE ad_clicks
            SET metrika_sent=TRUE,
                metrika_sent_at=%s,
                metrika_upload_id=%s,
                metrika_upload_status=%s,
                metrika_response=%s,
                metrika_error=NULL
            WHERE token=%s
            """,
            (
                now_utc(),
                upload_id,
                upload_status,
                clean_log_value(response_body, 4000),
                token,
            ),
        )


def mark_metrika_upload_status(token, status, response_body=None, error=None):
    processed_at = now_utc() if status == "PROCESSED" else None
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE ad_clicks
            SET metrika_upload_status=%s,
                metrika_last_checked_at=%s,
                metrika_processed_at=COALESCE(%s, metrika_processed_at),
                metrika_response=COALESCE(%s, metrika_response),
                metrika_error=%s
            WHERE token=%s
            """,
            (
                status,
                now_utc(),
                processed_at,
                clean_log_value(response_body, 4000),
                error,
                token,
            ),
        )


def get_pending_metrika_uploads(limit=50):
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT token,metrika_upload_id,metrika_upload_status
            FROM ad_clicks
            WHERE metrika_upload_id IS NOT NULL
              AND COALESCE(metrika_upload_status,'') NOT IN ('PROCESSED','LINKAGE_FAILURE')
            ORDER BY metrika_sent_at ASC NULLS LAST
            LIMIT %s
            """,
            (limit,),
        )
        return cur.fetchall()


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


async def check_metrika_upload_status(token, upload_id):
    if not upload_id:
        logger.warning(
            "METRIKA | STATUS_SKIP | reason=no_upload_id | token=%s",
            token,
        )
        return None

    if not METRIKA_COUNTER_ID or not METRIKA_OAUTH_TOKEN:
        logger.error(
            "METRIKA | STATUS_SKIP | reason=config_missing | token=%s | upload_id=%s",
            token,
            upload_id,
        )
        return None

    url = (
        "https://api-metrika.yandex.net/management/v1/counter/"
        f"{quote(METRIKA_COUNTER_ID)}/offline_conversions/uploading/{quote(str(upload_id))}"
    )
    headers = {"Authorization": f"OAuth {METRIKA_OAUTH_TOKEN}"}

    logger.info(
        "METRIKA | STATUS_CHECK | token=%s | upload_id=%s",
        token,
        upload_id,
    )

    try:
        timeout = aiohttp.ClientTimeout(total=20)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers=headers) as resp:
                body = await resp.text()
                logger.info(
                    "METRIKA | STATUS_HTTP_RESPONSE | status=%s | token=%s | "
                    "upload_id=%s | body=%s",
                    resp.status,
                    token,
                    upload_id,
                    clean_log_value(body, 1500),
                )

                if not 200 <= resp.status < 300:
                    error_text = f"STATUS HTTP {resp.status}: {body[:1500]}"
                    mark_metrika_upload_status(
                        token,
                        status="STATUS_CHECK_FAILED",
                        response_body=body,
                        error=error_text,
                    )
                    logger.error(
                        "METRIKA | STATUS_FAILED | token=%s | upload_id=%s | "
                        "http_status=%s | error=%s",
                        token,
                        upload_id,
                        resp.status,
                        clean_log_value(error_text, 1500),
                    )
                    return None

                try:
                    data = json.loads(body)
                except json.JSONDecodeError:
                    error_text = "Status API returned non-JSON response"
                    mark_metrika_upload_status(
                        token,
                        status="STATUS_CHECK_FAILED",
                        response_body=body,
                        error=error_text,
                    )
                    logger.error(
                        "METRIKA | STATUS_FAILED | token=%s | upload_id=%s | error=%s",
                        token,
                        upload_id,
                        error_text,
                    )
                    return None

                uploading = data.get("uploading") or {}
                status = str(uploading.get("status") or "UNKNOWN")
                source_quantity = uploading.get("source_quantity")
                line_quantity = uploading.get("line_quantity")
                client_id_type = uploading.get("client_id_type")

                linkage_error = (
                    "Yandex Metrica could not link the Yclid to a visit"
                    if status == "LINKAGE_FAILURE"
                    else None
                )
                mark_metrika_upload_status(
                    token,
                    status=status,
                    response_body=body,
                    error=linkage_error,
                )

                logger.info(
                    "METRIKA | STATUS | token=%s | upload_id=%s | status=%s | "
                    "source_quantity=%s | line_quantity=%s | client_id_type=%s",
                    token,
                    upload_id,
                    status,
                    source_quantity,
                    line_quantity,
                    client_id_type,
                )

                if status == "PROCESSED":
                    logger.info(
                        "METRIKA | PROCESSED | token=%s | upload_id=%s",
                        token,
                        upload_id,
                    )
                elif status == "LINKAGE_FAILURE":
                    logger.error(
                        "METRIKA | LINKAGE_FAILURE | token=%s | upload_id=%s",
                        token,
                        upload_id,
                    )

                return status

    except Exception as e:
        error_text = f"{type(e).__name__}: {e}"
        mark_metrika_upload_status(
            token,
            status="STATUS_CHECK_FAILED",
            error=error_text,
        )
        logger.exception(
            "METRIKA | STATUS_EXCEPTION | token=%s | upload_id=%s | error=%s",
            token,
            upload_id,
            error_text,
        )
        return None


async def metrika_monitor_loop():
    """Periodically checks Yandex processing status for accepted uploads."""
    await asyncio.sleep(30)
    while True:
        try:
            pending = get_pending_metrika_uploads(limit=50)
            if pending:
                logger.info(
                    "METRIKA | MONITOR | pending_uploads=%s",
                    len(pending),
                )
            for row in pending:
                await check_metrika_upload_status(
                    row["token"],
                    row["metrika_upload_id"],
                )
                await asyncio.sleep(0.2)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("METRIKA | MONITOR_EXCEPTION")

        await asyncio.sleep(max(METRIKA_STATUS_POLL_SECONDS, 60))


async def send_metrika_conversion(click_row, user_id=None, payload=None):
    """Upload bot_start as an offline conversion and log the complete lifecycle."""
    if not click_row:
        logger.warning(
            "METRIKA | SKIP | reason=no_click_row | user_id=%s | payload=%s",
            user_id,
            clean_log_value(payload, 300),
        )
        return

    token = click_row["token"]
    source = click_row.get("source")
    yclid = click_row.get("yclid")

    if click_row.get("metrika_sent"):
        logger.info(
            "METRIKA | SKIP | reason=already_uploaded | token=%s | source=%s | "
            "upload_id=%s | status=%s",
            token,
            source,
            click_row.get("metrika_upload_id"),
            click_row.get("metrika_upload_status"),
        )
        return

    if not yclid:
        logger.warning(
            "METRIKA | SKIP | reason=no_yclid | token=%s | source=%s | user_id=%s",
            token,
            source,
            user_id,
        )
        return

    if not METRIKA_COUNTER_ID or not METRIKA_OAUTH_TOKEN or not METRIKA_GOAL_ID:
        logger.error(
            "METRIKA | SKIP | reason=config_missing | counter_id_set=%s | "
            "oauth_token_set=%s | goal_id_set=%s | token=%s | source=%s",
            bool(METRIKA_COUNTER_ID),
            bool(METRIKA_OAUTH_TOKEN),
            bool(METRIKA_GOAL_ID),
            token,
            source,
        )
        return

    conversion_ts = int(time.time()) - 5

    out = io.StringIO(newline="")
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow(["Yclid", "Target", "DateTime"])
    writer.writerow([yclid, METRIKA_GOAL_ID, conversion_ts])
    csv_text = out.getvalue()
    payload_bytes = csv_text.encode("utf-8")

    logger.info(
        "METRIKA | PREPARE | token=%s | user_id=%s | source=%s | yclid=%s | "
        "counter_id=%s | goal=%s | datetime=%s | csv=%s",
        token,
        user_id,
        source,
        clean_log_value(yclid, 300),
        METRIKA_COUNTER_ID,
        METRIKA_GOAL_ID,
        conversion_ts,
        clean_log_value(csv_text, 800),
    )

    url = (
        "https://api-metrika.yandex.net/management/v1/counter/"
        f"{quote(METRIKA_COUNTER_ID)}/offline_conversions/upload"
    )
    headers = {"Authorization": f"OAuth {METRIKA_OAUTH_TOKEN}"}
    form = aiohttp.FormData()
    form.add_field(
        "file",
        payload_bytes,
        filename="offline-conversions.csv",
        content_type="text/csv; charset=utf-8",
    )

    mark_metrika_attempt(token)
    logger.info(
        "METRIKA | SEND_ATTEMPT | token=%s | source=%s | yclid=%s | goal=%s",
        token,
        source,
        clean_log_value(yclid, 300),
        METRIKA_GOAL_ID,
    )

    try:
        timeout = aiohttp.ClientTimeout(total=20)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, headers=headers, data=form) as resp:
                body = await resp.text()
                logger.info(
                    "METRIKA | HTTP_RESPONSE | status=%s | token=%s | body=%s",
                    resp.status,
                    token,
                    clean_log_value(body, 1500),
                )

                if not 200 <= resp.status < 300:
                    error_text = f"HTTP {resp.status}: {body[:1500]}"
                    mark_metrika_http(
                        token,
                        resp.status,
                        response_body=body,
                        error=error_text,
                    )
                    logger.error(
                        "METRIKA | FAILED | status=%s | token=%s | source=%s | "
                        "yclid=%s | error=%s",
                        resp.status,
                        token,
                        source,
                        clean_log_value(yclid, 300),
                        clean_log_value(error_text, 1500),
                    )
                    return

                mark_metrika_http(token, resp.status, response_body=body)

                try:
                    data = json.loads(body)
                except json.JSONDecodeError:
                    error_text = "HTTP 2xx but response is not valid JSON"
                    mark_metrika_http(
                        token,
                        resp.status,
                        response_body=body,
                        error=error_text,
                    )
                    logger.error(
                        "METRIKA | FAILED | reason=invalid_json | token=%s | body=%s",
                        token,
                        clean_log_value(body, 1500),
                    )
                    return

                uploading = data.get("uploading") or {}
                upload_id = uploading.get("id")
                upload_status = str(uploading.get("status") or "ACCEPTED")
                source_quantity = uploading.get("source_quantity")
                line_quantity = uploading.get("line_quantity")
                client_id_type = uploading.get("client_id_type")

                if upload_id is None:
                    error_text = "HTTP 2xx but Yandex did not return uploading.id"
                    mark_metrika_http(
                        token,
                        resp.status,
                        response_body=body,
                        error=error_text,
                    )
                    logger.error(
                        "METRIKA | FAILED | reason=no_upload_id | token=%s | body=%s",
                        token,
                        clean_log_value(body, 1500),
                    )
                    return

                upload_id = str(upload_id)
                mark_metrika_accepted(
                    token,
                    upload_id=upload_id,
                    upload_status=upload_status,
                    response_body=body,
                )

                logger.info(
                    "METRIKA | UPLOAD_ACCEPTED | token=%s | upload_id=%s | "
                    "status=%s | source_quantity=%s | line_quantity=%s | "
                    "client_id_type=%s",
                    token,
                    upload_id,
                    upload_status,
                    source_quantity,
                    line_quantity,
                    client_id_type,
                )

                # One near-immediate status check; the periodic monitor continues later.
                async def delayed_check():
                    await asyncio.sleep(5)
                    await check_metrika_upload_status(token, upload_id)

                spawn_task(delayed_check())

    except Exception as e:
        error_text = f"{type(e).__name__}: {e}"
        mark_metrika_http(token, None, error=error_text)
        logger.exception(
            "METRIKA | EXCEPTION | token=%s | source=%s | yclid=%s | error=%s",
            token,
            source,
            clean_log_value(yclid, 300),
            error_text,
        )


async def root_handler(request):
    return web.Response(text="Beauty Bot bridge is running", content_type="text/plain")


async def health_handler(request):
    return web.json_response({"ok": True, "service": "beauty-bot-test"})


async def go_handler(request):
    source = normalize_source(request.match_info.get("source"))
    try:
        token = create_click(source, request)
    except Exception:
        logger.exception(
            "BRIDGE | CLICK_SAVE_FAILED | source=%s | query=%s",
            source,
            clean_log_value(request.rel_url.query_string, 1000),
        )
        raise web.HTTPInternalServerError(text="Bridge error")

    tg_url = f"https://t.me/{BOT_USERNAME}?start={token}"
    logger.info(
        "BRIDGE | REDIRECT | token=%s | source=%s | has_yclid=%s | url=%s",
        token,
        source,
        bool(request.rel_url.query.get("yclid")),
        tg_url,
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
        cur.execute("SELECT COUNT(*) AS n FROM ad_clicks WHERE yclid IS NOT NULL AND yclid <> ''")
        clicks_with_yclid = cur.fetchone()["n"]
        cur.execute("SELECT COUNT(*) AS n FROM ad_clicks WHERE started_at IS NOT NULL")
        bridge_starts = cur.fetchone()["n"]
        cur.execute("SELECT COUNT(*) AS n FROM ad_clicks WHERE metrika_sent=TRUE")
        accepted = cur.fetchone()["n"]
        cur.execute("SELECT COUNT(*) AS n FROM ad_clicks WHERE metrika_upload_status='PROCESSED'")
        processed = cur.fetchone()["n"]
        cur.execute("SELECT COUNT(*) AS n FROM ad_clicks WHERE metrika_upload_status='LINKAGE_FAILURE'")
        linkage_failed = cur.fetchone()["n"]
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
        f"Из них с yclid: {clicks_with_yclid}\n"
        f"Дошли до Telegram Start: {bridge_starts}\n"
        f"Bridge Click → Start: {c2s:.1f}%\n"
        f"Метрика API приняла bot_start: {accepted}\n"
        f"Метрика обработала: {processed}\n"
        f"Linkage failure: {linkage_failed}\n\n"
        f"Пользователей: {users}\n"
        f"Добавили ≥1 товар: {item_users}\n"
        f"Уведомления включены: {notif}\n"
        f"Activation: {act}\n"
        f"Start → Activation: {s2a:.1f}%"
    )


def metrika_debug_text():
    with connect_db() as conn, conn.cursor() as cur:
        queries = {
            "bridge_clicks": "SELECT COUNT(*) AS n FROM ad_clicks",
            "clicks_with_yclid": "SELECT COUNT(*) AS n FROM ad_clicks WHERE yclid IS NOT NULL AND yclid <> ''",
            "linked_starts": "SELECT COUNT(*) AS n FROM ad_clicks WHERE started_at IS NOT NULL",
            "starts_with_yclid": "SELECT COUNT(*) AS n FROM ad_clicks WHERE started_at IS NOT NULL AND yclid IS NOT NULL AND yclid <> ''",
            "attempts": "SELECT COUNT(*) AS n FROM ad_clicks WHERE metrika_attempted_at IS NOT NULL",
            "accepted": "SELECT COUNT(*) AS n FROM ad_clicks WHERE metrika_sent=TRUE",
            "processed": "SELECT COUNT(*) AS n FROM ad_clicks WHERE metrika_upload_status='PROCESSED'",
            "linkage_failures": "SELECT COUNT(*) AS n FROM ad_clicks WHERE metrika_upload_status='LINKAGE_FAILURE'",
            "errors": "SELECT COUNT(*) AS n FROM ad_clicks WHERE metrika_error IS NOT NULL AND metrika_error <> ''",
            "pending": "SELECT COUNT(*) AS n FROM ad_clicks WHERE metrika_upload_id IS NOT NULL AND COALESCE(metrika_upload_status,'') NOT IN ('PROCESSED','LINKAGE_FAILURE')",
        }
        values = {}
        for key, query in queries.items():
            cur.execute(query)
            values[key] = cur.fetchone()["n"]

        cur.execute(
            """
            SELECT token,source,metrika_upload_status,metrika_http_status,metrika_error
            FROM ad_clicks
            WHERE metrika_error IS NOT NULL AND metrika_error <> ''
            ORDER BY COALESCE(metrika_last_checked_at,metrika_attempted_at,clicked_at) DESC
            LIMIT 5
            """
        )
        recent_errors = cur.fetchall()

    lines = [
        "🧪 Metrika diagnostics",
        "",
        f"Bridge clicks: {values['bridge_clicks']}",
        f"Clicks with yclid: {values['clicks_with_yclid']}",
        f"Telegram Starts linked: {values['linked_starts']}",
        f"Starts with yclid: {values['starts_with_yclid']}",
        "",
        f"Upload attempts: {values['attempts']}",
        f"Uploads accepted: {values['accepted']}",
        f"Processed: {values['processed']}",
        f"Linkage failures: {values['linkage_failures']}",
        f"Errors: {values['errors']}",
        f"Pending: {values['pending']}",
    ]
    if recent_errors:
        lines.extend(["", "Последние ошибки:"])
        for row in recent_errors:
            error = clean_log_value(row.get("metrika_error"), 220)
            lines.append(
                f"• {row['token']} | {row['source']} | "
                f"{row.get('metrika_upload_status') or '-'} | "
                f"HTTP {row.get('metrika_http_status') or '-'} | {error}"
            )
    return "\n".join(lines)[:4000]


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
    raw_text = message.text or ""
    parts = raw_text.split(maxsplit=1)
    has_payload = len(parts) > 1 and bool(parts[1].strip())
    payload = parts[1].strip() if has_payload else None

    logger.info(
        "ATTRIBUTION | START_RECEIVED | user_id=%s | raw=%s | payload=%s",
        message.from_user.id,
        clean_log_value(raw_text, 500),
        clean_log_value(payload, 300),
    )

    source = "direct"
    click_token = None
    click = None

    if not payload:
        logger.warning(
            "ATTRIBUTION | UNATTRIBUTED_START | reason=no_payload | user_id=%s",
            message.from_user.id,
        )
        log_recent_unbound_clicks("no_payload", message.from_user.id)
    elif TOKEN_RE.fullmatch(payload):
        logger.info(
            "ATTRIBUTION | TOKEN_VALID | user_id=%s | token=%s",
            message.from_user.id,
            payload,
        )
        click = get_click(payload)
        if click:
            source = click["source"]
            click_token = payload
            logger.info(
                "ATTRIBUTION | CLICK_FOUND | user_id=%s | token=%s | source=%s | "
                "yclid=%s | clicked_at=%s",
                message.from_user.id,
                payload,
                source,
                clean_log_value(click.get("yclid"), 300),
                click.get("clicked_at"),
            )
        else:
            logger.error(
                "ATTRIBUTION | CLICK_NOT_FOUND | user_id=%s | token=%s",
                message.from_user.id,
                payload,
            )
            log_recent_unbound_clicks("token_not_found", message.from_user.id)
    else:
        source = normalize_source(payload)
        logger.warning(
            "ATTRIBUTION | UNATTRIBUTED_START | reason=not_bridge_token | "
            "user_id=%s | payload=%s | source=%s",
            message.from_user.id,
            clean_log_value(payload, 300),
            source,
        )

    register_start(message, source, click_token)

    if click_token:
        updated = bind_click(click_token, message.from_user.id)
        logger.info(
            "ATTRIBUTION | CLICK_BOUND | user_id=%s | token=%s | updated_rows=%s",
            message.from_user.id,
            click_token,
            updated,
        )
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

    if click:
        spawn_task(
            send_metrika_conversion(
                click,
                user_id=message.from_user.id,
                payload=payload,
            )
        )
    elif payload and TOKEN_RE.fullmatch(payload):
        logger.warning(
            "METRIKA | SKIP | reason=token_not_found | user_id=%s | payload=%s",
            message.from_user.id,
            payload,
        )
    elif not payload:
        logger.info(
            "METRIKA | SKIP | reason=no_start_payload | user_id=%s",
            message.from_user.id,
        )
    else:
        logger.info(
            "METRIKA | SKIP | reason=not_bridge_start | user_id=%s | payload=%s",
            message.from_user.id,
            clean_log_value(payload, 300),
        )


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
        f"Status polling: {METRIKA_STATUS_POLL_SECONDS}s\n"
        f"Port: {PORT}"
    )


@router.message(Command("metrika_debug"))
async def cmd_metrika_debug(message: Message):
    if not is_admin(message.from_user.id):
        return
    await message.answer(metrika_debug_text())


@router.message(Command("metrika_check"))
async def cmd_metrika_check(message: Message):
    if not is_admin(message.from_user.id):
        return

    pending = get_pending_metrika_uploads(limit=50)
    if not pending:
        await message.answer("Нет ожидающих проверку загрузок Метрики.\n\n" + metrika_debug_text())
        return

    await message.answer(f"Проверяю статусы Метрики: {len(pending)}")
    for row in pending:
        await check_metrika_upload_status(row["token"], row["metrika_upload_id"])
        await asyncio.sleep(0.2)
    await message.answer(metrika_debug_text())


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

    logger.info(
        "STARTUP | Beauty Bot + Postgres + Yandex bridge started | "
        "bot_username=%s | counter_id=%s | goal_id=%s | metrika_oauth_set=%s | port=%s",
        BOT_USERNAME,
        METRIKA_COUNTER_ID or "-",
        METRIKA_GOAL_ID or "-",
        bool(METRIKA_OAUTH_TOKEN),
        PORT,
    )
    monitor_task = spawn_task(metrika_monitor_loop())

    try:
        await dp.start_polling(bot)
    finally:
        monitor_task.cancel()
        await web_runner.cleanup()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
