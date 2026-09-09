"""
Подробная статистика Beauty Bot Phase 1.

Запуск:
    python stats.py

База берётся из DB_PATH или из beauty_bot.db по умолчанию.
"""

import os
import sqlite3
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.getenv("DB_PATH", "beauty_bot.db")


def pct(num: int, den: int) -> str:
    return f"{(num / den * 100):.1f}%" if den else "—"


def main():
    if not Path(DB_PATH).exists():
        print(f"База {DB_PATH} не найдена.")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    total = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    if not total:
        print("Пока нет ни одной регистрации.")
        conn.close()
        return

    activated = conn.execute(
        "SELECT COUNT(*) FROM users WHERE activated_at IS NOT NULL"
    ).fetchone()[0]

    print("\nBEAUTY BOT — PHASE 1\n")
    print(f"Регистраций: {total}")
    print(f"Активированных: {activated}")
    print(f"Start → Activation: {pct(activated, total)}")

    print("\nПО ПЕРВОМУ ИСТОЧНИКУ\n")

    rows = conn.execute(
        """
        SELECT
            u.first_source AS source,
            COUNT(*) AS registrations,

            SUM(
                CASE WHEN EXISTS (
                    SELECT 1 FROM events e
                    WHERE e.user_id = u.user_id
                      AND e.event_name = 'open_wishlist'
                ) THEN 1 ELSE 0 END
            ) AS opened_wishlist,

            SUM(
                CASE WHEN EXISTS (
                    SELECT 1 FROM events e
                    WHERE e.user_id = u.user_id
                      AND e.event_name = 'open_polka'
                ) THEN 1 ELSE 0 END
            ) AS opened_polka,

            SUM(
                CASE WHEN EXISTS (
                    SELECT 1 FROM items i
                    WHERE i.user_id = u.user_id
                ) THEN 1 ELSE 0 END
            ) AS added_item,

            SUM(
                CASE WHEN u.notifications = 1
                    THEN 1 ELSE 0 END
            ) AS notifications_on,

            SUM(
                CASE WHEN u.activated_at IS NOT NULL
                    THEN 1 ELSE 0 END
            ) AS activated

        FROM users u
        GROUP BY u.first_source
        ORDER BY registrations DESC
        """
    ).fetchall()

    header = (
        f"{'Источник':<28}"
        f"{'Start':>7}"
        f"{'Wish':>7}"
        f"{'Polka':>7}"
        f"{'Item':>7}"
        f"{'Notif':>7}"
        f"{'Act':>7}"
        f"{'Conv':>9}"
    )
    print(header)
    print("-" * len(header))

    for row in rows:
        print(
            f"{row['source']:<28}"
            f"{row['registrations']:>7}"
            f"{row['opened_wishlist']:>7}"
            f"{row['opened_polka']:>7}"
            f"{row['added_item']:>7}"
            f"{row['notifications_on']:>7}"
            f"{row['activated']:>7}"
            f"{pct(row['activated'], row['registrations']):>9}"
        )

    print("\nПОВТОРНЫЕ START / РЕКЛАМНЫЕ КАСАНИЯ\n")

    starts = conn.execute(
        """
        SELECT source, COUNT(*) AS starts_count,
               COUNT(DISTINCT user_id) AS unique_users
        FROM starts
        GROUP BY source
        ORDER BY starts_count DESC
        """
    ).fetchall()

    for row in starts:
        print(
            f"{row['source']}: "
            f"{row['starts_count']} start / "
            f"{row['unique_users']} unique users"
        )

    conn.close()


if __name__ == "__main__":
    main()
