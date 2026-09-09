"""
Beauty Bot Phase 1 — PostgreSQL statistics.

Запуск:
    python stats.py

Использует DATABASE_URL.
"""

import os

import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()


def connect_db():
    if not DATABASE_URL:
        raise SystemExit("Не задан DATABASE_URL.")
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def pct(num: int, den: int) -> str:
    return f"{(num / den * 100):.1f}%" if den else "—"


def main():
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM users")
            total = cur.fetchone()["n"]

            if not total:
                print("Пока нет ни одной регистрации.")
                return

            cur.execute(
                """
                SELECT COUNT(*) AS n
                FROM users
                WHERE activated_at IS NOT NULL
                """
            )
            activated = cur.fetchone()["n"]

            print("\nBEAUTY BOT — PHASE 1 / POSTGRESQL\n")
            print(f"Регистраций: {total}")
            print(f"Активированных: {activated}")
            print(f"Start → Activation: {pct(activated, total)}")

            print("\nПО ПЕРВОМУ ИСТОЧНИКУ\n")

            cur.execute(
                """
                SELECT
                    u.first_source AS source,
                    COUNT(*) AS registrations,

                    COUNT(*) FILTER (
                        WHERE EXISTS (
                            SELECT 1
                            FROM events e
                            WHERE e.user_id = u.user_id
                              AND e.event_name = 'open_wishlist'
                        )
                    ) AS opened_wishlist,

                    COUNT(*) FILTER (
                        WHERE EXISTS (
                            SELECT 1
                            FROM events e
                            WHERE e.user_id = u.user_id
                              AND e.event_name = 'open_polka'
                        )
                    ) AS opened_polka,

                    COUNT(*) FILTER (
                        WHERE EXISTS (
                            SELECT 1
                            FROM items i
                            WHERE i.user_id = u.user_id
                        )
                    ) AS added_item,

                    COUNT(*) FILTER (
                        WHERE u.notifications = TRUE
                    ) AS notifications_on,

                    COUNT(*) FILTER (
                        WHERE u.activated_at IS NOT NULL
                    ) AS activated

                FROM users u
                GROUP BY u.first_source
                ORDER BY registrations DESC
                """
            )
            rows = cur.fetchall()

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

            cur.execute(
                """
                SELECT
                    source,
                    COUNT(*) AS starts_count,
                    COUNT(DISTINCT user_id) AS unique_users
                FROM starts
                GROUP BY source
                ORDER BY starts_count DESC
                """
            )

            for row in cur.fetchall():
                print(
                    f"{row['source']}: "
                    f"{row['starts_count']} start / "
                    f"{row['unique_users']} unique users"
                )


if __name__ == "__main__":
    main()
