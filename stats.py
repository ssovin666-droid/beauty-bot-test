"""
Считает метрики по каждому источнику (значению source из UTM-ссылки) —
именно то, что нужно для сравнения гипотез из плана теста:
регистрации, активные пользователи, конверсия регистрация -> активный.

Запуск:  python stats.py
(CAC добавляешь сама вручную — раздели потраченный бюджет на канал на
 число регистраций по этому источнику, бот трат не знает)
"""

import os
import sqlite3

DB_PATH = os.environ.get("DB_PATH", "beauty_bot.db")


def main():
    if not os.path.exists(DB_PATH):
        print(f"Файл {DB_PATH} не найден — бот ещё не запускался или не в этой папке.")
        return

    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("""
        SELECT source,
               COUNT(*) AS registrations,
               SUM(CASE WHEN activated_at IS NOT NULL THEN 1 ELSE 0 END) AS activated
        FROM users
        GROUP BY source
        ORDER BY registrations DESC
    """).fetchall()
    conn.close()

    if not rows:
        print("Пока нет ни одной регистрации.")
        return

    print(f"{'Источник':<20}{'Регистраций':<14}{'Активных':<11}{'Конверсия':<10}")
    print("-" * 55)
    total_reg, total_act = 0, 0
    for source, reg, act in rows:
        conv = f"{(act / reg * 100):.1f}%" if reg else "—"
        print(f"{source:<20}{reg:<14}{act:<11}{conv:<10}")
        total_reg += reg
        total_act += act

    print("-" * 55)
    conv_total = f"{(total_act / total_reg * 100):.1f}%" if total_reg else "—"
    print(f"{'ИТОГО':<20}{total_reg:<14}{total_act:<11}{conv_total:<10}")
    print("\nCAC по источнику = потраченный бюджет на канал / число регистраций (посчитай отдельно).")
    print("cost per active user = тот же бюджет / число активных — так сравниваешь ветки между собой.")


if __name__ == "__main__":
    main()
