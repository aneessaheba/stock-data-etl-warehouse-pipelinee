"""
Seed dim_date with every calendar day from 1980-01-01 to 2035-12-31.
Covers the full historical price range (AAPL starts 1980) and future dates.
Called by the populate_dim_date Airflow DAG (or run standalone).
"""

import psycopg2
from datetime import date, timedelta

START_DATE = date(1980, 1, 1)
END_DATE   = date(2035, 12, 31)

DB_CONN = dict(
    host="timescaledb",
    dbname="stockdw",
    user="data226",
    password="12345678",
    port=5432,
)


def generate_dates():
    conn = psycopg2.connect(**DB_CONN)
    cur = conn.cursor()

    current = START_DATE
    inserted = 0
    while current <= END_DATE:
        year      = current.year
        quarter   = (current.month - 1) // 3 + 1
        month     = current.month
        day       = current.day
        dow       = current.weekday()        # 0=Mon … 6=Sun
        is_weekend = dow >= 5

        cur.execute(
            """
            INSERT INTO dim_date (date, year, quarter, month, day_of_month, day_of_week, is_weekend)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date) DO NOTHING;
            """,
            (current, year, quarter, month, day, dow, is_weekend),
        )
        inserted += 1
        current += timedelta(days=1)

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ dim_date seeded: {inserted:,} days from {START_DATE} to {END_DATE}.")


if __name__ == "__main__":
    generate_dates()
