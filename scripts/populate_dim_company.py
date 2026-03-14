"""
Seed dim_company with all 9 tracked NASDAQ stocks.
Called by the populate_dim_company Airflow DAG (or run standalone).
"""

import os

import psycopg2

# Read DB credentials from environment — set by docker-compose.yml or a local .env
DB_CONN = dict(
    host=os.getenv("DB_HOST",     "timescaledb"),
    dbname=os.getenv("DB_NAME",   "stockdw"),
    user=os.getenv("DB_USER",     "data226"),
    password=os.getenv("DB_PASSWORD", "12345678"),
    port=int(os.getenv("DB_PORT", "5432")),
)

COMPANIES = [
    {
        "ticker": "AAPL", "company_name": "Apple Inc",
        "sector": "Technology", "industry": "Electronic Computers",
        "exchange": "NASDAQ",
    },
    {
        "ticker": "MSFT", "company_name": "Microsoft Corporation",
        "sector": "Technology", "industry": "Services-Prepackaged Software",
        "exchange": "NASDAQ",
    },
    {
        "ticker": "GOOGL", "company_name": "Alphabet Inc Class A",
        "sector": "Technology", "industry": "Services-Computer Programming",
        "exchange": "NASDAQ",
    },
    {
        "ticker": "AMZN", "company_name": "Amazon.com Inc",
        "sector": "Trade & Services", "industry": "Retail-Catalog & Mail-Order Houses",
        "exchange": "NASDAQ",
    },
    {
        "ticker": "TSLA", "company_name": "Tesla Inc",
        "sector": "Manufacturing", "industry": "Motor Vehicles & Passenger Car Bodies",
        "exchange": "NASDAQ",
    },
    {
        "ticker": "META", "company_name": "Meta Platforms Inc",
        "sector": "Technology", "industry": "Services-Computer Programming",
        "exchange": "NASDAQ",
    },
    {
        "ticker": "NFLX", "company_name": "Netflix Inc",
        "sector": "Trade & Services", "industry": "Services-Video Tape Rental",
        "exchange": "NASDAQ",
    },
    {
        "ticker": "NVDA", "company_name": "NVIDIA Corporation",
        "sector": "Manufacturing", "industry": "Semiconductors & Related Devices",
        "exchange": "NASDAQ",
    },
    {
        "ticker": "INTC", "company_name": "Intel Corporation",
        "sector": "Manufacturing", "industry": "Semiconductors & Related Devices",
        "exchange": "NASDAQ",
    },
]


def populate_dim_company():
    conn = psycopg2.connect(**DB_CONN)
    cur = conn.cursor()

    for c in COMPANIES:
        cur.execute(
            """
            INSERT INTO dim_company (ticker, company_name, sector, industry, exchange, is_current, effective_date)
            VALUES (%s, %s, %s, %s, %s, TRUE, CURRENT_DATE)
            ON CONFLICT (ticker) DO NOTHING;
            """,
            (c["ticker"], c["company_name"], c["sector"], c["industry"], c["exchange"]),
        )

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ dim_company seeded with {len(COMPANIES)} companies.")


if __name__ == "__main__":
    populate_dim_company()
