"""
Kafka Producer — Live Stock Data
=================================
Publishes real-time 1-minute OHLCV snapshots to the Kafka topic `stock-data`
every 15 seconds via yfinance. Also computes daily_return_pct and
intraday_range so the consumer can write them directly without recalculating.

Supported tickers: all symbols in TICKERS list below.
Runs as a Docker container (see Dockerfile.producer).
"""

import json
import os
import time

import psycopg2
from kafka import KafkaProducer
from yfinance import Ticker
from yfinance.exceptions import YFRateLimitError

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "stock-data-platform-kafka:9092")
TOPIC        = "stock-data"
POLL_SECS    = 15

TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NFLX", "NVDA", "INTC"]

# Read DB credentials from environment — injected by docker-compose.yml
DB_CONN = dict(
    host=os.getenv("DB_HOST",     "timescaledb"),
    dbname=os.getenv("DB_NAME",   "stockdw"),
    user=os.getenv("DB_USER",     "data226"),
    password=os.getenv("DB_PASSWORD", "12345678"),
    port=int(os.getenv("DB_PORT", "5432")),
)


# ── Wait for Kafka ─────────────────────────────────────────────────────────

def _connect_kafka() -> KafkaProducer:
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print("✅ Connected to Kafka broker.")
            return producer
        except Exception as exc:
            print(f"⏳ Kafka not ready, retrying in 5s … ({exc})")
            time.sleep(5)


# ── Wait for TimescaleDB ───────────────────────────────────────────────────

def _connect_db():
    while True:
        try:
            conn = psycopg2.connect(**DB_CONN)
            print("✅ Connected to TimescaleDB.")
            return conn
        except Exception as exc:
            print(f"⏳ DB not ready, retrying in 5s … ({exc})")
            time.sleep(5)


def _build_company_keys(conn) -> dict[str, int]:
    """
    Query dim_company once at startup and return ticker→company_key mapping.
    Retries until all tickers are present (waits for populate_dim_company DAG).
    Uses a single persistent connection — no new connection per ticker.
    """
    cur = conn.cursor()
    company_keys: dict[str, int] = {}
    missing = list(TICKERS)

    while missing:
        for ticker in list(missing):
            cur.execute(
                "SELECT company_key FROM dim_company WHERE ticker=%s AND is_current=TRUE",
                (ticker,),
            )
            row = cur.fetchone()
            if row:
                company_keys[ticker] = row[0]
                missing.remove(ticker)
                print(f"  company_key for {ticker}: {row[0]}")

        if missing:
            print(f"⏳ Waiting for dim_company entries: {missing} — retrying in 10s …")
            time.sleep(10)

    cur.close()
    return company_keys


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    producer  = _connect_kafka()
    db_conn   = _connect_db()
    company_keys = _build_company_keys(db_conn)
    db_conn.close()  # No longer needed after the initial key lookup

    print(f"✅ All company keys loaded. Starting live stream for {TICKERS}")

    while True:
        for ticker in TICKERS:
            try:
                data = Ticker(ticker).history(period="1d", interval="1m").tail(1)
                if data.empty:
                    print(f"⏭️  No market data yet for {ticker}")
                    continue

                last_ts = data.index[-1]
                open_p  = float(data["Open"].iloc[-1])
                high_p  = float(data["High"].iloc[-1])
                low_p   = float(data["Low"].iloc[-1])
                close_p = float(data["Close"].iloc[-1])

                payload = {
                    "company_key":      company_keys[ticker],
                    "ticker":           ticker,
                    "date":             last_ts.date().isoformat(),
                    "open":             open_p,
                    "high":             high_p,
                    "low":              low_p,
                    "close":            close_p,
                    "volume":           int(data["Volume"].iloc[-1]),
                    # Derived cols computed here so consumer writes them directly
                    "daily_return_pct": round((close_p - open_p) / open_p * 100, 4)
                                        if open_p else 0.0,
                    "intraday_range":   round(high_p - low_p, 4),
                }
                producer.send(TOPIC, value=payload)
                print(f"📤 Sent {ticker}: close={close_p:.2f}")

            except YFRateLimitError:
                print(f"⏳ Rate-limited for {ticker}. Backing off 60s …")
                time.sleep(60)
            except Exception as exc:
                print(f"❌ Error streaming {ticker}: {exc}")

        time.sleep(POLL_SECS)


if __name__ == "__main__":
    main()
