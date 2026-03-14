"""
Kafka Producer — Live Stock Data
=================================
Publishes real-time 1-minute OHLCV snapshots to the Kafka topic `stock-data`
every 15 seconds via yfinance.

Supported tickers: all symbols in TICKERS list below.
Runs as a Docker container (see Dockerfile.producer).
"""

import json
import time
import psycopg2
from kafka import KafkaProducer
from yfinance import Ticker
from yfinance.exceptions import YFRateLimitError

KAFKA_BROKER = "stock-data-platform-kafka:9092"
TOPIC        = "stock-data"
POLL_SECS    = 15

# Tickers to stream live — extend as needed
TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NFLX", "NVDA", "INTC"]

DB_CONN = dict(
    host="timescaledb",
    dbname="stockdw",
    user="data226",
    password="12345678",
    port=5432,
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


def _get_company_key(ticker: str) -> int | None:
    conn = _connect_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT company_key FROM dim_company WHERE ticker=%s AND is_current=TRUE",
        (ticker,),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row[0] if row else None


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    producer = _connect_kafka()

    # Build company_key map — wait until all tickers are seeded
    company_keys: dict[str, int] = {}
    while len(company_keys) < len(TICKERS):
        missing = [t for t in TICKERS if t not in company_keys]
        for ticker in missing:
            key = _get_company_key(ticker)
            if key is not None:
                company_keys[ticker] = key
                print(f"  company_key for {ticker}: {key}")
        if len(company_keys) < len(TICKERS):
            still_missing = [t for t in TICKERS if t not in company_keys]
            print(f"⏳ Waiting for dim_company entries: {still_missing} — retrying in 10s …")
            time.sleep(10)

    print(f"✅ All company keys loaded. Starting live stream for {TICKERS}")

    while True:
        for ticker in TICKERS:
            try:
                data = Ticker(ticker).history(period="1d", interval="1m").tail(1)
                if data.empty:
                    print(f"⏭️  No market data yet for {ticker}")
                    continue

                last_ts = data.index[-1]
                payload = {
                    "company_key": company_keys[ticker],
                    "ticker":      ticker,
                    "date":        last_ts.date().isoformat(),
                    "open":        float(data["Open"].iloc[-1]),
                    "high":        float(data["High"].iloc[-1]),
                    "low":         float(data["Low"].iloc[-1]),
                    "close":       float(data["Close"].iloc[-1]),
                    "volume":      int(data["Volume"].iloc[-1]),
                }
                producer.send(TOPIC, value=payload)
                print(f"📤 Sent {ticker}: close={payload['close']:.2f}")

            except YFRateLimitError:
                print(f"⏳ Rate-limited for {ticker}. Backing off 60s …")
                time.sleep(60)
            except Exception as exc:
                print(f"❌ Error streaming {ticker}: {exc}")

        time.sleep(POLL_SECS)


if __name__ == "__main__":
    main()
