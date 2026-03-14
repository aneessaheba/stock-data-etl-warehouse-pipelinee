"""
Kafka Consumer — Write to TimescaleDB
======================================
Reads messages from the `stock-data` Kafka topic published by producer.py
and upserts them into fact_stock_price_daily in TimescaleDB.

Runs as a Docker container (see Dockerfile.consumer).
"""

import json
import time
import psycopg2
from kafka import KafkaConsumer

KAFKA_BROKER = "stock-data-platform-kafka:9092"
TOPIC        = "stock-data"
GROUP_ID     = "stock-data-consumer"

DB_CONN = dict(
    host="timescaledb",
    dbname="stockdw",
    user="data226",
    password="12345678",
    port=5432,
)

# ── Wait for Kafka ─────────────────────────────────────────────────────────

def _connect_kafka() -> KafkaConsumer:
    time.sleep(10)  # Kafka needs a few seconds after startup
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=[KAFKA_BROKER],
                auto_offset_reset="earliest",
                group_id=GROUP_ID,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
            print("✅ Connected to Kafka broker.")
            return consumer
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


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    consumer = _connect_kafka()
    conn     = _connect_db()
    cur      = conn.cursor()

    print(f"👂 Listening on Kafka topic '{TOPIC}' …")

    for message in consumer:
        data = message.value
        try:
            cur.execute(
                """
                INSERT INTO fact_stock_price_daily
                    (date, company_key, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (date, company_key) DO UPDATE SET
                    open   = EXCLUDED.open,
                    high   = EXCLUDED.high,
                    low    = EXCLUDED.low,
                    close  = EXCLUDED.close,
                    volume = EXCLUDED.volume;
                """,
                (
                    data["date"],
                    data["company_key"],
                    data["open"],
                    data["high"],
                    data["low"],
                    data["close"],
                    data["volume"],
                ),
            )
            conn.commit()
            print(f"✅ Inserted {data['ticker']} @ {data['date']} close={data['close']:.2f}")

        except Exception as exc:
            conn.rollback()
            print(f"❌ Insert failed for {data}: {exc}")
            # Reconnect on connection errors
            try:
                conn.close()
            except Exception:
                pass
            conn = _connect_db()
            cur  = conn.cursor()

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
