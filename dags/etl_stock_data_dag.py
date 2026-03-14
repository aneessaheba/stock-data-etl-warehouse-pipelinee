"""
Airflow ETL DAG — Stock Market Data Pipeline
=============================================
Dynamically creates one DAG per ticker defined in tickers.txt.

DAG flow per ticker:
  extract → transform → load → trigger_csv_export

Schedule: daily
DB: TimescaleDB (PostgreSQL-compatible)
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import json
import os
import gzip
import psycopg2

TICKERS_FILE = "/opt/airflow/dags/tickers.txt"
with open(TICKERS_FILE, "r") as f:
    TICKERS = [line.strip() for line in f if line.strip()]

DB_CONN = dict(
    host="timescaledb",
    dbname="stockdw",
    user="data226",
    password="12345678",
    port=5432,
)

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


# ─────────────────────────────────────────────────────────────────────────────
# Helper: staged file path per (ticker, stage, run)
# ─────────────────────────────────────────────────────────────────────────────

def _stage_path(ticker, stage, run_suffix):
    base = "/tmp/stock_data_platform"
    os.makedirs(base, exist_ok=True)
    return os.path.join(base, f"{ticker.lower()}_{stage}_{run_suffix}.json.gz")


def _normalize_cols(df, ticker):
    """Normalise multi-index yfinance columns → Open_{ticker}, Close_{ticker}, …"""
    df = df.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ["_".join(c) for c in df.columns]
    expected = [f"Open_{ticker}", f"High_{ticker}", f"Low_{ticker}",
                f"Close_{ticker}", f"Volume_{ticker}"]
    if not all(c in df.columns for c in expected):
        rename = {c: f"{c}_{ticker}" for c in ["Open", "High", "Low", "Close", "Volume"]
                  if c in df.columns}
        df = df.rename(columns=rename)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Extract
# ─────────────────────────────────────────────────────────────────────────────

def extract_data(ticker, ti):
    end_date = datetime.today()
    start_date = end_date - timedelta(days=365 * 25)
    df = yf.download(ticker, start=start_date, end=end_date, progress=False)
    if df.empty:
        raise ValueError(f"No data downloaded for {ticker}")

    run_suffix = ti.ts_nodash or datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    path = _stage_path(ticker, "raw", run_suffix)
    with gzip.open(path, "wt", encoding="utf-8") as f:
        f.write(df.to_json(orient="split"))
    ti.xcom_push(key="raw_path", value=path)
    print(f"✅ Extracted {len(df):,} rows for {ticker}")


# ─────────────────────────────────────────────────────────────────────────────
# Transform
# ─────────────────────────────────────────────────────────────────────────────

def transform_data(ticker, ti):
    raw_path = ti.xcom_pull(key="raw_path", task_ids=f"{ticker}_extract")
    df = pd.read_json(raw_path, orient="split", compression="gzip")
    df = _normalize_cols(df, ticker)

    required = [f"Open_{ticker}", f"High_{ticker}", f"Low_{ticker}",
                f"Close_{ticker}", f"Volume_{ticker}"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns after normalisation for {ticker}: {missing}")

    df.dropna(inplace=True)
    df = df[df[f"Volume_{ticker}"] > 0]
    df.index = pd.to_datetime(df.index)

    # Derived metrics
    df[f"Daily_Return_Pct_{ticker}"] = (
        (df[f"Close_{ticker}"] - df[f"Open_{ticker}"]) / df[f"Open_{ticker}"] * 100
    ).round(4)
    df[f"Intraday_Range_{ticker}"] = (
        df[f"High_{ticker}"] - df[f"Low_{ticker}"]
    ).round(4)

    run_suffix = ti.ts_nodash or datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    path = _stage_path(ticker, "cleaned", run_suffix)
    with gzip.open(path, "wt", encoding="utf-8") as f:
        f.write(df.to_json(orient="split"))
    ti.xcom_push(key="cleaned_path", value=path)
    print(f"✅ Transformed {len(df):,} rows for {ticker}")


# ─────────────────────────────────────────────────────────────────────────────
# Load
# ─────────────────────────────────────────────────────────────────────────────

def load_data(ticker, ti):
    cleaned_path = ti.xcom_pull(key="cleaned_path", task_ids=f"{ticker}_transform")
    df = pd.read_json(cleaned_path, orient="split", compression="gzip")

    conn = psycopg2.connect(**DB_CONN)
    cur = conn.cursor()

    # Ensure company exists in dim_company
    cur.execute("SELECT company_key FROM dim_company WHERE ticker=%s AND is_current=TRUE", (ticker,))
    row = cur.fetchone()
    if row:
        company_key = row[0]
    else:
        cur.execute(
            "INSERT INTO dim_company (ticker, company_name, is_current, effective_date)"
            " VALUES (%s, %s, TRUE, CURRENT_DATE) RETURNING company_key",
            (ticker, ticker),
        )
        company_key = cur.fetchone()[0]
        conn.commit()

    # Bulk upsert into fact_stock_price_daily
    rows_loaded = 0
    for idx, row in df.iterrows():
        date = pd.to_datetime(idx).date()
        cur.execute(
            """
            INSERT INTO fact_stock_price_daily
                (date, company_key, open, high, low, close, volume, daily_return_pct, intraday_range)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date, company_key) DO UPDATE SET
                open              = EXCLUDED.open,
                high              = EXCLUDED.high,
                low               = EXCLUDED.low,
                close             = EXCLUDED.close,
                volume            = EXCLUDED.volume,
                daily_return_pct  = EXCLUDED.daily_return_pct,
                intraday_range    = EXCLUDED.intraday_range;
            """,
            (
                date, company_key,
                float(row[f"Open_{ticker}"]),
                float(row[f"High_{ticker}"]),
                float(row[f"Low_{ticker}"]),
                float(row[f"Close_{ticker}"]),
                int(row[f"Volume_{ticker}"]),
                float(row.get(f"Daily_Return_Pct_{ticker}", 0)),
                float(row.get(f"Intraday_Range_{ticker}", 0)),
            ),
        )
        rows_loaded += 1

    conn.commit()
    cur.close()
    conn.close()

    # Cleanup temp files
    for path in (
        ti.xcom_pull(key="raw_path", task_ids=f"{ticker}_extract"),
        cleaned_path,
    ):
        if path and os.path.exists(path):
            os.remove(path)

    print(f"✅ Loaded {rows_loaded:,} rows for {ticker}")


# ─────────────────────────────────────────────────────────────────────────────
# CSV Export (triggered after all ETL DAGs complete)
# ─────────────────────────────────────────────────────────────────────────────

def export_30_day_csvs():
    conn = psycopg2.connect(**DB_CONN)
    end_date = datetime.today().date()
    start_date = end_date - timedelta(days=30)
    output_dir = "/opt/airflow/dags/stock_csvs"
    os.makedirs(output_dir, exist_ok=True)

    with open(TICKERS_FILE) as f:
        tickers = [l.strip() for l in f if l.strip()]

    for ticker in tickers:
        query = """
            SELECT f.date, d.ticker, f.open, f.high, f.low, f.close, f.volume,
                   f.daily_return_pct, f.intraday_range
            FROM fact_stock_price_daily f
            JOIN dim_company d ON f.company_key = d.company_key
            WHERE d.ticker = %s AND f.date BETWEEN %s AND %s
            ORDER BY f.date DESC;
        """
        df = pd.read_sql(query, conn, params=(ticker, start_date, end_date))
        df.to_csv(os.path.join(output_dir, f"{ticker}_last_30_days.csv"), index=False)
        print(f"  Exported {len(df)} rows → {ticker}_last_30_days.csv")

    conn.close()
    print("✅ All 30-day CSVs updated.")


# ─────────────────────────────────────────────────────────────────────────────
# Dynamic DAG creation — one per ticker
# ─────────────────────────────────────────────────────────────────────────────

for ticker in TICKERS:
    with DAG(
        dag_id=f"etl_stock_data_{ticker.lower()}",
        default_args=default_args,
        schedule_interval="@daily",
        start_date=datetime(2025, 1, 1),
        catchup=False,
        tags=["stock", "ETL"],
        description=f"Daily ETL pipeline for {ticker}",
    ) as dag:

        extract = PythonOperator(
            task_id=f"{ticker}_extract",
            python_callable=extract_data,
            op_kwargs={"ticker": ticker},
        )
        transform = PythonOperator(
            task_id=f"{ticker}_transform",
            python_callable=transform_data,
            op_kwargs={"ticker": ticker},
        )
        load = PythonOperator(
            task_id=f"{ticker}_load",
            python_callable=load_data,
            op_kwargs={"ticker": ticker},
        )
        trigger_export = TriggerDagRunOperator(
            task_id=f"{ticker}_trigger_csv_export",
            trigger_dag_id="csv_export_dag",
            wait_for_completion=False,
            reset_dag_run=False,
        )

        extract >> transform >> load >> trigger_export

    globals()[f"etl_stock_data_{ticker.lower()}"] = dag


# ─────────────────────────────────────────────────────────────────────────────
# Shared CSV export DAG (triggered, not scheduled)
# ─────────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="csv_export_dag",
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["stock", "CSV"],
    description="Exports last 30 days of prices per ticker to CSV",
) as export_dag:
    export_task = PythonOperator(
        task_id="export_30_day_csvs",
        python_callable=export_30_day_csvs,
    )

globals()["csv_export_dag"] = export_dag
