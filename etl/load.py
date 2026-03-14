"""
Load Layer
==========
Exports transformed DataFrames to:
  1. CSV files in data/  (always)
  2. PostgreSQL / TimescaleDB tables (when DB connection is available)

Schema matches sql/schema.sql exactly:
  - dim_company        : company_key SERIAL PK, ticker TEXT UNIQUE
  - dim_date           : date PK (7 columns, 0-based weekday)
  - dim_financials     : (ticker, year) PK
  - fact_stock_price_daily : (date, company_key) PK — TimescaleDB hypertable
"""

import logging
import os
from datetime import date
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent.parent / "data"


# ---------------------------------------------------------------------------
# CSV export  (uses original DataFrame column names — unchanged)
# ---------------------------------------------------------------------------

def export_to_csv(
    dim_company: pd.DataFrame,
    dim_date: pd.DataFrame,
    dim_financials: pd.DataFrame,
    fact_stock_prices: pd.DataFrame,
    out_dir: Path = DATA_DIR,
) -> None:
    """Write all four tables to CSV files."""
    out_dir.mkdir(parents=True, exist_ok=True)

    files = {
        "dim_company.csv":       dim_company,
        "dim_date.csv":          dim_date,
        "dim_financials.csv":    dim_financials,
        "fact_stock_prices.csv": fact_stock_prices,
    }
    for filename, df in files.items():
        path = out_dir / filename
        df.to_csv(path, index=False)
        logger.info("Exported %d rows → %s", len(df), path)


# ---------------------------------------------------------------------------
# Database engine
# ---------------------------------------------------------------------------

def _get_engine():
    """Build SQLAlchemy engine from environment variables.

    Defaults match docker-compose.yml (TimescaleDB service).
    Override via .env or shell environment for local runs:
        DB_HOST=localhost DB_USER=data226 DB_PASSWORD=12345678 DB_NAME=stockdw
    """
    host     = os.getenv("DB_HOST",     "localhost")
    port     = os.getenv("DB_PORT",     "5432")
    dbname   = os.getenv("DB_NAME",     "stockdw")
    user     = os.getenv("DB_USER",     "data226")
    password = os.getenv("DB_PASSWORD", "12345678")
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(url, echo=False)


# ---------------------------------------------------------------------------
# Schema DDL  (mirrors sql/schema.sql)
# ---------------------------------------------------------------------------

def create_schema(engine) -> None:
    """Create all warehouse tables if they do not exist.

    Matches sql/schema.sql exactly so both the standalone ETL and the
    Airflow/Docker stack share the same database structure.
    """
    ddl = """
    CREATE TABLE IF NOT EXISTS dim_company (
        company_key     SERIAL       PRIMARY KEY,
        ticker          TEXT         NOT NULL UNIQUE,
        company_name    TEXT         NOT NULL,
        sector          TEXT,
        industry        TEXT,
        exchange        TEXT,
        country         TEXT,
        currency        TEXT,
        market_cap      BIGINT,
        eps             NUMERIC,
        pe_ratio        NUMERIC,
        dividend_per_share NUMERIC,
        beta            NUMERIC,
        fiscal_year_end TEXT,
        official_site   TEXT,
        is_current      BOOLEAN      NOT NULL DEFAULT TRUE,
        effective_date  DATE         NOT NULL DEFAULT CURRENT_DATE
    );

    CREATE TABLE IF NOT EXISTS dim_date (
        date          DATE     PRIMARY KEY,
        year          INT      NOT NULL,
        quarter       INT      NOT NULL,
        month         INT      NOT NULL,
        day_of_month  INT      NOT NULL,
        day_of_week   INT      NOT NULL,   -- 0=Mon … 6=Sun
        is_weekend    BOOLEAN  NOT NULL
    );

    CREATE TABLE IF NOT EXISTS dim_financials (
        ticker                    TEXT,
        year                      INT,
        category                  TEXT,
        market_cap_b_usd          NUMERIC,
        revenue                   NUMERIC,
        gross_profit              NUMERIC,
        net_income                NUMERIC,
        earning_per_share         NUMERIC,
        ebitda                    NUMERIC,
        share_holder_equity       NUMERIC,
        cash_flow_operating       NUMERIC,
        cash_flow_investing       NUMERIC,
        cash_flow_financial       NUMERIC,
        current_ratio             NUMERIC,
        debt_equity_ratio         NUMERIC,
        roe                       NUMERIC,
        roa                       NUMERIC,
        roi                       NUMERIC,
        net_profit_margin         NUMERIC,
        free_cash_flow_per_share  NUMERIC,
        return_on_tangible_equity NUMERIC,
        number_of_employees       INT,
        inflation_rate_us         NUMERIC,
        PRIMARY KEY (ticker, year)
    );

    CREATE TABLE IF NOT EXISTS fact_stock_price_daily (
        date               DATE             NOT NULL,
        company_key        INT              NOT NULL REFERENCES dim_company(company_key),
        open               DOUBLE PRECISION NOT NULL,
        high               DOUBLE PRECISION NOT NULL,
        low                DOUBLE PRECISION NOT NULL,
        close              DOUBLE PRECISION NOT NULL,
        adj_close          DOUBLE PRECISION,
        volume             BIGINT           NOT NULL,
        daily_return_pct   DOUBLE PRECISION,   -- open-to-close: (close-open)/open*100
        intraday_range     DOUBLE PRECISION,   -- high - low
        PRIMARY KEY (date, company_key),
        FOREIGN KEY (date) REFERENCES dim_date(date)
    );

    CREATE INDEX IF NOT EXISTS idx_daily_company
        ON fact_stock_price_daily (company_key, date DESC);
    """

    with engine.begin() as conn:
        # Enable TimescaleDB if available (no-op on plain Postgres)
        try:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb;"))
            logger.info("TimescaleDB extension enabled.")
        except Exception:
            logger.info("TimescaleDB not available — using standard PostgreSQL.")

        conn.execute(text(ddl))

        # Convert fact table to hypertable if TimescaleDB is available
        try:
            conn.execute(text(
                "SELECT create_hypertable('fact_stock_price_daily', 'date',"
                " if_not_exists => TRUE);"
            ))
            logger.info("fact_stock_price_daily confirmed as TimescaleDB hypertable.")
        except Exception:
            logger.info("Hypertable creation skipped (TimescaleDB not available).")

    logger.info("Schema created / verified.")


# ---------------------------------------------------------------------------
# Column-mapping helpers  (ETL DataFrame columns → DB column names)
# ---------------------------------------------------------------------------

def _prepare_dim_company(df: pd.DataFrame) -> pd.DataFrame:
    """Map etl/transform column names to the unified schema column names."""
    rename = {
        "Symbol":              "ticker",
        "Name":                "company_name",
        "Sector":              "sector",
        "Industry":            "industry",
        "Exchange":            "exchange",
        "Country":             "country",
        "Currency":            "currency",
        "MarketCapitalization":"market_cap",
        "EPS":                 "eps",
        "PERatio":             "pe_ratio",
        "DividendPerShare":    "dividend_per_share",
        "Beta":                "beta",
        "FiscalYearEnd":       "fiscal_year_end",
        "OfficialSite":        "official_site",
    }
    out = df.rename(columns=rename)[list(rename.values())]
    out["is_current"]     = True
    out["effective_date"] = date.today()
    return out


def _prepare_dim_date(df: pd.DataFrame) -> pd.DataFrame:
    """Produce the 7-column dim_date matching sql/schema.sql.

    Recomputes all fields from the Date column to ensure consistency
    regardless of which columns the transform layer produced.
    Weekday uses 0=Mon … 6=Sun to match sql/schema.sql and
    scripts/populate_dim_date.py.
    """
    dates = pd.to_datetime(df["Date"] if "Date" in df.columns else df["date"])
    return pd.DataFrame({
        "date":        dates,
        "year":        dates.dt.year,
        "quarter":     dates.dt.quarter,
        "month":       dates.dt.month,
        "day_of_month":dates.dt.day,
        "day_of_week": dates.dt.weekday,      # 0=Mon … 6=Sun
        "is_weekend":  dates.dt.weekday >= 5,
    })


def _prepare_dim_financials(df: pd.DataFrame) -> pd.DataFrame:
    """Map etl/transform column names to the unified schema column names."""
    rename = {
        "Symbol":                   "ticker",
        "Year":                     "year",
        "Category":                 "category",
        "Market_Cap_in_B_USD":      "market_cap_b_usd",
        "Revenue":                  "revenue",
        "Gross_Profit":             "gross_profit",
        "Net_Income":               "net_income",
        "Earning_Per_Share":        "earning_per_share",
        "EBITDA":                   "ebitda",
        "Share_Holder_Equity":      "share_holder_equity",
        "Cash_Flow_Operating":      "cash_flow_operating",
        "Cash_Flow_Investing":      "cash_flow_investing",
        "Cash_Flow_Financial":      "cash_flow_financial",
        "Current_Ratio":            "current_ratio",
        "Debt_Equity_Ratio":        "debt_equity_ratio",
        "ROE":                      "roe",
        "ROA":                      "roa",
        "ROI":                      "roi",
        "Net_Profit_Margin":        "net_profit_margin",
        "Free_Cash_Flow_Per_Share": "free_cash_flow_per_share",
        "Return_on_Tangible_Equity":"return_on_tangible_equity",
        "Number_of_Employees":      "number_of_employees",
        "Inflation_Rate_US":        "inflation_rate_us",
    }
    out = df.rename(columns=rename)
    cols = [v for v in rename.values() if v in out.columns]
    return out[cols]


def _prepare_fact(df: pd.DataFrame, company_keys: dict) -> pd.DataFrame:
    """Map Symbol → company_key and rename columns to match the DB schema."""
    out = df.copy()
    out["company_key"] = out["Symbol"].map(company_keys)

    missing = out["company_key"].isna().sum()
    if missing:
        logger.warning("%d fact rows have no matching company_key and will be dropped.", missing)
        out = out.dropna(subset=["company_key"])

    out["company_key"] = out["company_key"].astype(int)

    out = out.rename(columns={
        "Date":             "date",
        "Open":             "open",
        "High":             "high",
        "Low":              "low",
        "Close":            "close",
        "Adj_Close":        "adj_close",
        "Volume":           "volume",
        "Daily_Return_Pct": "daily_return_pct",
        "Intraday_Range":   "intraday_range",
    })

    return out[[
        "date", "company_key", "open", "high", "low", "close",
        "adj_close", "volume", "daily_return_pct", "intraday_range",
    ]]


# ---------------------------------------------------------------------------
# PostgreSQL load
# ---------------------------------------------------------------------------

def load_to_postgres(
    dim_company: pd.DataFrame,
    dim_date: pd.DataFrame,
    dim_financials: pd.DataFrame,
    fact_stock_prices: pd.DataFrame,
) -> None:
    """
    Load all four tables into PostgreSQL / TimescaleDB.

    Load order respects FK constraints:
      1. Truncate in reverse-FK order (dim_company CASCADE clears fact table)
      2. Load dim_company → query auto-generated company_key values
      3. Load dim_date
      4. Load dim_financials
      5. Load fact_stock_price_daily (using company_key mapping)
    """
    try:
        engine = _get_engine()
        create_schema(engine)

        # -- Truncate in reverse FK order -----------------------------------
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE dim_financials CASCADE"))
            # Cascades to fact_stock_price_daily (and fact_stock_price_monthly if present)
            conn.execute(text("TRUNCATE TABLE dim_company CASCADE"))
            conn.execute(text("TRUNCATE TABLE dim_date CASCADE"))

        # -- Load dim_company -----------------------------------------------
        dc = _prepare_dim_company(dim_company)
        dc.to_sql("dim_company", engine, if_exists="append", index=False,
                  method="multi", chunksize=1000)
        logger.info("Loaded %d rows into dim_company", len(dc))

        # -- Query auto-generated company_key values ------------------------
        with engine.connect() as conn:
            result = conn.execute(text("SELECT ticker, company_key FROM dim_company"))
            company_keys = {row.ticker: row.company_key for row in result}

        # -- Load dim_date --------------------------------------------------
        dd = _prepare_dim_date(dim_date)
        dd.to_sql("dim_date", engine, if_exists="append", index=False,
                  method="multi", chunksize=5000)
        logger.info("Loaded %d rows into dim_date", len(dd))

        # -- Load dim_financials -------------------------------------------
        df_fin = _prepare_dim_financials(dim_financials)
        df_fin.to_sql("dim_financials", engine, if_exists="append", index=False,
                      method="multi", chunksize=5000)
        logger.info("Loaded %d rows into dim_financials", len(df_fin))

        # -- Load fact_stock_price_daily ------------------------------------
        fact = _prepare_fact(fact_stock_prices, company_keys)
        fact.to_sql("fact_stock_price_daily", engine, if_exists="append", index=False,
                    method="multi", chunksize=5000)
        logger.info("Loaded %d rows into fact_stock_price_daily", len(fact))

        logger.info("All tables loaded successfully into PostgreSQL.")

    except Exception as exc:
        logger.error("PostgreSQL load failed: %s", exc)
        logger.info("Data is still available as CSV files in data/")
        raise
