"""
Load Layer
==========
Exports transformed DataFrames to:
  1. CSV files in data/  (always)
  2. PostgreSQL tables   (when DB connection is available)
"""

import logging
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent.parent / "data"


# ---------------------------------------------------------------------------
# CSV export
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
# PostgreSQL load
# ---------------------------------------------------------------------------

def _get_engine():
    """Build SQLAlchemy engine from environment variables."""
    host     = os.getenv("DB_HOST", "localhost")
    port     = os.getenv("DB_PORT", "5432")
    dbname   = os.getenv("DB_NAME", "stock_db")
    user     = os.getenv("DB_USER", "admin")
    password = os.getenv("DB_PASSWORD", "admin123")
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(url, echo=False)


def create_schema(engine) -> None:
    """Create all warehouse tables if they do not exist."""
    ddl = """
    CREATE TABLE IF NOT EXISTS dim_company (
        Symbol               TEXT PRIMARY KEY,
        Name                 TEXT,
        AssetType            TEXT,
        Exchange             TEXT,
        Sector               TEXT,
        Industry             TEXT,
        Country              TEXT,
        Currency             TEXT,
        MarketCapitalization BIGINT,
        EPS                  NUMERIC,
        PERatio              NUMERIC,
        DividendPerShare     NUMERIC,
        Beta                 NUMERIC,
        FiscalYearEnd        TEXT,
        OfficialSite         TEXT
    );

    CREATE TABLE IF NOT EXISTS dim_date (
        Date             DATE PRIMARY KEY,
        Year             INT,
        Quarter          INT,
        Month            INT,
        Month_Name       TEXT,
        Day              INT,
        Day_Name         TEXT,
        Week             INT,
        Weekday          INT,
        Is_Weekend       BOOLEAN,
        Day_Of_Year      INT,
        Is_Month_Start   BOOLEAN,
        Is_Month_End     BOOLEAN,
        Is_Quarter_Start BOOLEAN,
        Is_Quarter_End   BOOLEAN,
        Is_Year_Start    BOOLEAN,
        Is_Year_End      BOOLEAN
    );

    CREATE TABLE IF NOT EXISTS dim_financials (
        Year                       INT,
        Symbol                     TEXT,
        Category                   TEXT,
        Market_Cap_in_B_USD        NUMERIC,
        Revenue                    NUMERIC,
        Gross_Profit               NUMERIC,
        Net_Income                 NUMERIC,
        Earning_Per_Share          NUMERIC,
        EBITDA                     NUMERIC,
        Share_Holder_Equity        NUMERIC,
        Cash_Flow_Operating        NUMERIC,
        Cash_Flow_Investing        NUMERIC,
        Cash_Flow_Financial        NUMERIC,
        Current_Ratio              NUMERIC,
        Debt_Equity_Ratio          NUMERIC,
        ROE                        NUMERIC,
        ROA                        NUMERIC,
        ROI                        NUMERIC,
        Net_Profit_Margin          NUMERIC,
        Free_Cash_Flow_Per_Share   NUMERIC,
        Return_on_Tangible_Equity  NUMERIC,
        Number_of_Employees        INT,
        Inflation_Rate_US          NUMERIC,
        PRIMARY KEY (Symbol, Year)
    );

    CREATE TABLE IF NOT EXISTS fact_stock_prices (
        Date              DATE,
        Open              NUMERIC,
        High              NUMERIC,
        Low               NUMERIC,
        Close             NUMERIC,
        Adj_Close         NUMERIC,
        Volume            BIGINT,
        Symbol            TEXT,
        Daily_Return_Pct  NUMERIC,
        Intraday_Range    NUMERIC,
        PRIMARY KEY (Date, Symbol),
        FOREIGN KEY (Symbol) REFERENCES dim_company(Symbol),
        FOREIGN KEY (Date)   REFERENCES dim_date(Date)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))
    logger.info("Schema created / verified.")


def _upsert_table(df: pd.DataFrame, table: str, engine, pk_cols: list[str]) -> None:
    """
    Upsert a DataFrame into a PostgreSQL table.
    Strategy: truncate & reload (suitable for small-to-medium warehouse tables).
    For production, replace with proper MERGE / ON CONFLICT logic.
    """
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table} CASCADE"))
    df.to_sql(table, engine, if_exists="append", index=False, method="multi", chunksize=5000)
    logger.info("Loaded %d rows into %s", len(df), table)


def load_to_postgres(
    dim_company: pd.DataFrame,
    dim_date: pd.DataFrame,
    dim_financials: pd.DataFrame,
    fact_stock_prices: pd.DataFrame,
) -> None:
    """
    Load all four tables into PostgreSQL.
    Connection parameters are read from environment variables (see .env.example).
    """
    try:
        engine = _get_engine()
        create_schema(engine)

        # Load order respects FK constraints: dims before fact
        _upsert_table(dim_company,      "dim_company",      engine, ["Symbol"])
        _upsert_table(dim_date,         "dim_date",         engine, ["Date"])
        _upsert_table(dim_financials,   "dim_financials",   engine, ["Symbol", "Year"])
        _upsert_table(fact_stock_prices,"fact_stock_prices",engine, ["Date", "Symbol"])

        logger.info("All tables loaded successfully into PostgreSQL.")
    except Exception as exc:
        logger.error("PostgreSQL load failed: %s", exc)
        logger.info("Data is still available as CSV files in data/")
        raise
