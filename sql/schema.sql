-- =============================================================================
-- Stock Market Data Warehouse — Schema DDL
-- Database: TimescaleDB (PostgreSQL 14 + TimescaleDB extension)
--
-- Star Schema:
--   dim_company            — company master data (SCD Type 2 ready)
--   dim_date               — calendar dimension
--   dim_financials         — annual financial statements (FK → dim_company)
--   fact_stock_price_daily — daily OHLCV (TimescaleDB hypertable)
--   fact_stock_price_monthly — pre-aggregated monthly summaries
--
-- To apply:
--   psql -h localhost -U data226 -d stockdw -f sql/schema.sql
-- =============================================================================

-- Enable TimescaleDB
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Drop in reverse FK order (safe re-run)
DROP TABLE IF EXISTS fact_stock_price_monthly CASCADE;
DROP TABLE IF EXISTS fact_stock_price_daily    CASCADE;
DROP TABLE IF EXISTS dim_financials            CASCADE;
DROP TABLE IF EXISTS dim_date                  CASCADE;
DROP TABLE IF EXISTS dim_company               CASCADE;


-- =============================================================================
-- DIMENSION: Company  (SCD Type 2 — is_current + effective_date)
-- Includes one-time financial snapshot cols populated by the ETL pipeline.
-- =============================================================================
CREATE TABLE dim_company (
    company_key        SERIAL       PRIMARY KEY,
    ticker             TEXT         NOT NULL UNIQUE,
    company_name       TEXT         NOT NULL,
    sector             TEXT,
    industry           TEXT,
    exchange           TEXT,
    country            TEXT,
    currency           TEXT,
    market_cap         BIGINT,
    eps                NUMERIC,
    pe_ratio           NUMERIC,
    dividend_per_share NUMERIC,
    beta               NUMERIC,
    fiscal_year_end    TEXT,
    official_site      TEXT,
    is_current         BOOLEAN      NOT NULL DEFAULT TRUE,
    effective_date     DATE         NOT NULL DEFAULT CURRENT_DATE
);

COMMENT ON TABLE dim_company IS
    'Company master data — SCD Type 2. One active row per ticker (is_current=TRUE). '
    'Financial snapshot cols (market_cap, eps, beta, etc.) populated by ETL pipeline.';


-- =============================================================================
-- DIMENSION: Date
-- =============================================================================
CREATE TABLE dim_date (
    date         DATE     PRIMARY KEY,
    year         INT      NOT NULL,
    quarter      INT      NOT NULL,
    month        INT      NOT NULL,
    day_of_month INT      NOT NULL,
    day_of_week  INT      NOT NULL,   -- 0=Mon … 6=Sun
    is_weekend   BOOLEAN  NOT NULL
);

COMMENT ON TABLE dim_date IS
    'Calendar dimension — one row per day (1980-01-01 to 2035-12-31).';


-- =============================================================================
-- DIMENSION: Financial Statements  (annual, per company)
-- FK to dim_company enforces referential integrity at DB level.
-- =============================================================================
CREATE TABLE dim_financials (
    ticker                    TEXT         NOT NULL REFERENCES dim_company(ticker),
    year                      INT          NOT NULL,
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

COMMENT ON TABLE dim_financials IS
    'Annual financial statements (2009-2023). Source: Kaggle financial statements dataset. '
    'FK dim_financials.ticker → dim_company.ticker enforces referential integrity.';


-- =============================================================================
-- FACT: Daily Stock Prices  (TimescaleDB hypertable partitioned by date)
-- adj_close is populated by the batch ETL path; NULL for streaming intraday rows.
-- =============================================================================
CREATE TABLE fact_stock_price_daily (
    date              DATE             NOT NULL,
    company_key       INT              NOT NULL REFERENCES dim_company(company_key),
    open              DOUBLE PRECISION NOT NULL,
    high              DOUBLE PRECISION NOT NULL,
    low               DOUBLE PRECISION NOT NULL,
    close             DOUBLE PRECISION NOT NULL,
    adj_close         DOUBLE PRECISION,           -- NULL for streaming intraday rows
    volume            BIGINT           NOT NULL,
    daily_return_pct  DOUBLE PRECISION,           -- (close - open) / open * 100
    intraday_range    DOUBLE PRECISION,           -- high - low
    PRIMARY KEY (date, company_key),
    FOREIGN KEY (date) REFERENCES dim_date(date)
);

-- Convert to TimescaleDB hypertable (time-series optimised, partitioned by date)
SELECT create_hypertable(
    'fact_stock_price_daily', 'date',
    if_not_exists => TRUE
);

COMMENT ON TABLE fact_stock_price_daily IS
    'Daily OHLCV price observations — TimescaleDB hypertable. Grain: (date, company_key). '
    'adj_close is NULL for rows written by the streaming consumer (intraday bars have no split adjustment).';

CREATE INDEX IF NOT EXISTS idx_daily_company ON fact_stock_price_daily (company_key, date DESC);


-- =============================================================================
-- FACT: Monthly Aggregates
-- =============================================================================
CREATE TABLE fact_stock_price_monthly (
    company_key  INT              NOT NULL REFERENCES dim_company(company_key),
    month        DATE             NOT NULL,   -- first day of the month
    avg_open     DOUBLE PRECISION NOT NULL,
    avg_high     DOUBLE PRECISION NOT NULL,
    avg_low      DOUBLE PRECISION NOT NULL,
    avg_close    DOUBLE PRECISION NOT NULL,
    total_volume BIGINT           NOT NULL,
    trading_days INT,
    PRIMARY KEY (company_key, month)
);

COMMENT ON TABLE fact_stock_price_monthly IS
    'Pre-aggregated monthly price summaries. Populated by monthly_aggregate_dag / aggregate_monthly.sql.';
