-- =============================================================================
-- Load CSV files into PostgreSQL via COPY
-- Run AFTER applying schema.sql:
--
--   # Create tables
--   docker exec -i timescaledb psql -U data226 -d stockdw < sql/schema.sql
--
--   # Load data
--   docker exec -i timescaledb psql -U data226 -d stockdw < sql/load_data.sql
--
-- Or locally:
--   psql -h localhost -U data226 -d stockdw -f sql/load_data.sql
--
-- CSV column order must match what etl/load.py exports to data/.
-- =============================================================================


-- ─────────────────────────────────────────────────────────────────────────────
-- dim_company
-- CSV columns (from etl/transform.py):
--   Symbol, Name, AssetType, Exchange, Sector, Industry, Country, Currency,
--   MarketCapitalization, EPS, PERatio, DividendPerShare, Beta, FiscalYearEnd,
--   OfficialSite
-- Load via staging table to handle CamelCase→snake_case rename.
-- ─────────────────────────────────────────────────────────────────────────────

\echo 'Loading dim_company...'

CREATE TEMP TABLE _dim_company_stage (
    ticker             TEXT,
    company_name       TEXT,
    asset_type         TEXT,   -- not stored in warehouse, discarded
    exchange           TEXT,
    sector             TEXT,
    industry           TEXT,
    country            TEXT,
    currency           TEXT,
    market_cap         BIGINT,
    eps                NUMERIC,
    pe_ratio           NUMERIC,
    dividend_per_share NUMERIC,
    beta               NUMERIC,
    fiscal_year_end    TEXT,
    official_site      TEXT
);

\copy _dim_company_stage
  FROM 'data/dim_company.csv' CSV HEADER;

INSERT INTO dim_company
    (ticker, company_name, sector, industry, exchange, country, currency,
     market_cap, eps, pe_ratio, dividend_per_share, beta, fiscal_year_end, official_site)
SELECT
    ticker, company_name, sector, industry, exchange, country, currency,
    market_cap, eps, pe_ratio, dividend_per_share, beta, fiscal_year_end, official_site
FROM _dim_company_stage
ON CONFLICT (ticker) DO UPDATE SET
    company_name       = EXCLUDED.company_name,
    sector             = EXCLUDED.sector,
    industry           = EXCLUDED.industry,
    exchange           = EXCLUDED.exchange,
    country            = EXCLUDED.country,
    currency           = EXCLUDED.currency,
    market_cap         = EXCLUDED.market_cap,
    eps                = EXCLUDED.eps,
    pe_ratio           = EXCLUDED.pe_ratio,
    dividend_per_share = EXCLUDED.dividend_per_share,
    beta               = EXCLUDED.beta,
    fiscal_year_end    = EXCLUDED.fiscal_year_end,
    official_site      = EXCLUDED.official_site;

DROP TABLE _dim_company_stage;


-- ─────────────────────────────────────────────────────────────────────────────
-- dim_date
-- CSV columns (7 cols matching schema.sql):
--   Date, Year, Quarter, Month, Day_Of_Month, Day_Of_Week, Is_Weekend
-- ─────────────────────────────────────────────────────────────────────────────

\echo 'Loading dim_date...'

\copy dim_date (date, year, quarter, month, day_of_month, day_of_week, is_weekend)
  FROM 'data/dim_date.csv' CSV HEADER;


-- ─────────────────────────────────────────────────────────────────────────────
-- dim_financials
-- CSV columns match schema.sql column order exactly (ticker, year, category, …)
-- ─────────────────────────────────────────────────────────────────────────────

\echo 'Loading dim_financials...'

\copy dim_financials (ticker, year, category, market_cap_b_usd, revenue, gross_profit,
  net_income, earning_per_share, ebitda, share_holder_equity, cash_flow_operating,
  cash_flow_investing, cash_flow_financial, current_ratio, debt_equity_ratio,
  roe, roa, roi, net_profit_margin, free_cash_flow_per_share,
  return_on_tangible_equity, number_of_employees, inflation_rate_us)
  FROM 'data/dim_financials.csv' CSV HEADER;


-- ─────────────────────────────────────────────────────────────────────────────
-- fact_stock_price_daily
-- The CSV (data/fact_stock_prices.csv) uses Symbol (text ticker) rather than
-- company_key.  Load via staging table and look up company_key from dim_company.
-- CSV columns:
--   Date, Open, High, Low, Close, Adj_Close, Volume, Symbol,
--   Daily_Return_Pct, Intraday_Range
-- ─────────────────────────────────────────────────────────────────────────────

\echo 'Loading fact_stock_price_daily...'

CREATE TEMP TABLE _fact_stage (
    date             DATE,
    open             DOUBLE PRECISION,
    high             DOUBLE PRECISION,
    low              DOUBLE PRECISION,
    close            DOUBLE PRECISION,
    adj_close        DOUBLE PRECISION,
    volume           BIGINT,
    ticker           TEXT,
    daily_return_pct DOUBLE PRECISION,
    intraday_range   DOUBLE PRECISION
);

\copy _fact_stage (date, open, high, low, close, adj_close, volume, ticker,
                   daily_return_pct, intraday_range)
  FROM 'data/fact_stock_prices.csv' CSV HEADER;

INSERT INTO fact_stock_price_daily
    (date, company_key, open, high, low, close, adj_close,
     volume, daily_return_pct, intraday_range)
SELECT
    s.date,
    c.company_key,
    s.open, s.high, s.low, s.close, s.adj_close,
    s.volume, s.daily_return_pct, s.intraday_range
FROM _fact_stage s
JOIN dim_company c ON c.ticker = s.ticker
ON CONFLICT (date, company_key) DO UPDATE SET
    open             = EXCLUDED.open,
    high             = EXCLUDED.high,
    low              = EXCLUDED.low,
    close            = EXCLUDED.close,
    adj_close        = EXCLUDED.adj_close,
    volume           = EXCLUDED.volume,
    daily_return_pct = EXCLUDED.daily_return_pct,
    intraday_range   = EXCLUDED.intraday_range;

DROP TABLE _fact_stage;


\echo 'Done. Row counts:'
SELECT 'dim_company'            AS tbl, COUNT(*) FROM dim_company
UNION ALL
SELECT 'dim_date',               COUNT(*) FROM dim_date
UNION ALL
SELECT 'dim_financials',         COUNT(*) FROM dim_financials
UNION ALL
SELECT 'fact_stock_price_daily', COUNT(*) FROM fact_stock_price_daily;
