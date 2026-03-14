-- =============================================================================
-- Stock Market Data Warehouse — Analysis Queries
-- =============================================================================
-- All queries run against the star schema defined in schema.sql.
--
-- Key join pattern:
--   fact_stock_price_daily f
--   JOIN dim_company c ON f.company_key = c.company_key   -- fact → company dim
--   JOIN dim_date    d ON f.date = d.date                  -- fact → date dim
--   JOIN dim_financials df ON df.ticker = c.ticker         -- company → financials
-- =============================================================================


-- ─────────────────────────────────────────────────────────────────────────────
-- 1. BASIC SANITY CHECKS
-- ─────────────────────────────────────────────────────────────────────────────

-- Row counts per table
SELECT 'dim_company'            AS tbl, COUNT(*) AS rows FROM dim_company
UNION ALL
SELECT 'dim_date',               COUNT(*) FROM dim_date
UNION ALL
SELECT 'dim_financials',         COUNT(*) FROM dim_financials
UNION ALL
SELECT 'fact_stock_price_daily', COUNT(*) FROM fact_stock_price_daily;


-- ─────────────────────────────────────────────────────────────────────────────
-- 2. PRICE ANALYSIS
-- ─────────────────────────────────────────────────────────────────────────────

-- 2a. Monthly average closing price per stock
SELECT
    c.ticker,
    d.year,
    d.month,
    TO_CHAR(d.date, 'Mon') AS month_name,
    ROUND(AVG(f.close)::NUMERIC, 2)  AS avg_close,
    ROUND(MAX(f.close)::NUMERIC, 2)  AS max_close,
    ROUND(MIN(f.close)::NUMERIC, 2)  AS min_close
FROM fact_stock_price_daily f
JOIN dim_company c ON f.company_key = c.company_key
JOIN dim_date    d ON f.date = d.date
GROUP BY c.ticker, d.year, d.month, TO_CHAR(d.date, 'Mon')
ORDER BY c.ticker, d.year, d.month;


-- 2b. Yearly returns: first vs. last close
WITH yearly_bounds AS (
    SELECT
        c.ticker,
        d.year,
        FIRST_VALUE(f.close) OVER (
            PARTITION BY c.ticker, d.year ORDER BY f.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS first_close,
        LAST_VALUE(f.close) OVER (
            PARTITION BY c.ticker, d.year ORDER BY f.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS last_close
    FROM fact_stock_price_daily f
    JOIN dim_company c ON f.company_key = c.company_key
    JOIN dim_date    d ON f.date = d.date
)
SELECT DISTINCT
    ticker,
    year,
    ROUND(first_close::NUMERIC, 2) AS first_close,
    ROUND(last_close::NUMERIC, 2)  AS last_close,
    ROUND(((last_close - first_close) / NULLIF(first_close, 0) * 100)::NUMERIC, 2) AS yearly_return_pct
FROM yearly_bounds
ORDER BY ticker, year;


-- 2c. Top 10 single-day price drops
SELECT
    c.ticker,
    f.date,
    ROUND(f.open::NUMERIC, 2)                                          AS open,
    ROUND(f.close::NUMERIC, 2)                                         AS close,
    ROUND((f.open - f.close)::NUMERIC, 2)                             AS drop_value,
    ROUND(((f.open - f.close) / NULLIF(f.open, 0) * 100)::NUMERIC, 2) AS drop_pct
FROM fact_stock_price_daily f
JOIN dim_company c ON f.company_key = c.company_key
WHERE f.open > f.close
ORDER BY drop_pct DESC
LIMIT 10;


-- 2d. Top 10 highest-volume trading days
SELECT
    c.ticker,
    f.date,
    f.volume,
    ROUND(f.close::NUMERIC, 2) AS close
FROM fact_stock_price_daily f
JOIN dim_company c ON f.company_key = c.company_key
ORDER BY f.volume DESC
LIMIT 10;


-- ─────────────────────────────────────────────────────────────────────────────
-- 3. VOLATILITY & RISK
-- ─────────────────────────────────────────────────────────────────────────────

-- 3a. Average intraday range (volatility proxy) by sector and symbol
SELECT
    c.sector,
    c.ticker,
    ROUND(AVG(f.high - f.low)::NUMERIC, 2)    AS avg_intraday_range,
    ROUND(STDDEV(f.close)::NUMERIC, 2)        AS stddev_close,
    ROUND(AVG(f.volume)::NUMERIC, 0)          AS avg_volume
FROM fact_stock_price_daily f
JOIN dim_company c ON f.company_key = c.company_key
GROUP BY c.sector, c.ticker
ORDER BY c.sector, avg_intraday_range DESC;


-- 3b. Distribution of daily return percentages (open-to-close)
SELECT
    c.ticker,
    ROUND(AVG(f.daily_return_pct)::NUMERIC, 4)    AS mean_daily_return,
    ROUND(STDDEV(f.daily_return_pct)::NUMERIC, 4) AS stddev_daily_return,
    ROUND(MIN(f.daily_return_pct)::NUMERIC, 2)    AS worst_day_pct,
    ROUND(MAX(f.daily_return_pct)::NUMERIC, 2)    AS best_day_pct
FROM fact_stock_price_daily f
JOIN dim_company c ON f.company_key = c.company_key
GROUP BY c.ticker
ORDER BY stddev_daily_return DESC;


-- 3c. Beta cross-check: compare stock volatility to company Beta attribute
SELECT
    c.ticker,
    c.company_name,
    c.beta,
    ROUND(STDDEV(f.daily_return_pct)::NUMERIC, 4) AS empirical_stddev_return
FROM fact_stock_price_daily f
JOIN dim_company c ON f.company_key = c.company_key
GROUP BY c.ticker, c.company_name, c.beta
ORDER BY empirical_stddev_return DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- 4. SECTOR ANALYSIS
-- ─────────────────────────────────────────────────────────────────────────────

-- 4a. Total market capitalisation by sector
SELECT
    sector,
    COUNT(*)                                  AS num_companies,
    SUM(market_cap)                           AS total_market_cap_usd,
    ROUND(AVG(market_cap)::NUMERIC, 0)        AS avg_market_cap_usd
FROM dim_company
GROUP BY sector
ORDER BY total_market_cap_usd DESC;


-- 4b. Sector average closing price trend per year
SELECT
    d.year,
    c.sector,
    ROUND(AVG(f.close)::NUMERIC, 2) AS avg_close
FROM fact_stock_price_daily f
JOIN dim_company c ON f.company_key = c.company_key
JOIN dim_date    d ON f.date = d.date
GROUP BY d.year, c.sector
ORDER BY d.year, c.sector;


-- ─────────────────────────────────────────────────────────────────────────────
-- 5. FINANCIAL FUNDAMENTALS
-- ─────────────────────────────────────────────────────────────────────────────

-- 5a. 5-year revenue growth for mega-cap companies (> $500 B market cap)
SELECT
    df.ticker,
    c.company_name,
    df.year,
    df.revenue,
    LAG(df.revenue, 5) OVER (PARTITION BY df.ticker ORDER BY df.year) AS revenue_5yr_ago,
    ROUND(
        ((df.revenue - LAG(df.revenue, 5) OVER (PARTITION BY df.ticker ORDER BY df.year))
         / NULLIF(LAG(df.revenue, 5) OVER (PARTITION BY df.ticker ORDER BY df.year), 0) * 100)::NUMERIC,
        2
    ) AS revenue_growth_5yr_pct
FROM dim_financials df
JOIN dim_company c ON df.ticker = c.ticker
WHERE c.market_cap > 500000000000
ORDER BY df.ticker, df.year;


-- 5b. Top stocks by Return on Equity (most recent year per symbol)
SELECT DISTINCT ON (df.ticker)
    df.ticker,
    c.company_name,
    df.year,
    ROUND(df.roe::NUMERIC, 2)               AS roe,
    ROUND(df.net_profit_margin::NUMERIC, 2) AS net_margin_pct,
    ROUND(df.debt_equity_ratio::NUMERIC, 2) AS de_ratio
FROM dim_financials df
JOIN dim_company c ON df.ticker = c.ticker
ORDER BY df.ticker, df.year DESC;


-- 5c. Revenue vs. stock price correlation proxy
SELECT
    df.ticker,
    df.year,
    df.revenue,
    ROUND(AVG(f.close)::NUMERIC, 2) AS avg_annual_close
FROM dim_financials df
JOIN dim_company c             ON df.ticker = c.ticker
JOIN fact_stock_price_daily f  ON f.company_key = c.company_key
JOIN dim_date d                ON f.date = d.date AND d.year = df.year
GROUP BY df.ticker, df.year, df.revenue
ORDER BY df.ticker, df.year;


-- 5d. Valuation efficiency: market cap per dollar of EPS
SELECT
    ticker,
    company_name,
    ROUND(eps::NUMERIC, 2)                                AS eps,
    market_cap,
    ROUND((market_cap / NULLIF(eps, 0))::NUMERIC, 0)      AS implied_pe_from_mktcap
FROM dim_company
WHERE eps IS NOT NULL AND market_cap IS NOT NULL AND eps > 0
ORDER BY implied_pe_from_mktcap ASC;


-- ─────────────────────────────────────────────────────────────────────────────
-- 6. WINDOW FUNCTIONS & ADVANCED ANALYTICS
-- ─────────────────────────────────────────────────────────────────────────────

-- 6a. 30-day and 90-day moving average closing price
SELECT
    f.date,
    c.ticker,
    f.close,
    ROUND(AVG(f.close) OVER (
        PARTITION BY f.company_key
        ORDER BY f.date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    )::NUMERIC, 4) AS ma_30,
    ROUND(AVG(f.close) OVER (
        PARTITION BY f.company_key
        ORDER BY f.date
        ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
    )::NUMERIC, 4) AS ma_90
FROM fact_stock_price_daily f
JOIN dim_company c ON f.company_key = c.company_key
ORDER BY c.ticker, f.date;


-- 6b. Ranking stocks by yearly return within each year
WITH yearly_return AS (
    SELECT
        c.ticker,
        d.year,
        ROUND(
            ((LAST_VALUE(f.close) OVER (PARTITION BY f.company_key, d.year ORDER BY f.date
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
            - FIRST_VALUE(f.close) OVER (PARTITION BY f.company_key, d.year ORDER BY f.date
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))
            / NULLIF(FIRST_VALUE(f.close) OVER (PARTITION BY f.company_key, d.year ORDER BY f.date
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0) * 100)::NUMERIC,
            2
        ) AS return_pct
    FROM fact_stock_price_daily f
    JOIN dim_company c ON f.company_key = c.company_key
    JOIN dim_date    d ON f.date = d.date
)
SELECT DISTINCT
    ticker,
    year,
    return_pct,
    RANK() OVER (PARTITION BY year ORDER BY return_pct DESC) AS rank_in_year
FROM yearly_return
ORDER BY year, rank_in_year;


-- 6c. Cumulative volume traded per symbol (YTD)
SELECT
    f.date,
    c.ticker,
    f.volume,
    SUM(f.volume) OVER (
        PARTITION BY f.company_key, EXTRACT(YEAR FROM f.date)
        ORDER BY f.date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_volume_ytd
FROM fact_stock_price_daily f
JOIN dim_company c ON f.company_key = c.company_key
ORDER BY c.ticker, f.date;


-- 6d. Consecutive gain streak detection
WITH ranked AS (
    SELECT
        company_key, date, close,
        ROW_NUMBER() OVER (PARTITION BY company_key ORDER BY date) AS rn
    FROM fact_stock_price_daily
),
streaks AS (
    SELECT
        a.company_key,
        a.date,
        COUNT(*) AS gain_streak
    FROM ranked a
    JOIN ranked b ON a.company_key = b.company_key AND a.rn = b.rn + 1
    WHERE a.close > b.close
    GROUP BY a.company_key, a.date
)
SELECT c.ticker, s.date, s.gain_streak
FROM streaks s
JOIN dim_company c ON s.company_key = c.company_key
WHERE gain_streak >= 5
ORDER BY gain_streak DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- 7. DIVIDEND ANALYSIS
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    ticker,
    company_name,
    ROUND(dividend_per_share::NUMERIC, 4) AS dividend_per_share,
    ROUND((dividend_per_share / NULLIF(eps, 0) * 100)::NUMERIC, 2) AS payout_ratio_pct
FROM dim_company
WHERE dividend_per_share IS NOT NULL AND dividend_per_share > 0
ORDER BY dividend_per_share DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- 8. OLAP: CUBE & GROUPING SETS
-- ─────────────────────────────────────────────────────────────────────────────

-- 8a. CUBE: average close across sector × ticker × year combinations
SELECT
    c.sector,
    c.ticker,
    d.year,
    ROUND(AVG(f.close)::NUMERIC, 2) AS avg_close,
    GROUPING(c.sector)              AS g_sector,
    GROUPING(c.ticker)              AS g_ticker,
    GROUPING(d.year)                AS g_year
FROM fact_stock_price_daily f
JOIN dim_company c ON f.company_key = c.company_key
JOIN dim_date    d ON f.date = d.date
GROUP BY CUBE (c.sector, c.ticker, d.year)
ORDER BY c.sector NULLS LAST, c.ticker NULLS LAST, d.year NULLS LAST;


-- 8b. GROUPING SETS: subtotals by year+ticker, year+sector, sector-only, grand total
SELECT
    d.year,
    c.ticker,
    c.sector,
    ROUND(AVG(f.close)::NUMERIC, 2) AS avg_close
FROM fact_stock_price_daily f
JOIN dim_company c ON f.company_key = c.company_key
JOIN dim_date    d ON f.date = d.date
GROUP BY GROUPING SETS (
    (d.year, c.ticker),
    (d.year, c.sector),
    (c.sector),
    ()
)
ORDER BY d.year NULLS LAST, c.ticker NULLS LAST;
