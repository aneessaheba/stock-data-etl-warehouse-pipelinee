-- =============================================================================
-- Stock Market Data Warehouse — Analysis Queries
-- =============================================================================
-- All queries are designed to run against the star-schema loaded by schema.sql.
-- =============================================================================


-- ─────────────────────────────────────────────────────────────────────────────
-- 1. BASIC SANITY CHECKS
-- ─────────────────────────────────────────────────────────────────────────────

-- Row counts per table
SELECT 'dim_company'       AS tbl, COUNT(*) AS rows FROM dim_company
UNION ALL
SELECT 'dim_date',          COUNT(*) FROM dim_date
UNION ALL
SELECT 'dim_financials',    COUNT(*) FROM dim_financials
UNION ALL
SELECT 'fact_stock_prices', COUNT(*) FROM fact_stock_prices;


-- ─────────────────────────────────────────────────────────────────────────────
-- 2. PRICE ANALYSIS
-- ─────────────────────────────────────────────────────────────────────────────

-- 2a. Monthly average closing price per stock
SELECT
    f.Symbol,
    d.Year,
    d.Month,
    d.Month_Name,
    ROUND(AVG(f.Close)::NUMERIC, 2)  AS Avg_Close,
    ROUND(MAX(f.Close)::NUMERIC, 2)  AS Max_Close,
    ROUND(MIN(f.Close)::NUMERIC, 2)  AS Min_Close
FROM fact_stock_prices f
JOIN dim_date d ON f.Date = d.Date
GROUP BY f.Symbol, d.Year, d.Month, d.Month_Name
ORDER BY f.Symbol, d.Year, d.Month;


-- 2b. Yearly returns: first vs. last close
WITH yearly_bounds AS (
    SELECT
        f.Symbol,
        d.Year,
        FIRST_VALUE(f.Close) OVER (
            PARTITION BY f.Symbol, d.Year ORDER BY f.Date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS First_Close,
        LAST_VALUE(f.Close) OVER (
            PARTITION BY f.Symbol, d.Year ORDER BY f.Date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS Last_Close
    FROM fact_stock_prices f
    JOIN dim_date d ON f.Date = d.Date
)
SELECT DISTINCT
    Symbol,
    Year,
    ROUND(First_Close::NUMERIC, 2) AS First_Close,
    ROUND(Last_Close::NUMERIC, 2)  AS Last_Close,
    ROUND(((Last_Close - First_Close) / NULLIF(First_Close, 0) * 100)::NUMERIC, 2) AS Yearly_Return_Pct
FROM yearly_bounds
ORDER BY Symbol, Year;


-- 2c. Top 10 single-day price drops
SELECT
    f.Symbol,
    f.Date,
    ROUND(f.Open::NUMERIC, 2)                                          AS Open,
    ROUND(f.Close::NUMERIC, 2)                                         AS Close,
    ROUND((f.Open - f.Close)::NUMERIC, 2)                             AS Drop_Value,
    ROUND(((f.Open - f.Close) / NULLIF(f.Open, 0) * 100)::NUMERIC, 2) AS Drop_Pct
FROM fact_stock_prices f
WHERE f.Open > f.Close
ORDER BY Drop_Pct DESC
LIMIT 10;


-- 2d. Top 10 highest-volume trading days
SELECT
    f.Symbol,
    f.Date,
    f.Volume,
    ROUND(f.Close::NUMERIC, 2) AS Close
FROM fact_stock_prices f
ORDER BY f.Volume DESC
LIMIT 10;


-- ─────────────────────────────────────────────────────────────────────────────
-- 3. VOLATILITY & RISK
-- ─────────────────────────────────────────────────────────────────────────────

-- 3a. Average intraday range (volatility proxy) by sector and symbol
SELECT
    c.Sector,
    f.Symbol,
    ROUND(AVG(f.High - f.Low)::NUMERIC, 2)    AS Avg_Intraday_Range,
    ROUND(STDDEV(f.Close)::NUMERIC, 2)        AS StdDev_Close,
    ROUND(AVG(f.Volume)::NUMERIC, 0)          AS Avg_Volume
FROM fact_stock_prices f
JOIN dim_company c ON f.Symbol = c.Symbol
GROUP BY c.Sector, f.Symbol
ORDER BY c.Sector, Avg_Intraday_Range DESC;


-- 3b. Distribution of daily return percentages
SELECT
    f.Symbol,
    ROUND(AVG(f.Daily_Return_Pct)::NUMERIC, 4)    AS Mean_Daily_Return,
    ROUND(STDDEV(f.Daily_Return_Pct)::NUMERIC, 4) AS StdDev_Daily_Return,
    ROUND(MIN(f.Daily_Return_Pct)::NUMERIC, 2)    AS Worst_Day_Pct,
    ROUND(MAX(f.Daily_Return_Pct)::NUMERIC, 2)    AS Best_Day_Pct
FROM fact_stock_prices f
GROUP BY f.Symbol
ORDER BY StdDev_Daily_Return DESC;


-- 3c. Beta cross-check: compare stock volatility to company Beta attribute
SELECT
    c.Symbol,
    c.Name,
    c.Beta,
    ROUND(STDDEV(f.Daily_Return_Pct)::NUMERIC, 4) AS Empirical_StdDev_Return
FROM fact_stock_prices f
JOIN dim_company c ON f.Symbol = c.Symbol
GROUP BY c.Symbol, c.Name, c.Beta
ORDER BY Empirical_StdDev_Return DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- 4. SECTOR ANALYSIS
-- ─────────────────────────────────────────────────────────────────────────────

-- 4a. Total market capitalisation by sector
SELECT
    Sector,
    COUNT(*)                                           AS Num_Companies,
    SUM(MarketCapitalization)                          AS Total_Market_Cap_USD,
    ROUND(AVG(MarketCapitalization)::NUMERIC, 0)       AS Avg_Market_Cap_USD
FROM dim_company
GROUP BY Sector
ORDER BY Total_Market_Cap_USD DESC;


-- 4b. Sector average closing price trend per year
SELECT
    d.Year,
    c.Sector,
    ROUND(AVG(f.Close)::NUMERIC, 2) AS Avg_Close
FROM fact_stock_prices f
JOIN dim_company c ON f.Symbol = c.Symbol
JOIN dim_date d    ON f.Date   = d.Date
GROUP BY d.Year, c.Sector
ORDER BY d.Year, c.Sector;


-- ─────────────────────────────────────────────────────────────────────────────
-- 5. FINANCIAL FUNDAMENTALS
-- ─────────────────────────────────────────────────────────────────────────────

-- 5a. 5-year revenue growth for mega-cap companies (> $500 B market cap)
SELECT
    df.Symbol,
    c.Name,
    df.Year,
    df.Revenue,
    LAG(df.Revenue, 5) OVER (PARTITION BY df.Symbol ORDER BY df.Year) AS Revenue_5yr_Ago,
    ROUND(
        ((df.Revenue - LAG(df.Revenue, 5) OVER (PARTITION BY df.Symbol ORDER BY df.Year))
         / NULLIF(LAG(df.Revenue, 5) OVER (PARTITION BY df.Symbol ORDER BY df.Year), 0) * 100)::NUMERIC,
        2
    ) AS Revenue_Growth_5yr_Pct
FROM dim_financials df
JOIN dim_company c ON df.Symbol = c.Symbol
WHERE c.MarketCapitalization > 500000000000
ORDER BY df.Symbol, df.Year;


-- 5b. Top stocks by Return on Equity (most recent year per symbol)
SELECT DISTINCT ON (df.Symbol)
    df.Symbol,
    c.Name,
    df.Year,
    ROUND(df.ROE::NUMERIC, 2)              AS ROE,
    ROUND(df.Net_Profit_Margin::NUMERIC, 2) AS Net_Margin_Pct,
    ROUND(df.Debt_Equity_Ratio::NUMERIC, 2) AS DE_Ratio
FROM dim_financials df
JOIN dim_company c ON df.Symbol = c.Symbol
ORDER BY df.Symbol, df.Year DESC;


-- 5c. Revenue vs. stock price correlation proxy
SELECT
    df.Symbol,
    df.Year,
    df.Revenue,
    ROUND(AVG(f.Close)::NUMERIC, 2) AS Avg_Annual_Close
FROM dim_financials df
JOIN fact_stock_prices f  ON df.Symbol = f.Symbol
JOIN dim_date d           ON f.Date = d.Date AND d.Year = df.Year
GROUP BY df.Symbol, df.Year, df.Revenue
ORDER BY df.Symbol, df.Year;


-- 5d. Valuation efficiency: market cap per dollar of EPS
SELECT
    Symbol,
    Name,
    ROUND(EPS::NUMERIC, 2)                           AS EPS,
    MarketCapitalization,
    ROUND((MarketCapitalization / NULLIF(EPS, 0))::NUMERIC, 0) AS Implied_PE_From_Mktcap
FROM dim_company
WHERE EPS IS NOT NULL AND MarketCapitalization IS NOT NULL AND EPS > 0
ORDER BY Implied_PE_From_Mktcap ASC;


-- ─────────────────────────────────────────────────────────────────────────────
-- 6. WINDOW FUNCTIONS & ADVANCED ANALYTICS
-- ─────────────────────────────────────────────────────────────────────────────

-- 6a. 30-day and 90-day moving average closing price
SELECT
    Date,
    Symbol,
    Close,
    ROUND(AVG(Close) OVER (
        PARTITION BY Symbol
        ORDER BY Date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    )::NUMERIC, 4) AS MA_30,
    ROUND(AVG(Close) OVER (
        PARTITION BY Symbol
        ORDER BY Date
        ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
    )::NUMERIC, 4) AS MA_90
FROM fact_stock_prices
ORDER BY Symbol, Date;


-- 6b. Ranking stocks by yearly return within each year
WITH yearly_return AS (
    SELECT
        f.Symbol,
        d.Year,
        ROUND(
            ((LAST_VALUE(f.Close) OVER (PARTITION BY f.Symbol, d.Year ORDER BY f.Date
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
            - FIRST_VALUE(f.Close) OVER (PARTITION BY f.Symbol, d.Year ORDER BY f.Date
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))
            / NULLIF(FIRST_VALUE(f.Close) OVER (PARTITION BY f.Symbol, d.Year ORDER BY f.Date
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0) * 100)::NUMERIC,
            2
        ) AS Return_Pct
    FROM fact_stock_prices f
    JOIN dim_date d ON f.Date = d.Date
)
SELECT DISTINCT
    Symbol,
    Year,
    Return_Pct,
    RANK() OVER (PARTITION BY Year ORDER BY Return_Pct DESC) AS Rank_In_Year
FROM yearly_return
ORDER BY Year, Rank_In_Year;


-- 6c. Cumulative volume traded per symbol (YTD)
SELECT
    Date,
    Symbol,
    Volume,
    SUM(Volume) OVER (
        PARTITION BY Symbol, EXTRACT(YEAR FROM Date)
        ORDER BY Date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS Cumulative_Volume_YTD
FROM fact_stock_prices
ORDER BY Symbol, Date;


-- 6d. Consecutive gain streak detection
WITH ranked AS (
    SELECT
        Symbol, Date, Close,
        ROW_NUMBER() OVER (PARTITION BY Symbol ORDER BY Date) AS rn
    FROM fact_stock_prices
),
streaks AS (
    SELECT
        a.Symbol,
        a.Date,
        COUNT(*) AS Gain_Streak
    FROM ranked a
    JOIN ranked b ON a.Symbol = b.Symbol AND a.rn = b.rn + 1
    WHERE a.Close > b.Close
    GROUP BY a.Symbol, a.Date
)
SELECT Symbol, Date, Gain_Streak
FROM streaks
WHERE Gain_Streak >= 5
ORDER BY Gain_Streak DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- 7. DIVIDEND ANALYSIS
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    Symbol,
    Name,
    ROUND(DividendPerShare::NUMERIC, 4) AS DividendPerShare,
    ROUND((DividendPerShare / NULLIF(EPS, 0) * 100)::NUMERIC, 2) AS Payout_Ratio_Pct
FROM dim_company
WHERE DividendPerShare IS NOT NULL AND DividendPerShare > 0
ORDER BY DividendPerShare DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- 8. OLAP: CUBE & GROUPING SETS
-- ─────────────────────────────────────────────────────────────────────────────

-- 8a. CUBE: average close across sector × symbol × year combinations
SELECT
    c.Sector,
    f.Symbol,
    d.Year,
    ROUND(AVG(f.Close)::NUMERIC, 2) AS Avg_Close,
    GROUPING(c.Sector)              AS g_sector,
    GROUPING(f.Symbol)              AS g_symbol,
    GROUPING(d.Year)                AS g_year
FROM fact_stock_prices f
JOIN dim_company c ON f.Symbol = c.Symbol
JOIN dim_date d    ON f.Date   = d.Date
GROUP BY CUBE (c.Sector, f.Symbol, d.Year)
ORDER BY c.Sector NULLS LAST, f.Symbol NULLS LAST, d.Year NULLS LAST;


-- 8b. GROUPING SETS: subtotals by year+symbol, year+sector, sector-only, grand total
SELECT
    d.Year,
    f.Symbol,
    c.Sector,
    ROUND(AVG(f.Close)::NUMERIC, 2) AS Avg_Close
FROM fact_stock_prices f
JOIN dim_company c ON f.Symbol = c.Symbol
JOIN dim_date d    ON f.Date   = d.Date
GROUP BY GROUPING SETS (
    (d.Year, f.Symbol),
    (d.Year, c.Sector),
    (c.Sector),
    ()
)
ORDER BY d.Year NULLS LAST, f.Symbol NULLS LAST;
