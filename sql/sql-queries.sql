-- dim_company
CREATE TABLE dim_company (
    Symbol TEXT PRIMARY KEY,
    Name TEXT,
    AssetType TEXT,
    Exchange TEXT,
    Sector TEXT,
    Industry TEXT,
    Country TEXT,
    Currency TEXT,
    MarketCapitalization BIGINT,
    EPS NUMERIC,
    PERatio NUMERIC,
    DividendPerShare NUMERIC,
    Beta NUMERIC,
    FiscalYearEnd TEXT,
    OfficialSite TEXT
);

-- dim_date
CREATE TABLE dim_date (
    Date DATE PRIMARY KEY,
    Year INT,
    Quarter INT,
    Month INT,
    Month_Name TEXT,
    Day INT,
    Day_Name TEXT,
    Week INT,
    Weekday INT,
    Is_Weekend BOOLEAN,
    Day_Of_Year INT,
    Is_Month_Start BOOLEAN,
    Is_Month_End BOOLEAN,
    Is_Quarter_Start BOOLEAN,
    Is_Quarter_End BOOLEAN,
    Is_Year_Start BOOLEAN,
    Is_Year_End BOOLEAN
);

-- dim_financials
CREATE TABLE dim_financials (
    Year INT,
    Symbol TEXT,
    Category TEXT,
    Market_Cap_in_B_USD NUMERIC,
    Revenue NUMERIC,
    Gross_Profit NUMERIC,
    Net_Income NUMERIC,
    Earning_Per_Share NUMERIC,
    EBITDA NUMERIC,
    Share_Holder_Equity NUMERIC,
    Cash_Flow_Operating NUMERIC,
    Cash_Flow_Investing NUMERIC,
    Cash_Flow_Financial NUMERIC,
    Current_Ratio NUMERIC,
    Debt_Equity_Ratio NUMERIC,
    ROE NUMERIC,
    ROA NUMERIC,
    ROI NUMERIC,
    Net_Profit_Margin NUMERIC,
    Free_Cash_Flow_Per_Share NUMERIC,
    Return_on_Tangible_Equity NUMERIC,
    Number_of_Employees INT,
    Inflation_Rate_US NUMERIC
);

-- fact_stock_prices
CREATE TABLE fact_stock_prices (
    Date DATE,
    Open NUMERIC,
    High NUMERIC,
    Low NUMERIC,
    Close NUMERIC,
    Adj_Close NUMERIC,
    Volume BIGINT,
    Symbol TEXT,
    PRIMARY KEY (Date, Symbol)
);

select * from dim_company;
select * from fact_stock_prices;
select * from dim_financials;
select * from dim_date;



SELECT
    c.Symbol,
    d.Year,
    d.Month,
    ROUND(AVG(f.Close), 2) AS Avg_Close_Price
FROM fact_stock_prices f
JOIN dim_company c ON f.Symbol = c.Symbol
JOIN dim_date d ON f.Date = d.Date
GROUP BY c.Symbol, d.Year, d.Month
ORDER BY c.Symbol, d.Year, d.Month;


SELECT
    f.Symbol,
    f.Date,
    (f.Open - f.Close) AS Drop_Value,
    ROUND(((f.Open - f.Close) / f.Open) * 100, 2) AS Drop_Percentage
FROM fact_stock_prices f
WHERE f.Open > f.Close
ORDER BY Drop_Percentage DESC
LIMIT 10;


SELECT
    Symbol,
    Date,
    Volume
FROM fact_stock_prices
ORDER BY Volume DESC
LIMIT 10;

SELECT
    Symbol,
    Name,
    DividendPerShare
FROM dim_company
WHERE DividendPerShare IS NOT NULL
ORDER BY DividendPerShare DESC
LIMIT 5;

SELECT column_name
FROM information_schema.columns
WHERE table_name = 'dim_company';

SELECT
    c.Symbol,
    d.Year,
    d.Month,
    ROUND(AVG(f.Close), 2) AS Avg_Close_Price
FROM fact_stock_prices f
JOIN dim_company c ON f.Symbol = c.Symbol
JOIN dim_date d ON f.Date = d.Date
GROUP BY c.Symbol, d.Year, d.Month
ORDER BY c.Symbol, d.Year, d.Month;

SELECT
    f.Symbol,
    f.Date,
    (f.Open - f.Close) AS Drop_Value,
    ROUND(((f.Open - f.Close) / f.Open) * 100, 2) AS Drop_Percentage
FROM fact_stock_prices f
WHERE f.Open > f.Close
ORDER BY Drop_Percentage DESC
LIMIT 10;

SELECT
    Sector,
    SUM(MarketCapitalization) AS Total_Sector_Cap
FROM dim_company
GROUP BY Sector
ORDER BY Total_Sector_Cap DESC;

SELECT
    f.Symbol,
    ROUND(AVG(f.Close), 2) AS Avg_Close,
    dc.EPS
FROM fact_stock_prices f
JOIN dim_company dc ON f.Symbol = dc.Symbol
GROUP BY f.Symbol, dc.EPS
ORDER BY Avg_Close DESC;

SELECT
    symbol,
    year,
    ROUND(((last_close - first_close) / NULLIF(first_close, 0)) * 100, 2) AS yearly_return_pct
FROM (
    SELECT
        f.Symbol,
        d.Year,
        FIRST_VALUE(f.Close) OVER (PARTITION BY f.Symbol, d.Year ORDER BY d.Date) AS first_close,
        LAST_VALUE(f.Close) OVER (
            PARTITION BY f.Symbol, d.Year
            ORDER BY d.Date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS last_close
    FROM fact_stock_prices f
    JOIN dim_date d ON f.Date = d.Date
) sub
GROUP BY symbol, year, first_close, last_close
ORDER BY year, yearly_return_pct DESC
LIMIT 3;

SELECT
    df.Symbol,
    c.Name,
    df.Year,
    df.Revenue,
    LAG(df.Revenue, 5) OVER (PARTITION BY df.Symbol ORDER BY df.Year) AS Revenue_5_Yr_Ago,
    ROUND(((df.Revenue - LAG(df.Revenue, 5) OVER (PARTITION BY df.Symbol ORDER BY df.Year)) /
           NULLIF(LAG(df.Revenue, 5) OVER (PARTITION BY df.Symbol ORDER BY df.Year), 0)) * 100, 2) AS Growth_5yr_pct
FROM dim_financials df
JOIN dim_company c ON df.Symbol = c.Symbol
WHERE c.MarketCapitalization > 500000000000  -- Only mega-cap stocks
ORDER BY df.Symbol, df.Year;

SELECT
    Symbol,
    Name,
    EPS,
    MarketCapitalization,
    ROUND(MarketCapitalization / NULLIF(EPS, 0), 2) AS Valuation_Efficiency
FROM dim_company
WHERE EPS IS NOT NULL AND MarketCapitalization IS NOT NULL
ORDER BY Valuation_Efficiency ASC
LIMIT 10;

SELECT
    c.Sector,
    f.Symbol,
    ROUND(AVG(f.High - f.Low), 2) AS Avg_Daily_Volatility,
    ROUND(AVG(f.Volume)) AS Avg_Daily_Volume
FROM fact_stock_prices f
JOIN dim_company c ON f.Symbol = c.Symbol
GROUP BY c.Sector, f.Symbol
ORDER BY c.Sector, Avg_Daily_Volatility DESC;

WITH ranked_prices AS (
    SELECT
        Symbol,
        Date,
        Close,
        ROW_NUMBER() OVER (PARTITION BY Symbol ORDER BY Date) AS rn
    FROM fact_stock_prices
),
gains AS (
    SELECT
        a.Symbol,
        a.Date,
        COUNT(*) AS gain_streak
    FROM ranked_prices a
    JOIN ranked_prices b ON a.Symbol = b.Symbol AND a.rn = b.rn + 1
    WHERE a.Close > b.Close
    GROUP BY a.Symbol, a.Date
)
SELECT Symbol, Date, gain_streak
FROM gains
WHERE gain_streak >= 5;


WITH ranked_prices AS (
    SELECT
        Symbol,
        Date,
        Close,
        ROW_NUMBER() OVER (PARTITION BY Symbol ORDER BY Date) AS rn
    FROM fact_stock_prices
),
gains AS (
    SELECT
        a.Symbol,
        a.Date,
        COUNT(*) AS gain_streak
    FROM ranked_prices a
    JOIN ranked_prices b ON a.Symbol = b.Symbol AND a.rn = b.rn + 1
    WHERE a.Close > b.Close
    GROUP BY a.Symbol, a.Date
)
SELECT Symbol, Date, gain_streak
FROM gains
WHERE gain_streak >= 5;


SELECT
    c.Sector,
    f.Symbol,
    d.Year,
    ROUND(AVG(f.Close), 2) AS Avg_Close,
    GROUPING(c.Sector) AS g_sector,
    GROUPING(f.Symbol) AS g_symbol,
    GROUPING(d.Year) AS g_year
FROM fact_stock_prices f
JOIN dim_company c ON f.Symbol = c.Symbol
JOIN dim_date d ON f.Date = d.Date
GROUP BY CUBE (c.Sector, f.Symbol, d.Year)
ORDER BY c.Sector, f.Symbol, d.Year;

SELECT
    d.Year,
    f.Symbol,
    c.Sector,
    ROUND(AVG(f.Close), 2) AS Avg_Close
FROM fact_stock_prices f
JOIN dim_company c ON f.Symbol = c.Symbol
JOIN dim_date d ON f.Date = d.Date
GROUP BY GROUPING SETS (
    (d.Year, f.Symbol),
    (d.Year, c.Sector),
    (c.Sector),
    ()
)
ORDER BY d.Year, f.Symbol;






