-- =============================================================================
-- Load CSV files into PostgreSQL via COPY
-- Run from the repo root:
--   psql -U admin -d stock_db -f sql/load_data.sql
-- Or inside psql: \i sql/load_data.sql
-- =============================================================================

-- Adjust the file paths below to absolute paths on your machine if needed.
-- The \copy meta-command works from the psql client; COPY requires superuser.

\echo 'Loading dim_company...'
\copy dim_company(Symbol,Name,AssetType,Exchange,Sector,Industry,Country,Currency,MarketCapitalization,EPS,PERatio,DividendPerShare,Beta,FiscalYearEnd,OfficialSite)
  FROM 'data/dim_company.csv' CSV HEADER;

\echo 'Loading dim_date...'
\copy dim_date(Date,Year,Quarter,Month,Month_Name,Day,Day_Name,Week,Weekday,Is_Weekend,Day_Of_Year,Is_Month_Start,Is_Month_End,Is_Quarter_Start,Is_Quarter_End,Is_Year_Start,Is_Year_End)
  FROM 'data/dim_date.csv' CSV HEADER;

\echo 'Loading dim_financials...'
\copy dim_financials(Year,Symbol,Category,Market_Cap_in_B_USD,Revenue,Gross_Profit,Net_Income,Earning_Per_Share,EBITDA,Share_Holder_Equity,Cash_Flow_Operating,Cash_Flow_Investing,Cash_Flow_Financial,Current_Ratio,Debt_Equity_Ratio,ROE,ROA,ROI,Net_Profit_Margin,Free_Cash_Flow_Per_Share,Return_on_Tangible_Equity,Number_of_Employees,Inflation_Rate_US)
  FROM 'data/dim_financials.csv' CSV HEADER;

\echo 'Loading fact_stock_prices...'
\copy fact_stock_prices(Date,Open,High,Low,Close,Adj_Close,Volume,Symbol,Daily_Return_Pct,Intraday_Range)
  FROM 'data/fact_stock_prices.csv' CSV HEADER;

\echo 'Done. Row counts:'
SELECT 'dim_company'       AS tbl, COUNT(*) FROM dim_company
UNION ALL
SELECT 'dim_date',          COUNT(*) FROM dim_date
UNION ALL
SELECT 'dim_financials',    COUNT(*) FROM dim_financials
UNION ALL
SELECT 'fact_stock_prices', COUNT(*) FROM fact_stock_prices;
