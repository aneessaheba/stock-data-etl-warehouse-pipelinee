-- =============================================================================
-- Monthly Aggregation
-- =============================================================================
-- Upserts monthly OHLCV summaries from fact_stock_price_daily into
-- fact_stock_price_monthly.  Run by the monthly_aggregate_dag in Airflow.
-- =============================================================================

INSERT INTO fact_stock_price_monthly
    (company_key, month, avg_open, avg_high, avg_low, avg_close, total_volume, trading_days)
SELECT
    company_key,
    DATE_TRUNC('month', date)::DATE             AS month,
    ROUND(AVG(open)::NUMERIC,   4)::FLOAT       AS avg_open,
    ROUND(AVG(high)::NUMERIC,   4)::FLOAT       AS avg_high,
    ROUND(AVG(low)::NUMERIC,    4)::FLOAT       AS avg_low,
    ROUND(AVG(close)::NUMERIC,  4)::FLOAT       AS avg_close,
    SUM(volume)                                  AS total_volume,
    COUNT(*)                                     AS trading_days
FROM fact_stock_price_daily
GROUP BY company_key, DATE_TRUNC('month', date)
ON CONFLICT (company_key, month) DO UPDATE SET
    avg_open     = EXCLUDED.avg_open,
    avg_high     = EXCLUDED.avg_high,
    avg_low      = EXCLUDED.avg_low,
    avg_close    = EXCLUDED.avg_close,
    total_volume = EXCLUDED.total_volume,
    trading_days = EXCLUDED.trading_days;
