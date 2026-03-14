"""
Transform Layer
===============
Cleans raw extracted data and builds the star-schema dimensions:
  - dim_company
  - dim_date
  - dim_financials
  - fact_stock_prices
"""

import logging
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# dim_company
# ---------------------------------------------------------------------------

def transform_dim_company(raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and type-cast the company dimension.

    Expected input columns (from extract.extract_company_metadata):
        Symbol, Name, AssetType, Exchange, Sector, Industry, Country, Currency,
        MarketCapitalization, EPS, PERatio, DividendPerShare, Beta,
        FiscalYearEnd, OfficialSite
    """
    df = raw_df.copy()

    # Normalise strings
    for col in ["Symbol", "Exchange", "Sector", "Industry", "Country", "Currency"]:
        df[col] = df[col].str.strip().str.upper()

    df["Name"] = df["Name"].str.strip()
    df["FiscalYearEnd"] = df["FiscalYearEnd"].str.strip()
    df["OfficialSite"] = df["OfficialSite"].str.strip()

    # Numeric casts
    for col in ["MarketCapitalization", "EPS", "PERatio", "DividendPerShare", "Beta"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Remove duplicates on primary key
    df = df.drop_duplicates(subset=["Symbol"])
    df = df.sort_values("Symbol").reset_index(drop=True)

    logger.info("dim_company: %d rows", len(df))
    return df


# ---------------------------------------------------------------------------
# dim_date
# ---------------------------------------------------------------------------

def transform_dim_date(fact_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a comprehensive date dimension from all trading dates in fact_stock_prices.

    Input: fact_df must have a 'Date' column (datetime-like).
    """
    dates = pd.to_datetime(fact_df["Date"].dropna().unique())
    df = pd.DataFrame({"Date": sorted(dates)})

    df["Year"]             = df["Date"].dt.year
    df["Quarter"]          = df["Date"].dt.quarter
    df["Month"]            = df["Date"].dt.month
    df["Month_Name"]       = df["Date"].dt.month_name()
    df["Day"]              = df["Date"].dt.day
    df["Day_Name"]         = df["Date"].dt.day_name()
    df["Week"]             = df["Date"].dt.isocalendar().week.astype(int)
    df["Weekday"]          = df["Date"].dt.weekday + 1   # 1=Mon … 7=Sun
    df["Is_Weekend"]       = df["Weekday"].isin([6, 7])
    df["Day_Of_Year"]      = df["Date"].dt.dayofyear
    df["Is_Month_Start"]   = df["Date"].dt.is_month_start
    df["Is_Month_End"]     = df["Date"].dt.is_month_end
    df["Is_Quarter_Start"] = df["Date"].dt.is_quarter_start
    df["Is_Quarter_End"]   = df["Date"].dt.is_quarter_end
    df["Is_Year_Start"]    = df["Date"].dt.is_year_start
    df["Is_Year_End"]      = df["Date"].dt.is_year_end

    logger.info("dim_date: %d rows spanning %s → %s",
                len(df), df["Date"].min().date(), df["Date"].max().date())
    return df


# ---------------------------------------------------------------------------
# dim_financials
# ---------------------------------------------------------------------------

def transform_dim_financials(raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the financial statements dimension.

    Expected input columns (from the Kaggle financial statements CSV):
        Year, Symbol (Company), Category, Market_Cap, Revenue, Gross_Profit,
        Net_Income, Earning_Per_Share, EBITDA, Share_Holder_Equity,
        Cash_Flow_Operating, Cash_Flow_Investing, Cash_Flow_Financial,
        Current_Ratio, Debt/Equity_Ratio, ROE, ROA, ROI,
        Net_Profit_Margin, Free_Cash_Flow_per_Share,
        Return_on_Tangible_Equity, Number_of_Employees, Inflation_Rate
    """
    df = raw_df.copy()

    # Rename columns to be SQL-safe
    rename_map = {
        "Company":                    "Symbol",
        "Market_Cap":                 "Market_Cap_in_B_USD",
        "Debt/Equity_Ratio":          "Debt_Equity_Ratio",
        "Free_Cash_Flow_per_Share":   "Free_Cash_Flow_Per_Share",
        "Inflation_Rate":             "Inflation_Rate_US",
    }
    df = df.rename(columns=rename_map)

    # Keep only target symbols
    target = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NFLX", "NVDA", "INTC"]
    if "Symbol" in df.columns:
        df = df[df["Symbol"].isin(target)]

    # Fill missing numeric values with 0
    numeric_cols = df.select_dtypes(include="number").columns
    df[numeric_cols] = df[numeric_cols].fillna(0)

    # Remove duplicate (Symbol, Year) pairs
    df = df.drop_duplicates(subset=["Symbol", "Year"])
    df = df.sort_values(["Symbol", "Year"]).reset_index(drop=True)

    logger.info("dim_financials: %d rows", len(df))
    return df


# ---------------------------------------------------------------------------
# fact_stock_prices
# ---------------------------------------------------------------------------

def transform_fact_stock_prices(raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardise the stock price fact table.

    Input columns: Date, Open, High, Low, Close, Adj_Close, Volume, Symbol
    """
    df = raw_df.copy()

    # Standardise column names from various sources
    col_map = {
        "AdjClose":  "Adj_Close",
        "Adj Close": "Adj_Close",
    }
    df = df.rename(columns=col_map)

    df["Date"]   = pd.to_datetime(df["Date"], errors="coerce")
    df["Symbol"] = df["Symbol"].str.strip().str.upper()

    # Cast numerics
    for col in ["Open", "High", "Low", "Close", "Adj_Close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce").astype("Int64")

    # Drop rows with missing key values
    df = df.dropna(subset=["Date", "Open", "Close", "Symbol"])

    # No negative prices
    df = df[(df["Open"] > 0) & (df["Close"] > 0)]

    # Deduplicate on primary key
    df = df.drop_duplicates(subset=["Date", "Symbol"])
    df = df.sort_values(["Symbol", "Date"]).reset_index(drop=True)

    # Derived: intraday return % (open-to-close, NOT close-to-close daily return)
    df["Daily_Return_Pct"] = (
        (df["Close"] - df["Open"]) / df["Open"] * 100
    ).round(4)

    # Derived: intraday range
    df["Intraday_Range"] = (df["High"] - df["Low"]).round(4)

    logger.info("fact_stock_prices: %d rows, %d symbols",
                len(df), df["Symbol"].nunique())
    return df


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def validate_star_schema(
    fact: pd.DataFrame,
    dim_company: pd.DataFrame,
    dim_date: pd.DataFrame,
) -> dict:
    """
    Verify referential integrity between fact table and dimensions.
    Returns a dict with validation results.
    """
    results = {}

    # All symbols in fact must exist in dim_company
    fact_symbols  = set(fact["Symbol"].unique())
    dim_symbols   = set(dim_company["Symbol"].unique())
    orphan_symbols = fact_symbols - dim_symbols
    results["orphan_symbols"] = sorted(orphan_symbols)

    # All dates in fact must exist in dim_date
    fact_dates = set(fact["Date"].dt.normalize().unique())
    dim_dates  = set(pd.to_datetime(dim_date["Date"]).unique())
    orphan_dates = fact_dates - dim_dates
    results["orphan_dates_count"] = len(orphan_dates)

    # Null checks
    results["fact_null_close"]  = int(fact["Close"].isna().sum())
    results["fact_null_symbol"] = int(fact["Symbol"].isna().sum())
    results["fact_null_date"]   = int(fact["Date"].isna().sum())

    results["valid"] = (
        len(orphan_symbols) == 0
        and len(orphan_dates) == 0
        and results["fact_null_close"] == 0
        and results["fact_null_symbol"] == 0
        and results["fact_null_date"] == 0
    )

    return results
