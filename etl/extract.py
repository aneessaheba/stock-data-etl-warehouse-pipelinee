"""
Extract Layer
=============
Downloads historical stock price data and company metadata via yfinance.
Falls back to the pre-built CSVs in data/ when run offline.
"""

import os
import time
import logging
import pandas as pd
import yfinance as yf
from pathlib import Path

logger = logging.getLogger(__name__)

# Stocks tracked in this warehouse
TARGET_SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NFLX", "NVDA", "INTC"]

DATA_DIR = Path(__file__).parent.parent / "data"

# Company metadata (static - enriched from Alpha Vantage / public sources)
COMPANY_METADATA = [
    {
        "Symbol": "AAPL", "Name": "Apple Inc", "AssetType": "Common Stock",
        "Exchange": "NASDAQ", "Sector": "TECHNOLOGY",
        "Industry": "ELECTRONIC COMPUTERS", "Country": "USA", "Currency": "USD",
        "MarketCapitalization": 2959053160000, "EPS": 6.43, "PERatio": 28.5,
        "DividendPerShare": 0.96, "Beta": 1.24, "FiscalYearEnd": "September",
        "OfficialSite": "https://www.apple.com"
    },
    {
        "Symbol": "MSFT", "Name": "Microsoft Corporation", "AssetType": "Common Stock",
        "Exchange": "NASDAQ", "Sector": "TECHNOLOGY",
        "Industry": "SERVICES-PREPACKAGED SOFTWARE", "Country": "USA", "Currency": "USD",
        "MarketCapitalization": 2734069121000, "EPS": 9.81, "PERatio": 35.2,
        "DividendPerShare": 2.72, "Beta": 0.90, "FiscalYearEnd": "June",
        "OfficialSite": "https://www.microsoft.com"
    },
    {
        "Symbol": "GOOGL", "Name": "Alphabet Inc Class A", "AssetType": "Common Stock",
        "Exchange": "NASDAQ", "Sector": "TECHNOLOGY",
        "Industry": "SERVICES-COMPUTER PROGRAMMING, DATA PROCESSING",
        "Country": "USA", "Currency": "USD",
        "MarketCapitalization": 1854733287000, "EPS": 5.80, "PERatio": 25.1,
        "DividendPerShare": 0.0, "Beta": 1.06, "FiscalYearEnd": "December",
        "OfficialSite": "https://www.abc.xyz"
    },
    {
        "Symbol": "AMZN", "Name": "Amazon.com Inc", "AssetType": "Common Stock",
        "Exchange": "NASDAQ", "Sector": "TRADE & SERVICES",
        "Industry": "RETAIL-CATALOG & MAIL-ORDER HOUSES", "Country": "USA", "Currency": "USD",
        "MarketCapitalization": 1831806435000, "EPS": 2.90, "PERatio": 60.4,
        "DividendPerShare": 0.0, "Beta": 1.18, "FiscalYearEnd": "December",
        "OfficialSite": "https://www.amazon.com"
    },
    {
        "Symbol": "TSLA", "Name": "Tesla Inc", "AssetType": "Common Stock",
        "Exchange": "NASDAQ", "Sector": "MANUFACTURING",
        "Industry": "MOTOR VEHICLES & PASSENGER CAR BODIES", "Country": "USA", "Currency": "USD",
        "MarketCapitalization": 776371372000, "EPS": 3.62, "PERatio": 65.2,
        "DividendPerShare": 0.0, "Beta": 2.30, "FiscalYearEnd": "December",
        "OfficialSite": "https://www.tesla.com"
    },
    {
        "Symbol": "META", "Name": "Meta Platforms Inc", "AssetType": "Common Stock",
        "Exchange": "NASDAQ", "Sector": "TECHNOLOGY",
        "Industry": "SERVICES-COMPUTER PROGRAMMING, DATA PROCESSING",
        "Country": "USA", "Currency": "USD",
        "MarketCapitalization": 1270579855000, "EPS": 14.87, "PERatio": 23.8,
        "DividendPerShare": 0.0, "Beta": 1.20, "FiscalYearEnd": "December",
        "OfficialSite": "https://www.meta.com"
    },
    {
        "Symbol": "NFLX", "Name": "Netflix Inc", "AssetType": "Common Stock",
        "Exchange": "NASDAQ", "Sector": "TRADE & SERVICES",
        "Industry": "SERVICES-VIDEO TAPE RENTAL", "Country": "USA", "Currency": "USD",
        "MarketCapitalization": 416220414000, "EPS": 12.03, "PERatio": 45.0,
        "DividendPerShare": 0.0, "Beta": 1.33, "FiscalYearEnd": "December",
        "OfficialSite": "https://www.netflix.com"
    },
    {
        "Symbol": "NVDA", "Name": "NVIDIA Corporation", "AssetType": "Common Stock",
        "Exchange": "NASDAQ", "Sector": "MANUFACTURING",
        "Industry": "SEMICONDUCTORS & RELATED DEVICES", "Country": "USA", "Currency": "USD",
        "MarketCapitalization": 2476355879000, "EPS": 11.93, "PERatio": 52.6,
        "DividendPerShare": 0.16, "Beta": 1.68, "FiscalYearEnd": "January",
        "OfficialSite": "https://www.nvidia.com"
    },
    {
        "Symbol": "INTC", "Name": "Intel Corporation", "AssetType": "Common Stock",
        "Exchange": "NASDAQ", "Sector": "MANUFACTURING",
        "Industry": "SEMICONDUCTORS & RELATED DEVICES", "Country": "USA", "Currency": "USD",
        "MarketCapitalization": 82545967000, "EPS": -0.47, "PERatio": None,
        "DividendPerShare": 0.50, "Beta": 0.89, "FiscalYearEnd": "December",
        "OfficialSite": "https://www.intel.com"
    },
]


def extract_stock_prices(
    symbols: list[str] = TARGET_SYMBOLS,
    start: str = "2010-01-01",
    end: str = "2024-12-31",
    use_cache: bool = True,
) -> pd.DataFrame:
    """
    Download OHLCV data for all symbols via yfinance.

    Parameters
    ----------
    symbols  : list of ticker symbols
    start    : ISO date string, period start
    end      : ISO date string, period end
    use_cache: if True and data/fact_stock_prices.csv exists, load from disk

    Returns
    -------
    DataFrame with columns: Date, Open, High, Low, Close, Adj_Close, Volume, Symbol
    """
    cache_path = DATA_DIR / "fact_stock_prices.csv"
    if use_cache and cache_path.exists():
        logger.info("Loading stock prices from cache: %s", cache_path)
        df = pd.read_csv(cache_path, parse_dates=["Date"])
        return df

    logger.info("Downloading stock prices for %d symbols via yfinance...", len(symbols))
    frames = []
    for symbol in symbols:
        try:
            ticker = yf.Ticker(symbol)
            raw = ticker.history(start=start, end=end, auto_adjust=True)
            if raw.empty:
                logger.warning("No data returned for %s", symbol)
                continue
            raw = raw.reset_index()
            raw = raw.rename(columns={
                "Date": "Date",
                "Open": "Open",
                "High": "High",
                "Low": "Low",
                "Close": "Close",
                "Volume": "Volume",
            })
            # yfinance returns timezone-aware dates; strip timezone
            raw["Date"] = pd.to_datetime(raw["Date"]).dt.tz_localize(None)
            raw["Symbol"] = symbol
            raw["Adj_Close"] = raw["Close"]  # already adjusted when auto_adjust=True
            frames.append(raw[["Date", "Open", "High", "Low", "Close", "Adj_Close", "Volume", "Symbol"]])
            logger.info("  Downloaded %d rows for %s", len(raw), symbol)
            time.sleep(0.3)  # polite delay
        except Exception as exc:
            logger.error("Failed to download %s: %s", symbol, exc)

    if not frames:
        raise RuntimeError("No stock data could be downloaded. Check your internet connection.")

    df = pd.concat(frames, ignore_index=True)
    logger.info("Total rows downloaded: %d", len(df))
    return df


def extract_company_metadata() -> pd.DataFrame:
    """Return the curated company dimension DataFrame."""
    return pd.DataFrame(COMPANY_METADATA)


def extract_financials(use_cache: bool = True) -> pd.DataFrame:
    """
    Load financial statements data.
    Uses pre-built CSV (originally from Kaggle dataset:
    'Financial Statements of Major Companies 2009-2023').
    """
    cache_path = DATA_DIR / "dim_financials.csv"
    if use_cache and cache_path.exists():
        logger.info("Loading financials from cache: %s", cache_path)
        return pd.read_csv(cache_path)
    raise FileNotFoundError(
        f"Financials CSV not found at {cache_path}. "
        "Download from: https://www.kaggle.com/datasets/rish59/financial-statements-of-major-companies2009-2023"
    )
