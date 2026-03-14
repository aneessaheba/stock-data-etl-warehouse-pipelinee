"""
Stock Market ETL Pipeline
=========================
Extract → Transform → Load pipeline for stock market data warehouse.

Modules:
    extract   - Fetch stock prices and company data via yfinance
    transform - Clean and build star-schema dimensions
    load      - Load DataFrames into PostgreSQL and export CSVs
    pipeline  - Orchestrates the full ETL run
"""
