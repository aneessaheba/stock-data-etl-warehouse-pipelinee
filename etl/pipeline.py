"""
ETL Pipeline Orchestrator
=========================
Runs the full Extract → Transform → Load pipeline.

Usage:
    python -m etl.pipeline                    # run with defaults (cache=True)
    python -m etl.pipeline --refresh          # force re-download from yfinance
    python -m etl.pipeline --no-db            # CSV only, skip PostgreSQL load
    python -m etl.pipeline --start 2015-01-01 --end 2024-12-31
"""

import argparse
import logging
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

from etl.extract import (
    extract_stock_prices,
    extract_company_metadata,
    extract_financials,
)
from etl.transform import (
    transform_dim_company,
    transform_dim_date,
    transform_dim_financials,
    transform_fact_stock_prices,
    validate_star_schema,
)
from etl.load import export_to_csv, load_to_postgres

# Load .env for DB credentials
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("pipeline")


def run(
    start: str = "2010-01-01",
    end: str = "2024-12-31",
    use_cache: bool = True,
    load_db: bool = True,
) -> dict:
    """
    Execute the full ETL pipeline.

    Parameters
    ----------
    start     : start date for stock price download
    end       : end date for stock price download
    use_cache : use existing CSVs instead of re-downloading
    load_db   : whether to load into PostgreSQL

    Returns
    -------
    dict with all four transformed DataFrames
    """
    t0 = time.time()
    logger.info("=" * 60)
    logger.info("Stock Market ETL Pipeline — START")
    logger.info("=" * 60)

    # ------------------------------------------------------------------ EXTRACT
    logger.info("[1/3] EXTRACT")
    raw_prices      = extract_stock_prices(use_cache=use_cache, start=start, end=end)
    raw_company     = extract_company_metadata()
    raw_financials  = extract_financials(use_cache=use_cache)

    # ----------------------------------------------------------------- TRANSFORM
    logger.info("[2/3] TRANSFORM")
    dim_company     = transform_dim_company(raw_company)
    fact_prices     = transform_fact_stock_prices(raw_prices)
    dim_date        = transform_dim_date(fact_prices)
    dim_financials  = transform_dim_financials(raw_financials)

    # Validation
    validation = validate_star_schema(fact_prices, dim_company, dim_date)
    if validation["valid"]:
        logger.info("Star-schema validation: PASSED")
    else:
        logger.warning("Star-schema validation: WARNINGS — %s", validation)

    # ----------------------------------------------------------------------- LOAD
    logger.info("[3/3] LOAD")
    export_to_csv(dim_company, dim_date, dim_financials, fact_prices)

    if load_db:
        try:
            load_to_postgres(dim_company, dim_date, dim_financials, fact_prices)
        except Exception:
            logger.warning("DB load skipped (see error above). CSVs still written.")

    elapsed = time.time() - t0
    logger.info("=" * 60)
    logger.info("Pipeline complete in %.1f seconds.", elapsed)
    logger.info("=" * 60)

    return {
        "dim_company":       dim_company,
        "dim_date":          dim_date,
        "dim_financials":    dim_financials,
        "fact_stock_prices": fact_prices,
        "validation":        validation,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stock Market ETL Pipeline")
    parser.add_argument("--refresh",  action="store_true",
                        help="Re-download data instead of using cached CSVs")
    parser.add_argument("--no-db",   action="store_true",
                        help="Skip PostgreSQL load (CSV export only)")
    parser.add_argument("--start",   default="2010-01-01", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end",     default="2024-12-31", help="End date (YYYY-MM-DD)")
    args = parser.parse_args()

    results = run(
        start=args.start,
        end=args.end,
        use_cache=not args.refresh,
        load_db=not args.no_db,
    )
    sys.exit(0)
