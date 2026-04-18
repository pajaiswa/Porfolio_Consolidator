"""
run_all.py — Full Pipeline Orchestrator
=========================================
Runs the complete Portfolio Consolidator ingestion and valuation pipeline
in dependency order.  Stop-on-failure ensures partial data never reaches
the dashboard.

Usage:
    uv run python run_all.py
    LOG_LEVEL=DEBUG uv run python run_all.py   # verbose output
"""
import logging
import os
import sys
import subprocess
from pathlib import Path

import pandas as pd

from core.logging_config import configure_logging

configure_logging()
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def run_module(module_path: str) -> bool:
    """Runs a pipeline module file using `uv run python <module_path>`."""
    logger.info("─" * 55)
    logger.info("🚀 RUNNING: %s", module_path)
    logger.info("─" * 55)
    try:
        subprocess.run(
            ["uv", "run", "python", module_path],
            check=True,
            text=True,
            encoding='utf-8',
        )
        return True
    except subprocess.CalledProcessError as e:
        logger.error("Failed to execute %s: %s", module_path, e)
        return False


def validate_live_pricing(valuation_file: str = 'data/output/master_valuation.csv') -> bool:
    """
    Validates that the generated valuation file contains live prices
    and does not rely on static dummy values (zero / NaN NAVs).
    """
    path = Path(valuation_file)
    if not path.exists():
        logger.error("Valuation file missing: %s", valuation_file)
        return False

    df = pd.read_csv(path)
    if df.empty:
        logger.error("Valuation file is empty: %s", valuation_file)
        return False

    if df['Live NAV'].isna().any() or (df['Live NAV'] == 0).any():
        logger.error("Detected empty or zero NAV/Price values — live fetch may have failed.")
        return False

    return True


# ---------------------------------------------------------------------------
# Pipeline definition
# ---------------------------------------------------------------------------

# Ordered list of pipeline modules (relative paths from project root).
PIPELINE: list[str] = [
    "ingestion/ingest_mf.py",          # Step 1a: MF Ingestion (CAMS + Groww)
    "ingestion/ingest_stocks.py",       # Step 1b: Stock Ingestion (Groww)
    "ingestion/ingest_nps.py",          # Step 1c: NPS Ingestion (KFintech PDF)
    "ingestion/ingest_epf.py",          # Step 1d: EPF Ingestion (manual config)
    "ingestion/ingest_fd.py",           # Step 1e: FD Ingestion (manual config)
    "ingestion/ingest_global.py",       # Step 1f: Global Holdings Ingestion (IBKR manual CSV)
    "valuation/fetch_nps_navs.py",      # Step 1g: NPS Live NAV Cache (npsnav.in API)
    "valuation/valuate_mf_nps.py",      # Step 2a: MF & NPS Live Valuations (AMFI)
    "valuation/valuate_stocks.py",      # Step 2b: Stock Live Valuations (yfinance)
    "valuation/valuate_epf.py",         # Step 2c: EPF Live Valuations (accrual)
    "valuation/valuate_fd.py",          # Step 2d: FD Live Valuations (accrual)
    "valuation/valuate_global.py",      # Step 2e: Global Holdings Live Valuations (yfinance + USDINR FX)
    "analytics/calculate_xirr.py",      # Step 3:  XIRR & Performance Metrics
    "analytics/calc_allocations.py",    # Step 4a: Asset Sub-Class Categorisation
    "analytics/compute_equity_lookthrough.py",  # Step 4b: Equity Look-Through (Cap-Size + Geography)
    "analytics/benchmark.py",           # Step 5:  Nifty 50 Benchmark Cache
    "analytics/export_ai_summary.py",   # Step 6:  Export unified dataset for AI Advisor
]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    logger.info("PORTFOLIO CONSOLIDATOR: FULL PIPELINE START")

    for module in PIPELINE:
        if not Path(module).exists():
            logger.critical("Pipeline module not found: '%s'", module)
            sys.exit(1)

        if not run_module(module):
            logger.critical("Pipeline halted due to error in %s", module)
            sys.exit(1)

    # Post-run validation
    logger.info("─" * 55)
    logger.info("VALIDATING DYNAMIC PRICING")
    logger.info("─" * 55)

    if validate_live_pricing():
        logger.info("Pipeline complete. All valuations are dynamically priced.")
        logger.info("Check 'data/output/performance_metrics.csv' for the latest XIRR.")
    else:
        logger.warning("Validation warning — check NAV values in master_valuation.csv.")


if __name__ == "__main__":
    main()
