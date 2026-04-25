"""
run_all.py — Portfolio Consolidator Pipeline Orchestrator
==========================================================
Runs the complete ingestion and valuation pipeline in dependency order.
Stop-on-failure ensures partial data never reaches the dashboard.

Usage:
    uv run python run_all.py               # Full run: ingestion + valuation + analytics
    uv run python run_all.py --refresh     # Refresh only: valuation + analytics (no ingestion)
    LOG_LEVEL=DEBUG uv run python run_all.py   # Verbose output

Pipeline stages:
    INGESTION_PIPELINE  — Steps 1a–1g: parse raw statement files into master_ledger.csv
    REFRESH_PIPELINE    — Steps 2a–6:  fetch live NAVs, compute XIRR/allocations, export AI summary
"""

import argparse
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
# Pipeline Stage Definitions
# ---------------------------------------------------------------------------

# Stage 1: Parse raw statement files → master_ledger.csv
# Run this whenever new statement files are uploaded via Data Management.
INGESTION_PIPELINE: list[str] = [
    "ingestion/ingest_mf.py",          # Step 1a: MF Ingestion (CAMS + Groww)
    "ingestion/ingest_stocks.py",       # Step 1b: Stock Ingestion (Groww)
    "ingestion/ingest_nps.py",          # Step 1c: NPS Ingestion (KFintech PDF)
    "ingestion/ingest_epf.py",          # Step 1d: EPF Ingestion (manual config)
    "ingestion/ingest_fd.py",           # Step 1e: FD Ingestion (manual config)
    "ingestion/ingest_global.py",       # Step 1f: Global Holdings Ingestion (IBKR manual CSV)
    "valuation/fetch_nps_navs.py",      # Step 1g: NPS Live NAV Cache (npsnav.in API)
]

# Stage 2: Fetch live prices + compute analytics → dashboard-ready CSVs.
# Safe to run at any time without re-uploading files (uses existing master_ledger.csv).
REFRESH_PIPELINE: list[str] = [
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
    parser = argparse.ArgumentParser(
        description="Portfolio Consolidator Pipeline Orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  uv run python run_all.py             # Full run (ingestion + refresh)
  uv run python run_all.py --refresh   # Refresh prices only (no file ingestion)
        """
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help=(
            "Run valuation + analytics only (Steps 2–6). "
            "Skips file ingestion — uses the existing master_ledger.csv. "
            "Use this to update live prices without re-uploading statement files."
        ),
    )
    args = parser.parse_args()

    if args.refresh:
        pipeline = REFRESH_PIPELINE
        logger.info("=" * 55)
        logger.info("PORTFOLIO CONSOLIDATOR: NAV REFRESH (Steps 2–6)")
        logger.info("Skipping ingestion — using existing master_ledger.csv")
        logger.info("=" * 55)
    else:
        pipeline = INGESTION_PIPELINE + REFRESH_PIPELINE
        logger.info("=" * 55)
        logger.info("PORTFOLIO CONSOLIDATOR: FULL PIPELINE (Steps 1–6)")
        logger.info("=" * 55)

    for module in pipeline:
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
