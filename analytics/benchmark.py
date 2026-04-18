"""
benchmark.py — Nifty 50 Benchmark Data Fetch
==============================================
Fetches 15 years of Nifty 50 daily close prices and caches them locally.
Used by the dashboard for benchmark comparison overlays.

Output : data/output/nifty50_history.csv
"""
import logging
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)


def fetch_nifty50_history(output_path: str = 'data/output/nifty50_history.csv') -> None:
    """Fetches 15 years of Nifty 50 (^NSEI) daily close prices and saves to CSV."""
    logger.info("Fetching Nifty 50 (^NSEI) historical daily close prices...")

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    try:
        nifty = yf.Ticker("^NSEI")
        hist = nifty.history(period="15y")

        if hist.empty:
            logger.warning("Failed to fetch Nifty 50 data from Yahoo Finance.")
            return

        hist = hist.reset_index()
        hist['Date'] = pd.to_datetime(hist['Date']).dt.tz_localize(None).dt.date
        hist = hist[['Date', 'Close']].rename(columns={'Close': 'Nifty_Close'})
        hist.to_csv(out, index=False)
        logger.info("Cached %d days of Nifty 50 prices to '%s'", len(hist), output_path)

    except Exception as e:
        logger.error("Error fetching Nifty 50 data: %s", e)


if __name__ == "__main__":
    fetch_nifty50_history()
