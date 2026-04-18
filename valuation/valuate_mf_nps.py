"""
valuate_mf_nps.py — Live MF & NPS Valuation
=============================================
Reads mf_active_holdings.csv, fetches live NAVs from AMFI (for Mutual Funds)
or the cached nps_latest_navs.json (for NPS), and writes to master_valuation.csv.

Inputs  : data/output/mf_active_holdings.csv
          data/output/nps_latest_navs.json  (written by ingest_nps.py)
Output  : data/output/master_valuation.csv  (appends/replaces MF + NPS rows)
"""
from pathlib import Path
import logging
import json
import os
import time

import pandas as pd
from mftool import Mftool

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_last_nps_nav(scheme_name: str, nav_cache_path: str = 'data/output/nps_latest_navs.json') -> dict:
    """
    Returns the most recently fetched NAV and date for an NPS scheme from the JSON cache.

    This cache is populated by fetch_nps_navs.py which calls the npsnav.in API.
    Falls back to sentinel if the cache is missing or the scheme is not found.
    """
    cache = Path(nav_cache_path)
    if not cache.exists():
        return {"nav": 10.0, "date": "unknown"}  # fallback sentinel
    try:
        with open(cache, 'r') as f:
            navs = json.load(f)
        val = navs.get(scheme_name, {"nav": 10.0, "date": "unknown"})
        if isinstance(val, (int, float)):
            return {"nav": float(val), "date": "unknown"}
        return val
    except Exception:
        return {"nav": 10.0, "date": "unknown"}


def fetch_live_valuations(holdings_csv: str) -> tuple[pd.DataFrame, pd.Series] | None:
    """
    Reads universal holdings, fetches live NAV from AMFI or NPS cache,
    and returns a (valuation_df, per_owner_totals) tuple.
    """
    holdings_path = Path(holdings_csv)
    if not holdings_path.exists():
        logger.error("%s not found. Run ingest_mf.py first.", holdings_csv)
        return None

    logger.info("Loading active holdings from %s...", holdings_csv)
    df = pd.read_csv(holdings_path)

    mf = Mftool()
    live_data = []
    logger.info("Fetching live NAVs from AMFI...")

    for _, row in df.iterrows():
        is_nps = str(row['Asset Class']).upper() == 'NPS'
        amfi_code = str(row['Ticker']).strip()
        units = float(row['Units'])

        try:
            if is_nps:
                nps_data = get_last_nps_nav(amfi_code)
                current_nav = nps_data["nav"]
                nav_date = nps_data["date"]
                market_value = units * current_nav
                live_data.append({
                    'Portfolio Owner': row['Portfolio Owner'],
                    'Ticker': amfi_code,
                    'Asset Name': row['Asset Name'],
                    'Asset Class': 'NPS',
                    'Units': units,
                    'Live NAV': current_nav,
                    'Current Value': round(market_value, 2),
                    'Value Date': nav_date,
                })
                logger.info("NPS %s: NAV ₹%s (as of %s)", amfi_code, current_nav, nav_date)
            else:
                quote = mf.get_scheme_quote(amfi_code)
                if quote and 'nav' in quote:
                    current_nav = float(quote['nav'])
                    nav_date = quote.get('last_updated', 'unknown')
                    market_value = units * current_nav
                    live_data.append({
                        'Portfolio Owner': row['Portfolio Owner'],
                        'Ticker': amfi_code,
                        'Asset Name': quote.get('scheme_name', row['Asset Name']),
                        'Asset Class': 'Mutual Fund',
                        'Units': units,
                        'Live NAV': current_nav,
                        'Current Value': round(market_value, 2),
                        'Value Date': nav_date,
                    })
                    logger.info("MF %s: NAV ₹%s (as of %s)", amfi_code, current_nav, nav_date)
                else:
                    logger.warning("Could not fetch NAV for AMFI code %s", amfi_code)
        except Exception as e:
            logger.error("Error fetching %s: %s", amfi_code, e)

        time.sleep(0.1)

    df_valuation = pd.DataFrame(live_data)
    if df_valuation.empty:
        logger.warning("No live NAV data could be fetched — valuation DataFrame is empty.")
        return df_valuation, pd.Series(dtype=float)
    totals = df_valuation.groupby('Portfolio Owner')['Current Value'].sum()
    return df_valuation, totals


# ---------------------------------------------------------------------------
# Main entry-point
# ---------------------------------------------------------------------------

def process_mf_nps_valuations(
    holdings_path: str = 'data/output/mf_active_holdings.csv',
    output_path: str = 'data/output/master_valuation.csv',
) -> None:
    result = fetch_live_valuations(holdings_path)
    if result is None:
        return

    df_final, portfolio_totals = result

    if df_final.empty:
        logger.warning("No MF/NPS valuations were generated — no AMFI NAVs could be fetched. Check AMFI codes.")
        return

    logger.info("MF/NPS LIVE VALUATION")
    for owner, total in portfolio_totals.items():
        logger.info("  %-10s : ₹%,.2f", owner.upper(), total)
    logger.info("  CONSOLIDATED TOTAL: ₹%,.2f", portfolio_totals.sum())

    out = Path(output_path)
    if out.exists():
        existing_val = pd.read_csv(out)
        if 'Asset Class' in existing_val.columns:
            existing_val = existing_val[~existing_val['Asset Class'].isin(['Mutual Fund', 'NPS'])]
        updated_val = pd.concat([existing_val, df_final], ignore_index=True)
        updated_val.to_csv(out, index=False)
    else:
        df_final.to_csv(out, index=False)

    logger.info("MF/NPS valuations saved to '%s'", output_path)


if __name__ == "__main__":
    process_mf_nps_valuations()
