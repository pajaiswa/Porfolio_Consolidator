"""
valuate_global.py — Global Holdings Live Valuation (IBKR)
==========================================================
Fetches live USD prices for each IBKR position via yfinance, converts
to INR using the live USDINR=X FX rate, and writes Current Value rows
into master_valuation.csv.

Input  : data/input/global/global_transactions.csv
         Columns: Owner, Ticker, Asset_Name, Transaction_Type,
                  Shares, INR_Amount, Trade_Date
Output : data/output/master_valuation.csv  (replaces Global Holdings rows)
"""
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

TRANSACTION_FILE = 'data/input/global/global_transactions.csv'
VALUATION_FILE = 'data/output/master_valuation.csv'
ASSET_CLASS = 'Global Holdings'
FX_TICKER = 'USDINR=X'


def _fetch_live_price(ticker: str) -> float | None:
    """Fetch the latest market price for a yfinance ticker symbol."""
    try:
        info = yf.Ticker(ticker).fast_info
        price = info.get('lastPrice') or info.get('last_price')
        if price and float(price) > 0:
            return float(price)
    except Exception as e:
        logger.warning("Could not fetch price for %s: %s", ticker, e)
    return None


def _fetch_usdinr() -> float:
    """Fetch the live USD/INR FX rate via yfinance."""
    rate = _fetch_live_price(FX_TICKER)
    if rate:
        logger.info("Live USD/INR rate: %.4f", rate)
        return rate
    # Fallback: safe approximate rate — logs a warning so the user knows
    fallback = 84.0
    logger.warning(
        "Could not fetch live USDINR rate — using fallback ₹%.2f. "
        "Check internet connectivity or yfinance availability.", fallback
    )
    return fallback


def valuate_global_holdings(
    txn_path: str = TRANSACTION_FILE,
    valuation_path: str = VALUATION_FILE,
) -> bool:
    """
    Computes net share positions from the transaction ledger, fetches
    live USD prices, converts to INR and writes to master_valuation.csv.
    """
    logger.info("Valuating Global Holdings (IBKR)...")

    cfg = Path(txn_path)
    if not cfg.exists():
        logger.warning("Global transactions file not found at %s — skipping.", txn_path)
        return True

    df_txn = pd.read_csv(cfg).dropna(how='all')
    if df_txn.empty:
        logger.info("No Global Holdings transactions found — skipping valuation.")
        return True

    # Coerce numeric types
    df_txn['Shares'] = pd.to_numeric(df_txn['Shares'], errors='coerce').fillna(0.0)
    df_txn['Ticker'] = df_txn['Ticker'].astype(str).str.strip().str.upper()
    df_txn['Owner'] = df_txn['Owner'].astype(str).str.strip().str.title()
    df_txn['Asset_Name'] = df_txn['Asset_Name'].astype(str).str.strip()
    df_txn['Transaction_Type'] = df_txn['Transaction_Type'].astype(str).str.strip().str.title()

    # Compute net shares per (Owner, Ticker) — Buy adds, Sell subtracts
    df_txn['Signed_Shares'] = df_txn.apply(
        lambda r: r['Shares'] if r['Transaction_Type'] == 'Buy' else -abs(r['Shares']),
        axis=1,
    )

    net_positions = (
        df_txn.groupby(['Owner', 'Ticker', 'Asset_Name'], as_index=False)
        .agg(Net_Shares=('Signed_Shares', 'sum'))
    )
    net_positions = net_positions[net_positions['Net_Shares'] > 0.0001].copy()

    if net_positions.empty:
        logger.info("All Global Holdings positions are fully sold — nothing to value.")
        return True

    # Fetch live FX rate once
    usdinr = _fetch_usdinr()
    today_str = datetime.now().strftime('%d-%b-%Y')

    live_records = []
    for _, pos in net_positions.iterrows():
        ticker = pos['Ticker']
        owner = pos['Owner']
        name = pos['Asset_Name']
        net_shares = float(pos['Net_Shares'])

        usd_price = _fetch_live_price(ticker)
        if usd_price is None:
            logger.error(
                "Skipping %s (%s) — could not fetch live price from yfinance. "
                "Check that the ticker symbol is correct.", name, ticker
            )
            continue

        inr_value = net_shares * usd_price * usdinr
        inr_nav = usd_price * usdinr  # per-share INR price

        live_records.append({
            'Portfolio Owner': owner,
            'Ticker': ticker,
            'Asset Name': name,
            'Asset Class': ASSET_CLASS,
            'Units': round(net_shares, 6),
            'Live NAV': round(inr_nav, 4),
            'Current Value': round(inr_value, 2),
            'Value Date': today_str,
        })

        logger.info(
            "  %-10s | %-10s | %.4f shares × $%.2f × ₹%.2f/$ = ₹%,.2f",
            owner, ticker, net_shares, usd_price, usdinr, inr_value,
        )

    if not live_records:
        logger.warning("No Global Holdings could be valued (price fetch failed for all tickers).")
        return False

    df_new_val = pd.DataFrame(live_records)

    # Merge: replace existing Global Holdings rows only
    out = Path(valuation_path)
    if out.exists():
        df_master = pd.read_csv(out)
        df_master = df_master[df_master['Asset Class'] != ASSET_CLASS]
        pd.concat([df_master, df_new_val], ignore_index=True).to_csv(out, index=False)
    else:
        df_new_val.to_csv(out, index=False)

    logger.info(
        "Global Holdings valuation complete — %d positions saved to %s",
        len(live_records), valuation_path,
    )
    return True


if __name__ == "__main__":
    import logging as _logging
    _logging.basicConfig(level=_logging.INFO, format="%(asctime)s [%(levelname)-8s] %(message)s", datefmt="%H:%M:%S")
    valuate_global_holdings()
