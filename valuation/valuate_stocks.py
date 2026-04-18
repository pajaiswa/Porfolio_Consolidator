"""
valuate_stocks.py — Live Stock Valuation
==========================================
Calculates net active stock holdings and fetches live prices via yfinance,
with a fallback to nsepython for instruments that fail on yfinance (e.g. SGBs).

Input  : data/output/master_ledger.csv
Output : data/output/master_valuation.csv  (appends/replaces STOCK rows)
"""
import logging
from pathlib import Path

import pandas as pd
import yfinance as yf
from nsepython import nse_eq

logger = logging.getLogger(__name__)


def fetch_live_stock_valuations(df_ledger: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series] | None:
    """
    Filters STOCK holdings from a ledger DataFrame, computes net units,
    and fetches live INR prices.

    Returns (valuation_df, per_owner_totals) or None on failure.
    """
    df_stocks = df_ledger[df_ledger['Asset Class'] == 'STOCK'].copy()
    if df_stocks.empty:
        logger.warning("No STOCK holdings found in ledger.")
        return None

    sell_mask = df_stocks['Transaction Type'].str.contains('SELL', case=False, na=False)
    df_stocks.loc[sell_mask, 'Units'] = df_stocks.loc[sell_mask, 'Units'].abs() * -1

    active_holdings = df_stocks.groupby(
        ['Portfolio Owner', 'Ticker', 'Asset Name'], as_index=False
    )['Units'].sum()
    active_holdings = active_holdings[active_holdings['Units'] > 0.01].copy()

    if active_holdings.empty:
        logger.warning("No active STOCK holdings > 0.01 units found.")
        return None

    live_data: list[dict] = []
    logger.info("Fetching live stock prices via yfinance...")

    for _, row in active_holdings.iterrows():
        base_ticker = str(row['Ticker']).strip()
        units = float(row['Units'])
        yf_ticker = f"{base_ticker}.NS"

        try:
            stock = yf.Ticker(yf_ticker)
            current_price = stock.fast_info.last_price
            nav_date = "unknown"

            # Always try to fetch history to grab the latest date
            hist = stock.history(period="1d")
            if not hist.empty:
                nav_date = hist.index[-1].strftime('%d-%b-%Y')
                if current_price is None or pd.isna(current_price):
                    current_price = hist['Close'].iloc[-1]

            if current_price is None or pd.isna(current_price):
                raise ValueError("No price data returned")

            live_data.append({
                'Portfolio Owner': row['Portfolio Owner'],
                'Ticker': base_ticker,
                'Asset Name': row['Asset Name'],
                'Asset Class': 'STOCK',
                'Units': units,
                'Live NAV': current_price,
                'Current Value': round(units * current_price, 2),
                'Value Date': nav_date,
            })
            logger.info("STOCK %s: ₹%,.2f (as of %s)", yf_ticker, current_price, nav_date)

        except Exception as e:
            if "SGB" in base_ticker.upper():
                try:
                    logger.warning("yfinance failed for %s, trying nsepython fallback...", yf_ticker)
                    data = nse_eq(base_ticker)
                    current_price = float(data['priceInfo']['lastPrice'])
                    nav_date = data.get('metadata', {}).get('lastUpdateTime', 'unknown')
                    if nav_date != 'unknown':
                        nav_date = nav_date.split()[0]
                    live_data.append({
                        'Portfolio Owner': row['Portfolio Owner'],
                        'Ticker': base_ticker,
                        'Asset Name': row['Asset Name'],
                        'Asset Class': 'STOCK',
                        'Units': units,
                        'Live NAV': current_price,
                        'Current Value': round(units * current_price, 2),
                        'Value Date': nav_date,
                    })
                    logger.info("NSE fallback: %s ₹%,.2f (as of %s)", base_ticker, current_price, nav_date)
                except Exception as nse_e:
                    logger.error("NSE fallback also failed for %s: %s", base_ticker, nse_e)
            else:
                logger.error("Error fetching STOCK %s: %s", yf_ticker, e)

    if not live_data:
        logger.warning("Failed to fetch pricing for any stocks.")
        return None

    df_valuation = pd.DataFrame(live_data)
    totals = df_valuation.groupby('Portfolio Owner')['Current Value'].sum()
    return df_valuation, totals


def process_stock_valuations(
    ledger_path: str = 'data/output/master_ledger.csv',
    valuation_path: str = 'data/output/master_valuation.csv',
) -> None:
    ledger = Path(ledger_path)
    if not ledger.exists():
        logger.error("%s not found.", ledger_path)
        return

    result = fetch_live_stock_valuations(pd.read_csv(ledger))
    if result is None:
        return

    df_final, portfolio_totals = result
    logger.info("LIVE STOCK VALUATION")
    for owner, total in portfolio_totals.items():
        logger.info("  %-10s : ₹%,.2f", owner.upper(), total)
    logger.info("  STOCKS TOTAL: ₹%,.2f", portfolio_totals.sum())

    out = Path(valuation_path)
    if out.exists():
        existing_val = pd.read_csv(out)
        if 'Asset Class' in existing_val.columns:
            existing_val = existing_val[existing_val['Asset Class'] != 'STOCK']
        pd.concat([existing_val, df_final], ignore_index=True).to_csv(out, index=False)
    else:
        df_final.to_csv(out, index=False)

    logger.info("Stock valuations saved to '%s'", valuation_path)


if __name__ == "__main__":
    process_stock_valuations()
