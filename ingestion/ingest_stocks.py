"""
ingest_stocks.py — Stock Order Ingestion
==========================================
Parses Groww stock order Excel exports and appends STOCK rows to master_ledger.csv.

Input  : data/input/stock/*.xlsx  (one file per owner, filename starts with owner name)
Output : data/output/master_ledger.csv  (appends/replaces STOCK rows)
"""
import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def ingest_stock_orders(
    stock_input_dir: str = 'data/input/stock',
    master_ledger_path: str = 'data/output/master_ledger.csv',
) -> None:
    """
    Parses raw stock order Excel files, standardises them into the universal
    ledger format, and merges them into master_ledger.csv.

    Owner name is derived from the filename stem (e.g. 'Champalal_stocks.xlsx' → 'Champalal').
    """
    input_path = Path(stock_input_dir)
    logger.info("Looking for stock Excel files in: %s", stock_input_dir)

    stock_files = list(input_path.glob('*.xlsx'))
    if not stock_files:
        logger.warning("No Excel stock files found. Skipping.")
        return

    all_stock_txns: list[pd.DataFrame] = []

    for file in sorted(stock_files):
        owner_raw = file.stem.split('_')[0]
        owner = owner_raw[0].upper() + owner_raw[1:] if owner_raw else owner_raw
        try:
            df = pd.read_excel(file, skiprows=5)
        except Exception as e:
            logger.error("Error reading %s: %s", file.name, e)
            continue

        if 'Order status' in df.columns:
            df = df[df['Order status'].str.strip().str.upper() == 'EXECUTED'].copy()
        if df.empty:
            continue

        df_ledger = pd.DataFrame({
            'Asset Name': df['Stock name'],
            'Ticker': df['Symbol'],
            'Transaction Type': df['Type'],
            'Units': df['Quantity'],
            'Amount': df['Value'],
            'Portfolio Owner': owner,
            'Asset Class': 'STOCK',
            'ISIN': '',
            'Date': pd.to_datetime(df['Execution date and time'], format='mixed', dayfirst=True).dt.date,
        })

        all_stock_txns.append(df_ledger)
        logger.info("Processed %d executed stock orders for %s", len(df_ledger), owner)

    if not all_stock_txns:
        logger.warning("No executed stock orders found.")
        return

    combined_stocks = pd.concat(all_stock_txns, ignore_index=True)
    ledger_path = Path(master_ledger_path)

    if ledger_path.exists():
        existing_ledger = pd.read_csv(ledger_path)
        if 'Asset Class' not in existing_ledger.columns:
            existing_ledger['Asset Class'] = 'Mutual Fund'
        existing_ledger = existing_ledger[existing_ledger['Asset Class'] != 'STOCK']
        updated_ledger = pd.concat([existing_ledger, combined_stocks], ignore_index=True)
        updated_ledger.to_csv(ledger_path, index=False)
        logger.info("Appended stock orders to %s (%d total records)",
                    master_ledger_path, len(updated_ledger))
    else:
        ledger_path.parent.mkdir(parents=True, exist_ok=True)
        combined_stocks.to_csv(ledger_path, index=False)
        logger.info("Created new master ledger at %s (%d records)",
                    master_ledger_path, len(combined_stocks))


if __name__ == "__main__":
    ingest_stock_orders()
