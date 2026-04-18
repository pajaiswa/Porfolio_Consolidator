"""
ingest_global.py — Global Holdings (IBKR) Ingestion
=====================================================
Reads the persistent global_transactions.csv and writes every transaction
into the universal master_ledger.csv format as Asset Class = 'Global Holdings'.

Input  : data/input/global/global_transactions.csv
         Columns: Owner, Ticker, Asset_Name, Transaction_Type,
                  Shares, INR_Amount, Trade_Date
Output : data/output/master_ledger.csv  (replaces Global Holdings rows only)
"""
import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

TRANSACTION_FILE = 'data/input/global/global_transactions.csv'
LEDGER_FILE = 'data/output/master_ledger.csv'
ASSET_CLASS = 'Global Holdings'


def ingest_global_transactions(
    txn_path: str = TRANSACTION_FILE,
    ledger_path: str = LEDGER_FILE,
) -> bool:
    """
    Reads global_transactions.csv and appends every trade row into
    master_ledger.csv, replacing any existing Global Holdings rows.
    The source CSV is never modified — it is the permanent user-managed ledger.
    """
    logger.info("Starting Global Holdings (IBKR) ingestion...")

    cfg = Path(txn_path)
    if not cfg.exists():
        logger.warning("Global transactions file not found at %s — skipping.", txn_path)
        return True  # Not an error; user simply hasn't added any IBKR holdings yet

    df_txn = pd.read_csv(cfg)

    # Drop fully-empty rows (e.g. header-only file)
    df_txn = df_txn.dropna(how='all')

    if df_txn.empty:
        logger.info("No Global Holdings transactions found — skipping.")
        return True

    required_cols = {'Owner', 'Ticker', 'Asset_Name', 'Transaction_Type', 'Shares', 'INR_Amount', 'Trade_Date'}
    missing = required_cols - set(df_txn.columns)
    if missing:
        logger.error("global_transactions.csv is missing required columns: %s", missing)
        return False

    # Validate basic data quality
    df_txn['Shares'] = pd.to_numeric(df_txn['Shares'], errors='coerce')
    df_txn['INR_Amount'] = pd.to_numeric(df_txn['INR_Amount'], errors='coerce')
    df_txn['Trade_Date'] = pd.to_datetime(df_txn['Trade_Date'], format='mixed', dayfirst=True)

    invalid = df_txn[df_txn['Shares'].isna() | df_txn['INR_Amount'].isna() | df_txn['Trade_Date'].isna()]
    if not invalid.empty:
        logger.warning("Skipping %d rows with invalid/missing data in global_transactions.csv", len(invalid))
        df_txn = df_txn.dropna(subset=['Shares', 'INR_Amount', 'Trade_Date'])

    if df_txn.empty:
        logger.warning("All global transaction rows were invalid — nothing to ingest.")
        return True

    # Map to universal ledger schema
    ledger_rows = []
    for _, row in df_txn.iterrows():
        ledger_rows.append({
            'Portfolio Owner': str(row['Owner']).strip().title(),
            'Asset Class': ASSET_CLASS,
            'Asset Name': str(row['Asset_Name']).strip(),
            'Ticker': str(row['Ticker']).strip().upper(),
            'ISIN': '',
            'Transaction Type': str(row['Transaction_Type']).strip().title(),
            'Units': float(row['Shares']),
            'Amount': float(row['INR_Amount']),
            'Date': row['Trade_Date'].strftime('%Y-%m-%d'),
        })
        logger.info(
            "  %s | %s %s shares of %s on %s (₹%.0f)",
            row['Owner'], row['Transaction_Type'], row['Shares'],
            row['Ticker'], row['Trade_Date'].strftime('%d-%b-%Y'), row['INR_Amount'],
        )

    df_new = pd.DataFrame(ledger_rows)

    # Merge: drop existing Global Holdings rows and append fresh from source CSV
    ledger = Path(ledger_path)
    if ledger.exists():
        df_master = pd.read_csv(ledger)
        df_master = df_master[df_master['Asset Class'] != ASSET_CLASS]
        df_combined = pd.concat([df_master, df_new], ignore_index=True)
        df_combined['Date'] = pd.to_datetime(df_combined['Date'], format='mixed', dayfirst=False)
        df_combined = df_combined.sort_values('Date').reset_index(drop=True)
        df_combined['Date'] = df_combined['Date'].dt.strftime('%Y-%m-%d')
        df_combined.to_csv(ledger, index=False)
    else:
        df_new.to_csv(ledger, index=False)

    logger.info(
        "Global Holdings ingestion complete — %d transactions written to %s",
        len(df_new), ledger_path,
    )
    return True


if __name__ == "__main__":
    import logging as _logging
    _logging.basicConfig(level=_logging.INFO, format="%(asctime)s [%(levelname)-8s] %(message)s", datefmt="%H:%M:%S")
    ingest_global_transactions()
