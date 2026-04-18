"""
ingest_fd.py — Fixed Deposit Ingestion
========================================
Reads an FD details Excel file and appends each FD as a 'Buy' transaction
representing the principal investment in master_ledger.csv.

Input  : data/input/FD/FD_details.xlsx
         Columns: Owner, FD Start Date, Invested Amount, Interest Rate
Output : data/output/master_ledger.csv  (appends/replaces FD rows)
"""
import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def ingest_fd_data(
    fd_excel_path: str = 'data/input/FD/FD_details.xlsx',
    ledger_path: str = 'data/output/master_ledger.csv',
) -> bool:
    """
    Reads the FD details Excel and records each fixed deposit as a
    lump-sum 'Buy' transaction in master_ledger.csv.
    """
    logger.info("Starting FD Data Ingestion...")
    fd_path = Path(fd_excel_path)
    if not fd_path.exists():
        logger.error("FD Excel file not found at %s", fd_excel_path)
        return False

    df_fd = pd.read_excel(fd_path)
    if df_fd.empty:
        logger.warning("FD Excel file has no rows — no owners have FDs. Skipping.")
        return True  # not an error; owners simply have no FDs

    fd_records = []
    for _, row in df_fd.iterrows():
        fd_records.append({
            'Portfolio Owner': row['Owner'],
            'Asset Class': 'FD',
            'Asset Name': 'Fixed Deposit',
            'Ticker': f"FD_{row['Owner']}_{row['Invested Amount']}",
            'ISIN': '',
            'Transaction Type': 'Buy',
            'Units': float(row['Invested Amount']),
            'Amount': float(row['Invested Amount']),
            'Date': pd.to_datetime(row['FD Start Date']).strftime('%Y-%m-%d'),
        })
        logger.info("Ingested FD for %s: Amount ₹%s started %s",
                    row['Owner'], row['Invested Amount'], row['FD Start Date'])

    df_new = pd.DataFrame(fd_records)
    ledger = Path(ledger_path)

    if ledger.exists():
        df_master = pd.read_csv(ledger)
        df_master = df_master[df_master['Asset Class'] != 'FD']
        df_combined = pd.concat([df_master, df_new], ignore_index=True)
        df_combined['Date'] = pd.to_datetime(df_combined['Date'], format='mixed', dayfirst=False)
        df_combined = df_combined.sort_values('Date').reset_index(drop=True)
        df_combined['Date'] = df_combined['Date'].dt.strftime('%Y-%m-%d')
        df_combined.to_csv(ledger, index=False)
    else:
        df_new.to_csv(ledger, index=False)

    logger.info("FD records appended to %s", ledger_path)
    return True


if __name__ == "__main__":
    ingest_fd_data()
