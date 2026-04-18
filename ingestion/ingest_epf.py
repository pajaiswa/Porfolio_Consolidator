"""
ingest_epf.py — EPF Data Ingestion
=====================================
Reads the EPF config CSV and records each member's closing balance
as a single lump-sum 'Buy' transaction in master_ledger.csv.

Input  : data/input/EPF/epf_config.csv
         Columns: Owner, Asset_Name, Closing_Balance, As_Of_Date, Interest_Rate_Pct
Output : data/output/master_ledger.csv  (appends/replaces EPF rows)
"""
import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def ingest_epf_config(
    config_path: str = 'data/input/EPF/epf_config.csv',
    ledger_path: str = 'data/output/master_ledger.csv',
) -> bool:
    """
    Reads the EPF config and appends each member's balance as a single
    lump-sum contribution entry in master_ledger.csv.
    """
    logger.info("Starting EPF Data Ingestion...")
    cfg = Path(config_path)
    if not cfg.exists():
        logger.error("EPF config file not found at %s", config_path)
        return False

    df_epf = pd.read_csv(cfg)
    if df_epf.empty:
        logger.error("EPF config file is empty: %s", config_path)
        return False

    epf_records = []
    for _, row in df_epf.iterrows():
        epf_records.append({
            'Portfolio Owner': row['Owner'],
            'Asset Class': 'EPF',
            'Asset Name': row['Asset_Name'],
            'Ticker': 'EPF_LNT',
            'ISIN': '',
            'Transaction Type': 'Buy',
            'Units': float(row['Closing_Balance']),
            'Amount': float(row['Closing_Balance']),
            'Date': pd.to_datetime(row['As_Of_Date']).strftime('%Y-%m-%d'),
        })
        logger.info("Ingested EPF for %s: Balance ₹%s as of %s",
                    row['Owner'], row['Closing_Balance'], row['As_Of_Date'])

    df_new = pd.DataFrame(epf_records)
    ledger = Path(ledger_path)

    if ledger.exists():
        df_master = pd.read_csv(ledger)
        df_master = df_master[df_master['Asset Class'] != 'EPF']
        df_combined = pd.concat([df_master, df_new], ignore_index=True)
        df_combined['Date'] = pd.to_datetime(df_combined['Date'], format='mixed', dayfirst=False)
        df_combined = df_combined.sort_values('Date').reset_index(drop=True)
        df_combined['Date'] = df_combined['Date'].dt.strftime('%Y-%m-%d')
        df_combined.to_csv(ledger, index=False)
    else:
        df_new.to_csv(ledger, index=False)

    logger.info("EPF records appended to %s", ledger_path)
    return True


if __name__ == "__main__":
    ingest_epf_config()
