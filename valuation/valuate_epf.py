"""
valuate_epf.py — Live EPF Valuation
=====================================
Calculates the accrued live value of EPF balances using simple interest,
based on the closing balance, interest rate, and days elapsed since As_Of_Date.

Input  : data/input/EPF/epf_config.csv
         Columns: Owner, Asset_Name, Closing_Balance, As_Of_Date, Interest_Rate_Pct
Output : data/output/master_valuation.csv  (appends/replaces EPF rows)
"""
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def calculate_epf_valuation(
    config_path: str = 'data/input/EPF/epf_config.csv',
    valuation_path: str = 'data/output/master_valuation.csv',
) -> bool:
    """
    Calculates the current value of each EPF account using simple interest
    accrual since the As_Of_Date provided in the config.
    """
    logger.info("Calculating live EPF valuation...")
    cfg = Path(config_path)
    if not cfg.exists():
        logger.error("EPF config file not found at %s", config_path)
        return False

    df_epf = pd.read_csv(cfg)
    if df_epf.empty:
        logger.warning("EPF config file has no rows. Skipping.")
        return True

    today = datetime.now()
    live_records = []

    for _, row in df_epf.iterrows():
        as_of_date = pd.to_datetime(row['As_Of_Date'])
        closing_balance = float(row['Closing_Balance'])
        interest_rate = float(row['Interest_Rate_Pct']) / 100.0

        days_elapsed = (today - as_of_date).days
        if days_elapsed > 0:
            interest_earned = closing_balance * interest_rate * (days_elapsed / 365.25)
            live_value = closing_balance + interest_earned
        else:
            live_value = closing_balance

        current_nav = live_value / closing_balance
        live_records.append({
            'Portfolio Owner': row['Owner'],
            'Ticker': 'EPF_LNT',
            'Asset Name': row['Asset_Name'],
            'Asset Class': 'EPF',
            'Units': closing_balance,
            'Live NAV': round(current_nav, 6),
            'Current Value': round(live_value, 2),
        })
        logger.info("EPF %s (%s): ₹%,.2f (interest earned: ₹%,.2f)",
                    row['Owner'], row['Asset_Name'], live_value, live_value - closing_balance)

    df_new_val = pd.DataFrame(live_records)
    out = Path(valuation_path)

    if out.exists():
        df_master = pd.read_csv(out)
        df_master = df_master[df_master['Asset Class'] != 'EPF']
        pd.concat([df_master, df_new_val], ignore_index=True).to_csv(out, index=False)
    else:
        df_new_val.to_csv(out, index=False)

    logger.info("EPF valuations saved to %s", valuation_path)
    return True


if __name__ == "__main__":
    calculate_epf_valuation()
