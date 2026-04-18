"""
valuate_fd.py — Live Fixed Deposit Valuation
==============================================
Calculates the current accrued value of each FD using simple interest accrual
since the FD Start Date.

Input  : data/input/FD/FD_details.xlsx
         Columns: Owner, FD Start Date, Invested Amount, Interest Rate
Output : data/output/master_valuation.csv  (appends/replaces FD rows)
"""
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def calculate_fd_valuation(
    fd_excel_path: str = 'data/input/FD/FD_details.xlsx',
    valuation_path: str = 'data/output/master_valuation.csv',
) -> bool:
    """
    Calculates the current accrued value of each fixed deposit and merges
    the results into master_valuation.csv.
    """
    logger.info("Calculating live FD valuation...")
    fd_path = Path(fd_excel_path)
    if not fd_path.exists():
        logger.error("FD Excel file not found at %s", fd_excel_path)
        return False

    df_fd = pd.read_excel(fd_path)
    if df_fd.empty:
        logger.warning("FD Excel file has no rows — no owners have FDs. Skipping.")
        return True  # not an error; owners simply have no FDs

    today = datetime.now()
    live_records = []

    for _, row in df_fd.iterrows():
        start_date = pd.to_datetime(row['FD Start Date'])
        invested_amount = float(row['Invested Amount'])
        interest_rate = float(row['Interest Rate']) / 100.0

        days_elapsed = (today - start_date).days
        if days_elapsed > 0:
            interest_earned = invested_amount * interest_rate * (days_elapsed / 365.25)
            live_value = invested_amount + interest_earned
        else:
            live_value = invested_amount

        current_nav = live_value / invested_amount
        live_records.append({
            'Portfolio Owner': row['Owner'],
            'Ticker': f"FD_{row['Owner']}_{row['Invested Amount']}",
            'Asset Name': 'Fixed Deposit',
            'Asset Class': 'FD',
            'Units': invested_amount,
            'Live NAV': round(current_nav, 6),
            'Current Value': round(live_value, 2),
        })
        logger.info("FD %s: ₹%,.2f (interest earned: ₹%,.2f)",
                    row['Owner'], live_value, live_value - invested_amount)

    df_new_val = pd.DataFrame(live_records)
    out = Path(valuation_path)

    if out.exists():
        df_master = pd.read_csv(out)
        df_master = df_master[df_master['Asset Class'] != 'FD']
        pd.concat([df_master, df_new_val], ignore_index=True).to_csv(out, index=False)
    else:
        df_new_val.to_csv(out, index=False)

    logger.info("FD valuations saved to %s", valuation_path)
    return True


if __name__ == "__main__":
    calculate_fd_valuation()
