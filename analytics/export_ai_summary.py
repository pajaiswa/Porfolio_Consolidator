"""
export_ai_summary.py — Unified AI Portfolio Summary
===================================================
Generates a comprehensive flat file containing position-level details, 
including FIFO cost-basis, XIRR, absolute return, and primary sub-class.
This is meant to be consumed by the AI Advisor for detailed portfolio research.

Inputs  : data/output/master_ledger.csv
          data/output/master_valuation.csv
          data/output/asset_allocation_drilldown.csv
Output  : data/output/ai_portfolio_summary.csv
"""
import logging
from datetime import datetime
import sys
from pathlib import Path

# Ensure project root is in sys.path so 'analytics' is resolvable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from pyxirr import xirr

from analytics.calculate_xirr import normalize_cash_flows, calculate_fifo_invested

logger = logging.getLogger(__name__)

LEDGER_PATH = "data/output/master_ledger.csv"
VALUATION_PATH = "data/output/master_valuation.csv"
DRILLDOWN_PATH = "data/output/asset_allocation_drilldown.csv"
OUTPUT_PATH = "data/output/ai_portfolio_summary.csv"


def export_ai_summary(
    ledger_path: str = LEDGER_PATH,
    valuation_path: str = VALUATION_PATH,
    drilldown_path: str = DRILLDOWN_PATH,
    out_path: str = OUTPUT_PATH,
) -> None:
    """
    Consolidates valuation, ledger (cash flows), and drilldown (sub-class)
    into a single per-holding dataset for AI analysis.
    """
    logger.info("Exporting Unified AI Portfolio Summary...")

    if not Path(ledger_path).exists() or not Path(valuation_path).exists():
        logger.error("Files missing: ledger or valuation. Run pipeline first.")
        return

    df_ledger_raw = pd.read_csv(ledger_path)
    df_ledger = normalize_cash_flows(df_ledger_raw)
    df_val = pd.read_csv(valuation_path)

    # Load Drilldown for Sub-Class mapping
    df_drill = pd.DataFrame()
    if Path(drilldown_path).exists():
        df_drill = pd.read_csv(drilldown_path)

    # Build Map: (Owner, Asset Name) -> Primary Sub Class
    # (By taking the one with the highest grouped value)
    primary_subclass_map = {}
    if not df_drill.empty:
        idx = df_drill.groupby(['Owner', 'Asset Name'])['Value'].idxmax()
        primary_drill = df_drill.loc[idx]
        for _, row in primary_drill.iterrows():
            key = (str(row['Owner']).strip(), str(row['Asset Name']).strip())
            primary_subclass_map[key] = str(row['Sub Class'])

    eval_date = datetime.now()
    eval_date_str = eval_date.strftime('%Y-%m-%d')
    summary_rows = []

    for _, val_row in df_val.iterrows():
        owner = str(val_row.get('Portfolio Owner', '')).strip()
        ticker = str(val_row.get('Ticker', '')).strip()
        asset_name = str(val_row.get('Asset Name', '')).strip()
        asset_class = str(val_row.get('Asset Class', '')).strip()
        units = float(val_row.get('Units', 0.0))
        live_nav = float(val_row.get('Live NAV', 0.0))
        current_value = float(val_row.get('Current Value', 0.0))

        if current_value <= 0:
            continue

        # Sub-class lookup
        map_key = (owner, asset_name)
        sub_class = primary_subclass_map.get(map_key, "Unknown")
        
        # Fallbacks for items not captured in drilldown
        if sub_class == "Unknown":
            if asset_class == "Global Holdings":
                sub_class = "Equity_Foreign"
            elif asset_class in ["FD", "EPF"]:
                sub_class = "Debt"

        # Ticker edge-case matching (sometimes numeric tickers get .0 appended in pandas)
        ticker_norm = ticker.replace('.0', '')
        mask = (
            (df_ledger['Portfolio Owner'].astype(str).str.strip() == owner) &
            (df_ledger['Ticker'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True) == ticker_norm)
        )
        item_ledger = df_ledger[mask]

        # Calculate metrics
        item_invested = calculate_fifo_invested(item_ledger)
        
        abs_return_pct = 0.0
        if item_invested > 0:
            abs_return_pct = round(((current_value - item_invested) / item_invested) * 100, 2)

        item_xirr = 0.0
        if not item_ledger.empty:
            cf_grouped = item_ledger.groupby('Date')['Net_Cash_Flow'].sum().reset_index()
            cf_grouped = cf_grouped[cf_grouped['Net_Cash_Flow'] != 0]

            terminal_row = pd.DataFrame({'Date': [eval_date], 'Net_Cash_Flow': [current_value]})
            xirr_data = pd.concat([cf_grouped, terminal_row], ignore_index=True)
            xirr_data = xirr_data.groupby('Date')['Net_Cash_Flow'].sum().reset_index()

            amounts = xirr_data['Net_Cash_Flow'].tolist()
            if amounts and any(a < 0 for amounts in [amounts] for a in amounts) and any(a > 0 for amounts in [amounts] for a in amounts):
                try:
                    item_xirr = round(xirr(xirr_data['Date'].tolist(), amounts) * 100, 2)
                except Exception:
                    pass

        summary_rows.append({
            'Owner': owner,
            'Asset_Class': asset_class,
            'Sub_Class': sub_class,
            'Ticker': ticker,
            'Asset_Name': asset_name,
            'Units': round(units, 4),
            'Live_NAV': round(live_nav, 4),
            'Invested_Amount': round(item_invested, 2),
            'Current_Value': round(current_value, 2),
            'Abs_Return_Pct': abs_return_pct,
            'XIRR_Pct': item_xirr,
            'Value_Date': eval_date_str
        })

    if summary_rows:
        df_summary = pd.DataFrame(summary_rows)
        # Order by Owner then Size
        df_summary = df_summary.sort_values(by=['Owner', 'Current_Value'], ascending=[True, False])
        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
        df_summary.to_csv(out_path, index=False)
        logger.info("AI Portfolio Summary saved to %s (%d active holdings)", out_path, len(df_summary))
    else:
        logger.warning("No active holdings found to export.")


if __name__ == "__main__":
    import logging as _logging
    _logging.basicConfig(level=_logging.INFO, format="%(asctime)s [%(levelname)-8s] %(message)s", datefmt="%H:%M:%S")
    export_ai_summary()
