"""
calc_allocations.py — Asset Sub-Class Allocation Calculator
=============================================================
Computes the true equity/debt/gold/cash exposure for the entire portfolio
by applying look-through weights from asset_allocation_map.csv.

Heuristics are used for asset classes not in the map (NPS, EPF, FD).

Inputs  : data/output/master_valuation.csv
          data/input/asset_allocation_map.csv  (from fetch_allocations.py)
Outputs : data/output/asset_allocation.csv         (summary totals by sub-class)
          data/output/asset_allocation_drilldown.csv (per-asset breakdown)
"""
import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_stock_subclass(ticker: str, name: str) -> str:
    """Heuristic sub-class for stocks and ETFs not found in the allocation map."""
    ticker_up = str(ticker).upper()
    name_up = str(name).upper()

    if 'GOLD' in ticker_up or 'SGB' in ticker_up or 'GOLD' in name_up:
        return 'Gold'
    if any(f in ticker_up for f in ['MON100', 'MAFANG', 'MASPTOP50']) or 'NASDAQ' in name_up:
        return 'Equity_Foreign'
    return 'Equity_India'


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def calculate_allocations(
    val_path: str = 'data/output/master_valuation.csv',
    map_path: str = 'data/input/asset_allocation_map.csv',
    out_path: str = 'data/output/asset_allocation.csv',
    drilldown_path: str = 'data/output/asset_allocation_drilldown.csv',
) -> None:
    """
    Applies look-through weights from asset_allocation_map.csv to calculate
    the true sub-class exposure across all portfolio owners.
    """
    logger.info("CALCULATING ASSET ALLOCATIONS")

    if not Path(val_path).exists():
        logger.error("Error: %s not found.", val_path)
        return

    df_val = pd.read_csv(val_path)

    mf_map: dict = {}
    if Path(map_path).exists():
        try:
            df_map = pd.read_csv(map_path)
            for _, row in df_map.iterrows():
                mf_map[str(row['Ticker'])] = row
        except Exception:
            pass  # Empty or malformed map — no overrides; heuristics used for all assets

    allocation_totals = {'Equity_India': 0.0, 'Equity_Foreign': 0.0, 'Debt': 0.0, 'Gold': 0.0, 'Cash': 0.0}
    drilldown_rows: list[dict] = []

    for _, row in df_val.iterrows():
        aclass = row['Asset Class']
        val = float(row['Current Value'])
        name = row['Asset Name']
        ticker = str(row['Ticker'])
        owner = row['Portfolio Owner']

        if val <= 0:
            continue

        # 1. Explicit map entry (highest priority — works for any asset class)
        if ticker in mf_map:
            m_row = mf_map[ticker]
            contributions = {
                'Equity_India': float(m_row.get('Equity_India_Pct', 0.0)) / 100.0,
                'Equity_Foreign': float(m_row.get('Equity_Foreign_Pct', 0.0)) / 100.0,
                'Debt': float(m_row.get('Debt_Pct', 0.0)) / 100.0,
                'Gold': float(m_row.get('Gold_Pct', 0.0)) / 100.0,
                'Cash': float(m_row.get('Cash_Pct', 0.0)) / 100.0,
            }
            for sub, pct in contributions.items():
                if pct > 0:
                    allocation_totals[sub] += val * pct
                    drilldown_rows.append({
                        'Owner': owner, 'Asset Name': name,
                        'Asset Class': aclass, 'Sub Class': sub, 'Value': val * pct,
                    })
            continue

        # 2. Asset-class heuristics for non-mapped items
        if aclass in ['FD', 'EPF']:
            allocation_totals['Debt'] += val
            drilldown_rows.append({'Owner': owner, 'Asset Name': name, 'Asset Class': aclass, 'Sub Class': 'Debt', 'Value': val})

        elif aclass == 'NPS':
            sub = 'Debt' if ('Scheme C' in name or 'Scheme G' in name) else 'Equity_India'
            allocation_totals[sub] += val
            drilldown_rows.append({'Owner': owner, 'Asset Name': name, 'Asset Class': aclass, 'Sub Class': sub, 'Value': val})

        elif aclass == 'Stock':
            sub = get_stock_subclass(ticker, name)
            allocation_totals[sub] += val
            drilldown_rows.append({'Owner': owner, 'Asset Name': name, 'Asset Class': aclass, 'Sub Class': sub, 'Value': val})

        elif aclass == 'Mutual Fund':
            # Default to Indian Equity if not in map
            allocation_totals['Equity_India'] += val
            drilldown_rows.append({'Owner': owner, 'Asset Name': name, 'Asset Class': aclass, 'Sub Class': 'Equity_India', 'Value': val})

        elif aclass == 'Global Holdings':
            # IBKR positions — always foreign equity (international ETFs/stocks)
            allocation_totals['Equity_Foreign'] += val
            drilldown_rows.append({'Owner': owner, 'Asset Name': name, 'Asset Class': aclass, 'Sub Class': 'Equity_Foreign', 'Value': val})

    # Save summary — always write headers even if no assets qualified
    summ_rows = [{'Sub Class': k, 'Total Value': round(v, 2)} for k, v in allocation_totals.items() if v > 0]
    df_summ = pd.DataFrame(summ_rows, columns=['Sub Class', 'Total Value'])
    df_summ.to_csv(out_path, index=False)

    # Save drilldown — always write headers
    df_drill = pd.DataFrame(drilldown_rows,
                            columns=['Owner', 'Asset Name', 'Asset Class', 'Sub Class', 'Value'])
    df_drill.to_csv(drilldown_path, index=False)

    logger.info("  Summary saved to %s", out_path)
    logger.info("Summary DataFrame:\n%s", df_summ)


if __name__ == "__main__":
    calculate_allocations()
