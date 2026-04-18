"""
calculate_xirr.py — Cash-Flow Normalisation & XIRR Engine
==========================================================
Core financial calculation module.  Imported by dashboard.py and run_all.py.

Public API:
    normalize_cash_flows(df_ledger)              -> pd.DataFrame
    calculate_fifo_invested(df_subset)           -> float
    calculate_portfolio_performance(ledger_path, valuation_path) -> pd.DataFrame
"""
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
from pyxirr import xirr

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 1. Cash-flow normalisation
# ---------------------------------------------------------------------------

def normalize_cash_flows(df_ledger: pd.DataFrame) -> pd.DataFrame:
    """
    Standardises cash-flow directions for every asset class.

    Convention (investor perspective):
      Negative  = money leaving the bank (Buy/SIP/Invest)
      Positive  = money returning to the bank (Sell/Redemption/Payout)
    """
    df = df_ledger.copy()
    df['Date'] = pd.to_datetime(df['Date'], format='mixed', dayfirst=True).dt.normalize()
    df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce').fillna(0)
    df['Net_Cash_Flow'] = 0.0

    for idx, row in df.iterrows():
        ttype = str(row['Transaction Type']).upper()
        amt = abs(row['Amount'])

        if any(w in ttype for w in ['REDEEM', 'REDEMPTION', 'SELL', 'WITHDRAWAL', 'PAYOUT', 'DIVIDEND', 'MATURITY']):
            df.at[idx, 'Net_Cash_Flow'] = amt
        elif any(w in ttype for w in ['PURCHASE', 'BUY', 'LUMP', 'SYSTEMATIC', 'SIP', 'STAMP', 'DUTY', 'INVEST']):
            df.at[idx, 'Net_Cash_Flow'] = -amt
        elif 'SWITCH' in ttype:
            df.at[idx, 'Net_Cash_Flow'] = amt if 'OUT' in ttype else -amt
        elif any(w in ttype for w in ['REINVEST', 'STT PAID', 'TDS', 'BONUS', 'SPLIT']):
            df.at[idx, 'Net_Cash_Flow'] = 0.0

    return df


# ---------------------------------------------------------------------------
# 2. FIFO cost-basis calculation
# ---------------------------------------------------------------------------

def calculate_fifo_invested(df_subset: pd.DataFrame) -> float:
    """
    Returns the true out-of-pocket net-invested cost basis of currently
    active holdings using a First-In-First-Out (FIFO) lot queue.
    """
    df = df_subset.copy()
    if df.empty:
        return 0.0

    df['Date'] = pd.to_datetime(df['Date'], format='mixed', dayfirst=True)
    df['Units'] = pd.to_numeric(df['Units'], errors='coerce').fillna(0)
    df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce').fillna(0)
    df.sort_values('Date', inplace=True)

    total_invested = 0.0

    for _ticker, grp in df.groupby('Ticker'):
        lots: list[dict] = []
        for _, row in grp.iterrows():
            ttype = str(row['Transaction Type']).upper()
            amt = abs(row['Amount'])

            if any(w in ttype for w in ['BUY', 'PURCHASE', 'LUMP', 'SIP', 'SYSTEMATIC', 'INVEST']):
                if row['Units'] > 0:
                    lots.append({'units': row['Units'], 'price': amt / row['Units'], 'amount': amt})
                elif amt > 0:
                    # NPS-style: units=0, amount is the direct cash invested
                    lots.append({'units': 1.0, 'price': amt, 'amount': amt})

            elif 'SWITCH' in ttype and 'IN' in ttype and row['Units'] > 0:
                lots.append({'units': row['Units'], 'price': amt / row['Units'], 'amount': amt})

            elif any(w in ttype for w in ['REDEEM', 'SELL', 'REDEMPTION']) or ('SWITCH' in ttype and 'OUT' in ttype):
                sold_units = abs(row['Units'])
                while sold_units > 0.001 and lots:
                    if lots[0]['units'] <= sold_units + 0.001:
                        sold_units -= lots[0]['units']
                        lots.pop(0)
                    else:
                        lots[0]['units'] -= sold_units
                        lots[0]['amount'] = lots[0]['units'] * lots[0]['price']
                        sold_units = 0

        total_invested += sum(lot['amount'] for lot in lots)

    return round(total_invested, 2)


# ---------------------------------------------------------------------------
# 3. Full portfolio performance engine
# ---------------------------------------------------------------------------

def calculate_portfolio_performance(
    ledger_path: str,
    valuation_path: str,
    nav_date: str | None = None,
) -> pd.DataFrame | None:
    """Computes XIRR and return metrics at the member, asset-class, and family level."""
    if nav_date is None:
        nav_date = datetime.now().strftime('%Y-%m-%d')

    if not Path(ledger_path).exists() or not Path(valuation_path).exists():
        logger.error("Required files missing at '%s' or '%s'", ledger_path, valuation_path)
        return None

    df_ledger = normalize_cash_flows(pd.read_csv(ledger_path))
    df_val = pd.read_csv(valuation_path)

    # Standardise tickers
    for df in (df_ledger, df_val):
        df['Ticker'] = df['Ticker'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)

    eval_date = pd.to_datetime(nav_date).normalize()
    member_summaries: list[dict] = []
    family_cf = pd.DataFrame(columns=['Date', 'Net_Cash_Flow'])

    owners = df_ledger['Portfolio Owner'].unique()
    asset_classes = df_ledger['Asset Class'].unique()

    # --- Member + Asset Class Level ---
    for owner in owners:
        owner_ledger = df_ledger[df_ledger['Portfolio Owner'] == owner]
        owner_val = df_val[df_val['Portfolio Owner'] == owner]
        member_total_cf = pd.DataFrame()
        member_total_current_val = 0.0
        member_total_invested = 0.0

        for ac in asset_classes:
            ac_ledger = owner_ledger[owner_ledger['Asset Class'] == ac]
            ac_val = owner_val[owner_val['Asset Class'] == ac]
            if ac_ledger.empty:
                continue

            ac_cf = ac_ledger.groupby('Date')['Net_Cash_Flow'].sum().reset_index()
            ac_cf = ac_cf[ac_cf['Net_Cash_Flow'] != 0]
            if ac_cf.empty:
                continue

            member_total_cf = pd.concat([member_total_cf, ac_cf], ignore_index=True)
            ac_current_val = ac_val['Current Value'].sum() if not ac_val.empty else 0.0
            member_total_current_val += ac_current_val

            if ac_current_val > 0:
                terminal_row = pd.DataFrame({'Date': [eval_date], 'Net_Cash_Flow': [ac_current_val]})
                ac_xirr_data = pd.concat([ac_cf, terminal_row], ignore_index=True)
            else:
                ac_xirr_data = ac_cf.copy()

            ac_xirr_data = ac_xirr_data.groupby('Date')['Net_Cash_Flow'].sum().reset_index()
            amounts = ac_xirr_data['Net_Cash_Flow'].tolist()

            if amounts and any(a < 0 for a in amounts):
                try:
                    m_xirr = round(xirr(ac_xirr_data['Date'].tolist(), amounts) * 100, 2)
                    m_invested = calculate_fifo_invested(ac_ledger)
                    member_total_invested += m_invested
                    member_summaries.append({
                        'Entity': f"{owner} - {ac}",
                        'Invested': m_invested,
                        'Current': ac_current_val,
                        'Return': ac_current_val - m_invested,
                        'Abs %': round(((ac_current_val - m_invested) / m_invested) * 100, 2) if m_invested else 0,
                        'XIRR %': m_xirr,
                    })
                except Exception as e:
                    logger.warning("Could not calculate XIRR for %s %s: %s", owner, ac, e)

        # Member total
        if not member_total_cf.empty:
            family_cf = pd.concat([family_cf, member_total_cf], ignore_index=True)
            member_total_cf = member_total_cf.groupby('Date')['Net_Cash_Flow'].sum().reset_index()
            member_total_cf = member_total_cf[member_total_cf['Net_Cash_Flow'] != 0]

            if member_total_current_val > 0:
                terminal_row = pd.DataFrame({'Date': [eval_date], 'Net_Cash_Flow': [member_total_current_val]})
                mem_xirr_data = pd.concat([member_total_cf, terminal_row], ignore_index=True)
            else:
                mem_xirr_data = member_total_cf.copy()

            mem_xirr_data = mem_xirr_data.groupby('Date')['Net_Cash_Flow'].sum().reset_index()
            amounts = mem_xirr_data['Net_Cash_Flow'].tolist()
            if amounts and any(a < 0 for a in amounts):
                try:
                    m_xirr = round(xirr(mem_xirr_data['Date'].tolist(), amounts) * 100, 2)
                    member_summaries.append({
                        'Entity': f"👤 {owner.upper()} TOTAL",
                        'Invested': member_total_invested,
                        'Current': member_total_current_val,
                        'Return': member_total_current_val - member_total_invested,
                        'Abs %': round(((member_total_current_val - member_total_invested) / member_total_invested) * 100, 2) if member_total_invested else 0,
                        'XIRR %': m_xirr,
                    })
                except Exception as e:
                    logger.warning("Could not calculate XIRR for %s TOTAL: %s", owner, e)

    # --- Asset Class Total Level ---
    for ac in asset_classes:
        ac_ledger = df_ledger[df_ledger['Asset Class'] == ac]
        ac_val = df_val[df_val['Asset Class'] == ac]
        if ac_ledger.empty:
            continue

        ac_cf = ac_ledger.groupby('Date')['Net_Cash_Flow'].sum().reset_index()
        ac_cf = ac_cf[ac_cf['Net_Cash_Flow'] != 0]
        ac_current_val = ac_val['Current Value'].sum() if not ac_val.empty else 0.0
        ac_invested = calculate_fifo_invested(ac_ledger)

        if ac_current_val > 0:
            terminal_row = pd.DataFrame({'Date': [eval_date], 'Net_Cash_Flow': [ac_current_val]})
            ac_xirr_data = pd.concat([ac_cf, terminal_row], ignore_index=True)
        else:
            ac_xirr_data = ac_cf.copy()

        ac_xirr_data = ac_xirr_data.groupby('Date')['Net_Cash_Flow'].sum().reset_index()
        amounts = ac_xirr_data['Net_Cash_Flow'].tolist()
        if amounts and any(a < 0 for a in amounts):
            try:
                member_summaries.append({
                    'Entity': f"📈 FAMILY {ac.upper()}",
                    'Invested': ac_invested,
                    'Current': ac_current_val,
                    'Return': ac_current_val - ac_invested,
                    'Abs %': round(((ac_current_val - ac_invested) / ac_invested) * 100, 2) if ac_invested else 0,
                    'XIRR %': round(xirr(ac_xirr_data['Date'].tolist(), amounts) * 100, 2),
                })
            except Exception as e:
                logger.warning("Could not calculate XIRR for FAMILY %s: %s", ac, e)

    # --- Consolidated Family Total ---
    if not family_cf.empty:
        family_cf = family_cf.groupby('Date')['Net_Cash_Flow'].sum().reset_index()
        family_cf = family_cf[family_cf['Net_Cash_Flow'] != 0]
        f_current = df_val['Current Value'].sum()
        f_invested = calculate_fifo_invested(df_ledger)

        if f_current > 0:
            terminal_row = pd.DataFrame({'Date': [eval_date], 'Net_Cash_Flow': [f_current]})
            family_xirr_data = pd.concat([family_cf, terminal_row], ignore_index=True)
        else:
            family_xirr_data = family_cf.copy()

        family_xirr_data = family_xirr_data.groupby('Date')['Net_Cash_Flow'].sum().reset_index()
        amounts = family_xirr_data['Net_Cash_Flow'].tolist()
        if amounts and any(a < 0 for a in amounts):
            try:
                member_summaries.append({
                    'Entity': '🏠 CONSOLIDATED FAMILY',
                    'Invested': f_invested,
                    'Current': f_current,
                    'Return': f_current - f_invested,
                    'Abs %': round(((f_current - f_invested) / f_invested) * 100, 2) if f_invested else 0,
                    'XIRR %': round(xirr(family_xirr_data['Date'].tolist(), amounts) * 100, 2),
                })
            except Exception:
                pass

    return pd.DataFrame(member_summaries)


# ---------------------------------------------------------------------------
# CLI convenience — mirrors original Milestone_3.py behaviour
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    LEDGER = 'data/output/master_ledger.csv'
    VALUATION = 'data/output/master_valuation.csv'

    logger.info("Starting Performance Metrics Calculation...")
    summary = calculate_portfolio_performance(LEDGER, VALUATION)

    if summary is not None:
        logger.info("─" * 125)
        logger.info("%-35s | %15s | %15s | %15s | %10s | %10s",
                    'Entity', 'Invested', 'Current', 'Return', 'Abs %', 'XIRR %')
        logger.info("-" * 125)
        for _, row in summary.iterrows():
            logger.info(
                "%-35s | ₹%13,.0f | ₹%13,.2f | ₹%13,.0f | %9s%% | %9s%%",
                row['Entity'], row['Invested'], row['Current'], row['Return'],
                row['Abs %'], row['XIRR %']
            )
        logger.info("=" * 125)

        metrics_path = Path('data/output/performance_metrics.csv')
        metrics_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            summary.to_csv(metrics_path, index=False)
            logger.info("Saved detailed metrics to '%s'", metrics_path)
        except PermissionError:
            logger.error("Permission Denied: Could not save to '%s'. Close it in Excel and retry.", metrics_path)
