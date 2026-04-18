"""
ingest_nps.py — NPS Statement Ingestion
=========================================
Parses a password-protected KFintech NPS PDF statement into:
  1. master_ledger.csv rows (NPS cash flows per-scheme)
  2. mf_active_holdings.csv rows (net units per scheme for valuate_mf_nps.py)
  3. nps_latest_navs.json (scheme NAVs extracted from the PDF — see NOTE below)

NOTE (known limitation / backlog item):
  NAVs are parsed directly from the uploaded PDF statement.
  If the PDF is old, the cached NAV will be stale.
  Future fix: fetch live NPS NAVs from an external API.
  See DEVELOPER.md → To-Do / Backlog.
"""
from datetime import datetime
from pathlib import Path
import json
import logging
import os
import re

logger = logging.getLogger(__name__)

import pandas as pd
import pdfplumber
from dotenv import load_dotenv

load_dotenv()

INPUT_DIR = 'data/input/nps'
OUTPUT_DIR = 'data/output'
LEDGER_FILE = Path(OUTPUT_DIR) / 'master_ledger.csv'
NAV_CACHE_FILE = Path(OUTPUT_DIR) / 'nps_latest_navs.json'


def _parse_number(s: str) -> float:
    """Parse '1,234.56' or '(123.45)' or '(123.45' (unclosed paren) → float."""
    s = s.strip().replace(',', '')
    negative = s.startswith('(')
    s = s.replace('(', '').replace(')', '')
    try:
        value = float(s)
    except ValueError:
        return 0.0
    return -value if negative else value



def get_last_nps_nav(scheme_name: str, cache_path: str | None = None) -> float:
    """
    Look up the last known NAV for a given NPS scheme from the JSON cache.

    Returns 10.0 as a sentinel value if the cache is absent or the scheme
    is not found — this signals to the upstream caller that live data is needed.
    """
    import json
    path = Path(cache_path) if cache_path else NAV_CACHE_FILE
    if not path.exists():
        return 10.0
    try:
        with open(path, encoding='utf-8') as f:
            cache = json.load(f)
        return float(cache.get(scheme_name, 10.0))
    except Exception:
        return 10.0


def parse_kfintech_nps(pdf_path: str, password: str, owner_name: str) -> pd.DataFrame:
    """
    Parses a KFintech NPS Transaction Statement PDF.

    Returns a DataFrame of cash flow and unit-allocation rows in the
    universal ledger format, ready to be merged into master_ledger.csv.
    """
    logger.info("Parsing NPS Statement for %s...", owner_name)

    latest_navs: dict = {}
    p1_holdings: list = []
    final_cash: list = []

    SCHEME_NAMES = {
        'SCHEME E': 'NPS - Scheme E (Equity)',
        'SCHEME C': 'NPS - Scheme C (Corp Debt)',
        'SCHEME G': 'NPS - Scheme G (Govt Debt)',
        'ADVANTAGE FUND': 'NPS - Equity Advantage Fund',
    }
    SCHEME_MAP = {
        'SCHEME E': 'NPS - Scheme E (Equity)',
        'SCHEME C': 'NPS - Scheme C (Corp Debt)',
        'SCHEME G': 'NPS - Scheme G (Govt Debt)',
        'SCHEME A': 'NPS - Scheme A (Alternative Inv)',
        'ADVANTAGE FUND': 'NPS - Equity Advantage Fund',
    }

    with pdfplumber.open(pdf_path, password=password) as pdf:

        # Phase 1: Extract current holdings and NAVs from Page 1 summary
        p1_lines = (pdf.pages[0].extract_text() or "").split('\n')
        for i, line in enumerate(p1_lines):
            lu = line.upper()
            m = re.search(r'(\d[\d,]*\.\d+)\s+(\d[\d,]*\.\d+)\s+(\d[\d,]*\.\d+)$', line.strip())
            if not m:
                continue
            ctx = lu + ' ' + (p1_lines[i + 1].upper() if i + 1 < len(p1_lines) else '')
            scheme_name = next((name for key, name in SCHEME_NAMES.items() if key in ctx), None)
            if scheme_name is None:
                continue
            units_val = _parse_number(m.group(1))
            nav_val = _parse_number(m.group(2))
            latest_navs[scheme_name] = nav_val
            p1_holdings.append({'scheme': scheme_name, 'units': units_val, 'nav': nav_val})

        logger.info("  Page 1 holdings: %s", [(h['scheme'], h['units']) for h in p1_holdings])
        logger.info("  Latest NAVs: %s", latest_navs)
        if latest_navs:
            with open(NAV_CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(latest_navs, f)


        # Phase 2: Transaction details → cash flows per scheme
        # -------------------------------------------------------
        # The KFintech PDF uses a two-line format for every scheme entry:
        #   Line 1:  "... NPS TRUST- A/C HDFC PENSION FUND  <units>  <nav>  <amount>"
        #   Line 2:  "MANAGEMENT LIMITED SCHEME E - TIER I POP"
        # Scheme A uses a slightly different fund name but same two-line structure.
        # Equity Advantage Fund: "NPS TRUST A/C - HDFC PF NPS EQUITY ... ADVANTAGE FUND - TIER I"
        # Merger entries (16-Jan-26): amount = 0.00; only 2 numbers (no NAV column).
        # The old hard-coded Phase 2.5 merger value (89956.91) is removed.
        # -------------------------------------------------------

        _RE3 = re.compile(
            r'(\(?\d[\d,]*(?:\.\d+)?\)?)\s+(\(?\d[\d,]*(?:\.\d+)?\)?)\s+(\(?\d[\d,]*(?:\.\d+)?\)?)$'
        )
        _RE2 = re.compile(
            r'(\(?\d[\d,]*(?:\.\d+)?\)?)\s+(\(?\d[\d,]*(?:\.\d+)?\)?)$'
        )

        def _is_amount_line(lu: str) -> bool:
            return "HDFC" in lu and (
                "PENSION FUND" in lu or "NPS TRUST" in lu
            )

        def _detect_scheme_label(lu: str, date: datetime | None) -> str | None:
            if "ADVANTAGE FUND" in lu and "TIER" in lu:
                return 'NPS - Equity Advantage Fund'
            for k, v in SCHEME_MAP.items():
                if k == 'ADVANTAGE FUND':
                    continue
                if k in lu and ("TIER" in lu or "SCHEME" in lu or "MERGER" in lu):
                    return v
            return None


        current_date: datetime | None = None
        in_txn = False
        pending_row: dict | None = None

        def _flush_pending(scheme_label: str) -> None:
            nonlocal pending_row
            if pending_row is not None and scheme_label is not None:
                r = dict(pending_row)
                r['Asset Name'] = scheme_label
                r['Ticker'] = scheme_label
                final_cash.append(r)
            pending_row = None

        for page in pdf.pages:
            text = page.extract_text() or ""
            for line in text.split('\n'):
                s = line.strip()
                lu = s.upper()

                if "TRANSACTION DETAILS" in lu:
                    in_txn = True
                    pending_row = None
                    continue

                dm = re.match(r'^(\d{2}-[A-Za-z]{3}-\d{2})\s+', s)
                if dm:
                    try:
                        current_date = datetime.strptime(dm.group(1), '%d-%b-%y')
                    except ValueError:
                        pass

                scheme = _detect_scheme_label(lu, current_date)
                if scheme is not None:
                    _flush_pending(scheme)
                    continue

                if not in_txn or current_date is None:
                    continue

                if not _is_amount_line(lu):
                    continue

                m3 = _RE3.search(s)
                m2 = _RE2.search(s)

                if m3:
                    units  = _parse_number(m3.group(1))
                    amount = _parse_number(m3.group(3))
                elif m2:
                    units  = _parse_number(m2.group(1))
                    amount = _parse_number(m2.group(2))
                else:
                    continue

                pending_row = {
                    'Portfolio Owner': owner_name,
                    'Asset Class': 'NPS',
                    'Asset Name': None,
                    'Ticker': None,
                    'ISIN': '',
                    'Transaction Type': 'Purchase' if units >= 0 else 'Sell',
                    'Units': abs(units),   # actual units so FIFO correctly dequeues lots for sells
                    'Amount': abs(amount),
                    'Date': current_date.strftime('%Y-%m-%d 00:00:00'),

                }

        pending_row = None  # discard any un-matched trailing row

        # Phase 2.5: Scheme A → Scheme C dynamic merger cost-basis carryover
        # The PDF shows Amount = 0.00 for the merger, which zeroes out the cost basis.
        # We calculate the exact net invested capital of Scheme A prior to the merger
        # and assign it to the transfer rows so the FIFO logic preserves the capital.
        scheme_a_invested = sum(
            r['Amount'] for r in final_cash
            if r['Ticker'] == 'NPS - Scheme A (Alternative Inv)'
            and r['Date'] < '2026-01-16'
            and r['Transaction Type'] == 'Purchase'
        )
        scheme_a_sold = sum(
            r['Amount'] for r in final_cash
            if r['Ticker'] == 'NPS - Scheme A (Alternative Inv)'
            and r['Date'] < '2026-01-16'
            and r['Transaction Type'] == 'Sell'
        )
        merger_transfer_val = round(scheme_a_invested - scheme_a_sold, 2)

        for r in final_cash:
            if r['Amount'] == 0.0 and r['Ticker'] in ('NPS - Scheme A (Alternative Inv)', 'NPS - Scheme C (Corp Debt)'):
                if '2026-01-16' in r['Date'] or '2026-01-17' in r['Date']:
                    r['Amount'] = merger_transfer_val


        # Phase 3 (REMOVED): Earlier parser versions missed multi-line transactions, 
        # relying on Page 1 summary to populate units. Since Phase 2 now captures
        # full history flawlessly, injecting Page 1 summary as fake 'Purchase' rows
        # is removed to stop the Portfolio from artificially doubling its size.

    df = pd.DataFrame(final_cash)
    total_pos = df[df['Transaction Type'] == 'Purchase']['Amount'].sum()
    logger.info("  Total Gross Invested : ₹%s", f"{total_pos:,.2f}")
    return df


def process_all_nps_data() -> None:
    """Auto-discovers NPS PDFs in data/input/nps/ and ingests all of them."""
    logger.info("NPS INGESTION")

    input_path = Path(INPUT_DIR)
    if not input_path.exists():
        logger.warning("No NPS directory found. Skipping.")
        return

    nps_password = os.getenv('NPS_PASSWORD')
    if not nps_password:
        logger.error("NPS_PASSWORD not found in environment variables (.env file).")
        return

    all_dfs: list[pd.DataFrame] = []
    for pdf_file in sorted(input_path.glob('*.pdf')):
        owner = pdf_file.stem.split('_')[0].title()
        df = parse_kfintech_nps(str(pdf_file), nps_password, owner)
        if df is not None and not df.empty:
            all_dfs.append(df)

    if not all_dfs:
        logger.warning("No NPS records generated. Exiting.")
        return

    df_all = pd.concat(all_dfs, ignore_index=True)

    # Update master ledger
    if LEDGER_FILE.exists():
        ex = pd.read_csv(LEDGER_FILE)
        if 'Asset Class' in ex.columns:
            ex = ex[ex['Asset Class'] != 'NPS']
        pd.concat([ex, df_all], ignore_index=True).to_csv(LEDGER_FILE, index=False)
    else:
        df_all.to_csv(LEDGER_FILE, index=False)

    # Update active holdings — compute NET units (purchases minus sells)
    unit_df = df_all[df_all['Asset Name'] != 'NPS - Total'].copy()
    # Assign signed units: positive for purchases, negative for sells/redemptions
    unit_df['Signed_Units'] = unit_df.apply(
        lambda r: r['Units'] if str(r['Transaction Type']).upper() in ('PURCHASE', 'BUY', 'SIP', 'SWITCH IN')
                  else -abs(r['Units']),
        axis=1
    )
    holdings = unit_df.groupby(
        ['Portfolio Owner', 'Asset Class', 'Ticker', 'Asset Name'],
        dropna=False,
        as_index=False,
    )['Signed_Units'].sum().rename(columns={'Signed_Units': 'Units'})
    holdings = holdings[holdings['Units'] > 0.001].copy()
    holdings['Units'] = holdings['Units'].round(4)

    holdings_path = Path(OUTPUT_DIR) / 'mf_active_holdings.csv'
    if holdings_path.exists():
        exh = pd.read_csv(holdings_path)
        if 'Asset Class' in exh.columns:
            exh = exh[exh['Asset Class'] != 'NPS']
        pd.concat([exh, holdings], ignore_index=True).to_csv(holdings_path, index=False)
    else:
        holdings.to_csv(holdings_path, index=False)

    logger.info("ingest_nps complete.")
    logger.info("  Total Cash Flow events: %d", len(df_all[df_all['Amount'] > 0]))
    logger.info("  Per-scheme unit rows  : %d", len(df_all[df_all['Units'] > 0.0]))
    logger.info("  Total Gross Invested  : ₹%s",
                f"{df_all[df_all['Transaction Type'] == 'Purchase']['Amount'].sum():,.2f}")
    logger.info("  Net Holdings by Scheme:\n%s",
                holdings[['Asset Name', 'Units']].to_string(index=False))


if __name__ == "__main__":
    process_all_nps_data()
