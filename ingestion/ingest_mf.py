"""
ingest_mf.py — Mutual Fund Ingestion
======================================
Parses Groww Excel exports and CAMS CAS PDF statements
into the universal master_ledger.csv format.

Inputs  : data/input/mf/*.xlsx  (Groww orders per owner)
          data/input/mf/*.pdf   (CAMS CAS PDF per owner)
Output  : data/output/master_ledger.csv (appends/replaces Mutual Fund rows)
          data/output/mf_active_holdings.csv
"""
import logging
import os
from pathlib import Path

import pandas as pd
import casparser
import difflib
from mftool import Mftool
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 1. Input loaders
# ---------------------------------------------------------------------------

def load_groww_excel(file_path: str, owner_name: str) -> pd.DataFrame:
    """Load a Groww mutual-fund Excel export and tag it with the owner name."""
    logger.info("Loading Groww Excel for %s...", owner_name)
    df = pd.read_excel(file_path, skiprows=11)
    df.insert(0, 'Portfolio Owner', owner_name)
    if 'Status' in df.columns:
        df = df[df['Status'].astype(str).str.strip() != 'Rejected']
    return df


def load_cams_pdf(file_path: str, password: str, owner_name: str) -> pd.DataFrame:
    """Parse a password-protected CAMS CAS PDF and return transaction rows."""
    logger.info("Parsing CAMS PDF for %s...", owner_name)
    # force_pdfminer=True avoids corruption where table parsers choke on wrapped "*** Stamp Duty ***" lines
    parsed_data = casparser.read_cas_pdf(file_path, password, force_pdfminer=True)

    transactions = []
    for folio in parsed_data.get('folios', []):
        for scheme in folio.get('schemes', []):
            for txn in scheme.get('transactions', []):
                amt = txn.get('amount')
                uni = txn.get('units')
                if amt is not None or uni is not None:
                    transactions.append({
                        'Portfolio Owner': owner_name,
                        'Scheme Name': scheme.get('scheme'),
                        'AMFI': scheme.get('amfi'),
                        'Date': txn.get('date'),
                        'Transaction Type': txn.get('description'),
                        'Amount': amt,
                        'Units': uni,
                    })
    return pd.DataFrame(transactions)


# ---------------------------------------------------------------------------
# 2. Data cleaning & normalisation
# ---------------------------------------------------------------------------

def clean_and_consolidate(df_list: list[pd.DataFrame]) -> pd.DataFrame:
    """Merge, deduplicate, and type-cast a list of raw MF DataFrames."""
    logger.info("Consolidating and cleaning master ledger...")
    df_combined = pd.concat(df_list, ignore_index=True)
    df_combined = df_combined.dropna(subset=['Scheme Name'])

    df_combined['Scheme Name'] = (
        df_combined['Scheme Name'].astype(str).str.split(' - ISIN').str[0].str.strip()
    )
    df_combined['Transaction Type'] = df_combined['Transaction Type'].astype(str).str.title()

    for col in ['Units', 'Amount']:
        if col in df_combined.columns:
            df_combined[col] = (
                df_combined[col].astype(str).str.replace(',', '', regex=False)
            )
            df_combined[col] = pd.to_numeric(df_combined[col], errors='coerce')

    df_combined['Date'] = pd.to_datetime(df_combined['Date'], format='mixed', dayfirst=True)
    return df_combined


# ---------------------------------------------------------------------------
# 3. AMFI code assignment
# ---------------------------------------------------------------------------

def assign_amfi_codes(df: pd.DataFrame) -> pd.DataFrame:
    """Fuzzy-match scheme names to official AMFI codes for rows that lack them."""
    logger.info("Standardizing AMFI codes...")
    if 'AMFI' not in df.columns:
        df['AMFI'] = None

    mf = Mftool()
    master_dict = mf.get_scheme_codes()
    official_names = {v.lower(): k for k, v in master_dict.items()}

    missing_amfi_mask = df['AMFI'].isnull()
    unique_missing_schemes = df[missing_amfi_mask]['Scheme Name'].unique()

    mapping: dict[str, str] = {}
    for scheme in unique_missing_schemes:
        clean_scheme = str(scheme).lower().replace('-', ' ')
        groww_words = clean_scheme.split()
        if not groww_words:
            continue

        amc_word = groww_words[0]
        valid_names = [
            n for n in official_names.keys()
            if n.replace('-', ' ').replace('(', ' ').split()[0] == amc_word
        ]

        if "direct" in clean_scheme:
            valid_names = [n for n in valid_names if "direct" in n]
        else:
            valid_names = [n for n in valid_names if "direct" not in n]

        if "growth" in clean_scheme:
            valid_names = [n for n in valid_names if "growth" in n]
        if "fof" in clean_scheme or "fund of" in clean_scheme:
            valid_names = [n for n in valid_names if "fof" in n or "fund of" in n]
        if "small cap" in clean_scheme:
            valid_names = [n for n in valid_names if "small cap" in n]
        if "midcap" in clean_scheme or "mid cap" in clean_scheme:
            valid_names = [n for n in valid_names if "midcap" in n or "mid cap" in n]
        if "flexi cap" in clean_scheme or "flexicap" in clean_scheme:
            valid_names = [n for n in valid_names if "flexi" in n]

        matches = difflib.get_close_matches(clean_scheme, valid_names, n=1, cutoff=0.1)
        mapping[scheme] = official_names[matches[0]] if matches else "UNKNOWN"

    df.loc[missing_amfi_mask, 'AMFI'] = df.loc[missing_amfi_mask, 'Scheme Name'].map(mapping)
    return df


# ---------------------------------------------------------------------------
# 4. Net holdings snapshot
# ---------------------------------------------------------------------------

def calculate_net_holdings(df_universal: pd.DataFrame) -> pd.DataFrame:
    """Compute the net unit balance per (owner, asset) from the universal ledger."""
    logger.info("Calculating net unit holdings snapshot...")
    df_calc = df_universal.copy()

    sell_mask = df_calc['Transaction Type'].str.contains(
        'Redeem|Sell|Redemption', case=False, na=False
    )
    df_calc.loc[sell_mask, 'Units'] = df_calc.loc[sell_mask, 'Units'].abs() * -1

    active_holdings = df_calc.groupby(
        ['Portfolio Owner', 'Asset Class', 'Ticker', 'Asset Name'],
        dropna=False,
        as_index=False,
    )['Units'].sum()
    active_holdings = active_holdings[active_holdings['Units'] > 0.01].copy()
    active_holdings['Units'] = active_holdings['Units'].round(3)

    return active_holdings


# ---------------------------------------------------------------------------
# 5. Main entry-point (auto-discovers input files)
# ---------------------------------------------------------------------------

def process_all_mf_data(
    input_folder: str = 'data/input/mf',
    output_folder: str = 'data/output',
) -> None:
    """
    Auto-discovers all Groww *.xlsx and CAMS *.pdf files in *input_folder*,
    derives the owner name from the filename stem (e.g. 'Champalal_MF.xlsx'
    → owner 'Champalal'), and runs the full ingestion pipeline.
    """
    logger.info("Processing all MF data in %s...", input_folder)

    input_path = Path(input_folder)
    if not input_path.exists():
        logger.error("Input folder '%s' not found.", input_folder)
        return

    cams_password = os.getenv('CAMS_PASSWORD')

    df_list: list[pd.DataFrame] = []

    # --- Groww Excel exports (.xlsx) ---
    for xl_file in sorted(input_path.glob('*.xlsx')):
        owner_name = xl_file.stem.split('_')[0].title()
        df_list.append(load_groww_excel(str(xl_file), owner_name))

    # --- CAMS CAS PDFs (.pdf) ---
    for pdf_file in sorted(input_path.glob('*.pdf')):
        owner_name = pdf_file.stem.split('_')[0].title()
        if not cams_password:
            logger.error("CAMS_PASSWORD not found in environment variables (.env file).")
            return
        df_list.append(load_cams_pdf(str(pdf_file), cams_password, owner_name))

    if not df_list:
        logger.warning("No MF input files found. Skipping.")
        return

    master_ledger = clean_and_consolidate(df_list)
    master_ledger = assign_amfi_codes(master_ledger)

    # Map to universal ledger schema
    master_ledger['Asset Class'] = 'Mutual Fund'
    master_ledger['Asset Name'] = master_ledger['Scheme Name']
    # Normalise AMFI codes: Excel stores them as floats (e.g. "120828.0") — strip trailing .0
    master_ledger['Ticker'] = (
        master_ledger['AMFI']
        .astype(str)
        .str.strip()
        .str.replace(r'\.0$', '', regex=True)
    )
    master_ledger['ISIN'] = ''

    universal_columns = [
        'Portfolio Owner', 'Asset Class', 'Asset Name', 'Ticker',
        'ISIN', 'Transaction Type', 'Units', 'Amount', 'Date',
    ]
    df_universal = master_ledger[universal_columns].copy()

    active_holdings = calculate_net_holdings(df_universal)

    output_path = Path(output_folder)
    output_path.mkdir(parents=True, exist_ok=True)

    # Merge with existing ledger, replacing MF rows only
    master_ledger_path = output_path / 'master_ledger.csv'
    if master_ledger_path.exists():
        existing_ledger = pd.read_csv(master_ledger_path)
        if 'Asset Class' in existing_ledger.columns:
            existing_ledger = existing_ledger[existing_ledger['Asset Class'] != 'Mutual Fund']
        updated_ledger = pd.concat([existing_ledger, df_universal], ignore_index=True)
        updated_ledger.to_csv(master_ledger_path, index=False)
    else:
        df_universal.to_csv(master_ledger_path, index=False)

    active_holdings.to_csv(output_path / 'mf_active_holdings.csv', index=False)
    logger.info("ingest_mf complete. Files saved to %s/", output_folder)


if __name__ == "__main__":
    process_all_mf_data()
