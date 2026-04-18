"""
test_ingest_mf.py â€” Tests for pipeline/ingest_mf.py
=====================================================
Tests AMFI code fuzzy-matching, data consolidation, and net holdings math.
conftest.py ensures the project root is on sys.path.
"""
import pytest
import pandas as pd
import numpy as np

from ingestion.ingest_mf import assign_amfi_codes, clean_and_consolidate, calculate_net_holdings


# ---------------------------------------------------------------------------
# Test 1: AMFI NLP Mapping Logic
# ---------------------------------------------------------------------------
def test_amfi_mapping_logic():
    mock_data = pd.DataFrame({
        'Scheme Name': [
            'Quant Small Cap Fund Direct Plan Growth',
            'ICICI Prudential Passive Multi Asset FoF Direct Growth',
            'Parag Parikh Flexi Cap Fund Direct Growth',
        ]
    })
    mock_data['AMFI'] = None
    df_mapped = assign_amfi_codes(mock_data)

    assert df_mapped.loc[0, 'AMFI'] == '120828', "Failed Quant tokenization"
    assert df_mapped.loc[1, 'AMFI'] == '149441', "Failed FoF lock"
    assert df_mapped.loc[2, 'AMFI'] == '122639', "Failed PPFAS direct lock"


# ---------------------------------------------------------------------------
# Test 2: Data Consolidation & Comma Stripping
# ---------------------------------------------------------------------------
def test_clean_and_consolidate_types():
    mock_data = pd.DataFrame({
        'Scheme Name': ['Fund A - ISIN 12345', 'Fund B'],
        'Transaction Type': ['SIP', 'Lump Sum'],
        'Amount': ['1,000.50', '2000'],
        'Units': ['10.5', '20,000.123'],
        'Date': ['15-01-2023', '01-12-2023'],
    })

    df_clean = clean_and_consolidate([mock_data])

    assert df_clean['Amount'].iloc[0] == 1000.50, "Failed to strip commas from Amount"
    assert df_clean['Units'].iloc[1] == 20000.123, "Failed to strip commas from Units"
    assert df_clean['Scheme Name'].iloc[0] == 'Fund A', "Failed to split ISIN from Scheme Name"


# ---------------------------------------------------------------------------
# Test 3: NaN and Missing Value Handling
# ---------------------------------------------------------------------------
def test_nan_and_missing_values():
    mock_data = pd.DataFrame({
        'Scheme Name': ['Fund A', np.nan, 'Fund C'],
        'Transaction Type': ['Buy', 'Buy', 'Buy'],
        'Amount': ['1000', np.nan, '500'],
        'Units': [np.nan, '10', '5'],
        'Date': ['15-01-2023', '16-01-2023', '17-01-2023'],
    })

    df_clean = clean_and_consolidate([mock_data])

    assert len(df_clean) == 2, "Failed to drop rows with missing Scheme Names"
    assert pd.isna(df_clean.loc[df_clean['Scheme Name'] == 'Fund A', 'Units'].iloc[0])


# ---------------------------------------------------------------------------
# Test 4: Negative Units & Rounding Math
# ---------------------------------------------------------------------------
def test_calculate_net_holdings_math():
    mock_master = pd.DataFrame({
        'Portfolio Owner': ['Komal', 'Komal', 'Komal'],
        'Ticker': ['123', '123', '456'],
        'Asset Name': ['Fund A', 'Fund A', 'Fund B'],
        'Asset Class': ['Mutual Fund', 'Mutual Fund', 'Mutual Fund'],
        'Transaction Type': ['Buy', 'Sell', 'Buy'],
        'Units': [10.5555, 5.0, 0.005],
    })

    holdings = calculate_net_holdings(mock_master)

    fund_a_units = holdings.loc[holdings['Asset Name'] == 'Fund A', 'Units'].iloc[0]
    assert fund_a_units == 5.556, f"Math failed! Expected 5.556, got {fund_a_units}"
    assert 'Fund B' not in holdings['Asset Name'].values, "Failed to filter out inactive holdings"

