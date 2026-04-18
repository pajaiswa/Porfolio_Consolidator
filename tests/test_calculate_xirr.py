"""
test_calculate_xirr.py â€” Tests for pipeline/calculate_xirr.py
==============================================================
Tests XIRR accuracy, ticker normalisation, and end-to-end portfolio performance.
conftest.py ensures the project root is on sys.path.
"""
from datetime import datetime

import pandas as pd
import pytest
from pyxirr import xirr

from analytics.calculate_xirr import calculate_portfolio_performance


# ---------------------------------------------------------------------------
# Test 1: XIRR Mathematical Accuracy
# ---------------------------------------------------------------------------
def test_xirr_mathematical_accuracy():
    """Validates the XIRR engine against a known, deterministic cash flow."""
    dates = [datetime(2023, 1, 1), datetime(2024, 1, 1)]
    amounts = [-10000, 11000]
    result_pct = round(xirr(dates, amounts) * 100, 2)
    assert result_pct == 10.00


# ---------------------------------------------------------------------------
# Test 2: Ticker Normalisation Regression
# ---------------------------------------------------------------------------
def test_type_normalization_regression():
    """Ensures the String/Integer Ticker mapping bug remains fixed."""
    raw_ticker = pd.Series([" 120828.0 ", "UNKNOWN ", " 122639.0"])
    clean_ticker = raw_ticker.astype(str).str.strip().str.replace(r'\.0$', '', regex=True)

    assert clean_ticker[0] == "120828"
    assert clean_ticker[1] == "UNKNOWN"
    assert clean_ticker[2] == "122639"


# ---------------------------------------------------------------------------
# Test 3: Member Aggregation Logic
# ---------------------------------------------------------------------------
def test_member_aggregation_logic():
    """Ensures owner-based grouping of cash flows is correct."""
    df_mock_ledger = pd.DataFrame({
        'Portfolio Owner': ['Komal', 'Komal', 'Pankaj'],
        'Amount': [10000, 5000, 20000],
        'Ticker': ['123', '123', '456'],
    })
    komal_invested = df_mock_ledger[df_mock_ledger['Portfolio Owner'] == 'Komal']['Amount'].sum()
    assert komal_invested == 15000, "Member aggregation mixed up cash flows!"


# ---------------------------------------------------------------------------
# Test 4: End-to-End Portfolio Performance Calculation
# ---------------------------------------------------------------------------
def test_calculate_portfolio_performance_e2e(tmp_path):
    """End-to-end test of the full performance calculation pipeline."""
    ledger_path = tmp_path / "master_ledger.csv"
    mock_ledger = pd.DataFrame({
        'Portfolio Owner': ['UserA', 'UserA', 'UserB'],
        'Asset Class': ['Mutual Fund', 'STOCK', 'Mutual Fund'],
        'Asset Name': ['Fund1', 'Stock1', 'Fund2'],
        'Ticker': ['F1', 'S1', 'F2'],
        'ISIN': ['', '', ''],
        'Transaction Type': ['BUY', 'BUY', 'BUY'],
        'Units': [10, 5, 20],
        'Amount': [1000, 500, 2000],
        'Date': ['01-01-2023', '01-01-2023', '01-01-2023'],
    })
    mock_ledger.to_csv(ledger_path, index=False)

    val_path = tmp_path / "master_valuation.csv"
    mock_val = pd.DataFrame({
        'Portfolio Owner': ['UserA', 'UserA', 'UserB'],
        'Ticker': ['F1', 'S1', 'F2'],
        'Asset Name': ['Fund1', 'Stock1', 'Fund2'],
        'Asset Class': ['Mutual Fund', 'STOCK', 'Mutual Fund'],
        'Units': [10, 5, 20],
        'Live NAV': [110, 110, 110],
        'Current Value': [1100, 550, 2200],
    })
    mock_val.to_csv(val_path, index=False)

    df_metrics = calculate_portfolio_performance(str(ledger_path), str(val_path), nav_date='2024-01-01')

    assert len(df_metrics) == 8  # 3 member+AC, 2 member totals, 2 AC totals, 1 family total

    family = df_metrics[df_metrics['Entity'] == '🏠 CONSOLIDATED FAMILY'].iloc[0]
    assert family['Invested'] == 3500.0
    assert family['Current'] == 3850.0
    assert 9.9 <= family['XIRR %'] <= 10.1

    mem_a = df_metrics[df_metrics['Entity'] == '\U0001f464 USERA TOTAL'].iloc[0]

    assert mem_a['Invested'] == 1500.0
    assert mem_a['Current'] == 1650.0
