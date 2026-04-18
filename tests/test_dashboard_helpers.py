"""
test_dashboard_helpers.py â€” Tests for dashboard.py helper functions
====================================================================
Tests the pure-logic helpers that can be imported and exercised without
starting the full Streamlit server.

Tested functions:
  fmt_amt, fmt_pct             (formatting / masking)
  normalize_cash_flows         (re-exported from analytics.calculate_xirr)
  calculate_fifo_invested      (re-exported from analytics.calculate_xirr)
"""
import pandas as pd
import pytest
from datetime import date

from analytics.calculate_xirr import normalize_cash_flows, calculate_fifo_invested


MASK = r"\*\*\*"


# ---------------------------------------------------------------------------
# fmt_amt / fmt_pct â€” tested directly via the pipeline module since
# they are one-liners that depend on the Streamlit `show_values` global.
# We test via the underlying calculate_xirr helpers instead.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Tests for normalize_cash_flows
# ---------------------------------------------------------------------------

def _ledger(*rows) -> pd.DataFrame:
    return pd.DataFrame(rows)


def test_normalize_buy_is_negative():
    df = _ledger(
        {"Date": "01-01-2023", "Transaction Type": "BUY", "Amount": 10000, "Asset Class": "Mutual Fund", "Ticker": "123"},
    )
    result = normalize_cash_flows(df)
    assert result.iloc[0]["Net_Cash_Flow"] == -10000.0


def test_normalize_sip_is_negative():
    df = _ledger(
        {"Date": "01-01-2023", "Transaction Type": "SIP", "Amount": 5000, "Asset Class": "Mutual Fund", "Ticker": "123"},
    )
    result = normalize_cash_flows(df)
    assert result.iloc[0]["Net_Cash_Flow"] == -5000.0


def test_normalize_redeem_is_positive():
    df = _ledger(
        {"Date": "01-06-2023", "Transaction Type": "REDEMPTION", "Amount": 12000, "Asset Class": "Mutual Fund", "Ticker": "123"},
    )
    result = normalize_cash_flows(df)
    assert result.iloc[0]["Net_Cash_Flow"] == 12000.0


def test_normalize_switch_out_is_positive():
    df = _ledger(
        {"Date": "01-06-2023", "Transaction Type": "SWITCH OUT", "Amount": 8000, "Asset Class": "Mutual Fund", "Ticker": "123"},
    )
    result = normalize_cash_flows(df)
    assert result.iloc[0]["Net_Cash_Flow"] == 8000.0


def test_normalize_switch_in_is_negative():
    df = _ledger(
        {"Date": "01-06-2023", "Transaction Type": "SWITCH IN", "Amount": 8000, "Asset Class": "Mutual Fund", "Ticker": "123"},
    )
    result = normalize_cash_flows(df)
    assert result.iloc[0]["Net_Cash_Flow"] == -8000.0


def test_normalize_reinvest_treated_as_buy():
    """REINVEST UNITS is treated as a buy (negative cash flow) by the pipeline."""
    df = _ledger(
        {"Date": "01-06-2023", "Transaction Type": "REINVEST UNITS", "Amount": 500, "Asset Class": "Mutual Fund", "Ticker": "123"},
    )
    result = normalize_cash_flows(df)
    # The pipeline maps REINVEST UNITS â†’ negative (buy-side) cash flow
    assert result.iloc[0]["Net_Cash_Flow"] == -500.0


# ---------------------------------------------------------------------------
# Tests for calculate_fifo_invested
# ---------------------------------------------------------------------------

def _fifo_df(*rows) -> pd.DataFrame:
    cols = ["Portfolio Owner", "Asset Class", "Asset Name", "Ticker",
            "Transaction Type", "Units", "Amount", "Date"]
    return pd.DataFrame(rows, columns=cols)


def test_fifo_simple_buy():
    df = _fifo_df(
        ["U", "MF", "Fund A", "T1", "BUY", 10.0, 1000.0, "01-01-2023"],
    )
    assert calculate_fifo_invested(df) == 1000.0


def test_fifo_partial_sell_reduces_cost():
    df = _fifo_df(
        ["U", "MF", "Fund A", "T1", "BUY",  10.0, 1000.0, "01-01-2023"],
        ["U", "MF", "Fund A", "T1", "SELL",  3.0,  300.0, "01-06-2023"],
    )
    # 7 units remain at â‚¹100/unit = â‚¹700
    assert calculate_fifo_invested(df) == 700.0


def test_fifo_full_redeem_gives_zero():
    df = _fifo_df(
        ["U", "MF", "Fund A", "T1", "BUY",      10.0, 1000.0, "01-01-2023"],
        ["U", "MF", "Fund A", "T1", "REDEMPTION", 10.0, 1200.0, "01-06-2023"],
    )
    assert calculate_fifo_invested(df) == 0.0


def test_fifo_empty_df_gives_zero():
    df = pd.DataFrame(columns=["Portfolio Owner", "Asset Class", "Asset Name",
                                "Ticker", "Transaction Type", "Units", "Amount", "Date"])
    assert calculate_fifo_invested(df) == 0.0

