"""
conftest.py — Shared pytest configuration and fixtures
=======================================================
Adds the project root to sys.path so all tests can import from any package
(ingestion, valuation, analytics, ai_advisor, core, dashboard) without
per-file sys.path hacks.
"""
import sys
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import pytest

# Insert the project root (parent of tests/) at the front of sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def sample_ledger_df() -> pd.DataFrame:
    """10-row universal ledger with a mix of asset classes and transaction types."""
    return pd.DataFrame([
        {"Portfolio Owner": "Pankaj",    "Asset Class": "Mutual Fund", "Asset Name": "Fund A",
         "Ticker": "T1", "Transaction Type": "BUY",        "Units": 10.0,  "Amount": 1000.0,  "Date": "01-01-2023"},
        {"Portfolio Owner": "Pankaj",    "Asset Class": "Mutual Fund", "Asset Name": "Fund A",
         "Ticker": "T1", "Transaction Type": "SIP",        "Units": 5.0,   "Amount": 500.0,   "Date": "01-02-2023"},
        {"Portfolio Owner": "Pankaj",    "Asset Class": "Mutual Fund", "Asset Name": "Fund A",
         "Ticker": "T1", "Transaction Type": "REDEMPTION", "Units": 3.0,   "Amount": 350.0,   "Date": "01-06-2023"},
        {"Portfolio Owner": "Komal",     "Asset Class": "Mutual Fund", "Asset Name": "Fund B",
         "Ticker": "T2", "Transaction Type": "BUY",        "Units": 20.0,  "Amount": 2000.0,  "Date": "15-01-2023"},
        {"Portfolio Owner": "Komal",     "Asset Class": "STOCK",       "Asset Name": "Stock X",
         "Ticker": "SX", "Transaction Type": "BUY",        "Units": 5.0,   "Amount": 2500.0,  "Date": "01-03-2023"},
        {"Portfolio Owner": "Champalal", "Asset Class": "EPF",         "Asset Name": "L&T EPF",
         "Ticker": "EPF_LNT", "Transaction Type": "Buy",   "Units": 300000.0, "Amount": 300000.0, "Date": "31-03-2023"},
        {"Portfolio Owner": "Champalal", "Asset Class": "FD",          "Asset Name": "Fixed Deposit",
         "Ticker": "FD_C_100000", "Transaction Type": "Buy", "Units": 100000.0, "Amount": 100000.0, "Date": "01-04-2023"},
        {"Portfolio Owner": "Pankaj",    "Asset Class": "NPS",         "Asset Name": "NPS - Scheme E (Equity)",
         "Ticker": "NPS_E", "Transaction Type": "Purchase", "Units": 1000.0, "Amount": 50000.0, "Date": "01-01-2023"},
        {"Portfolio Owner": "Pankaj",    "Asset Class": "NPS",         "Asset Name": "NPS - Scheme C (Corp Debt)",
         "Ticker": "NPS_C", "Transaction Type": "Purchase", "Units": 800.0,  "Amount": 40000.0, "Date": "01-01-2023"},
        {"Portfolio Owner": "Komal",     "Asset Class": "Mutual Fund", "Asset Name": "Fund C",
         "Ticker": "T3", "Transaction Type": "SWITCH IN",  "Units": 8.0,   "Amount": 800.0,   "Date": "01-07-2023"},
    ])


@pytest.fixture()
def sample_valuation_df() -> pd.DataFrame:
    """Matching valuation rows for sample_ledger_df owners and tickers."""
    return pd.DataFrame([
        {"Portfolio Owner": "Pankaj",    "Asset Class": "Mutual Fund", "Asset Name": "Fund A",
         "Ticker": "T1", "Units": 12.0, "Live NAV": 120.0, "Current Value": 1440.0},
        {"Portfolio Owner": "Komal",     "Asset Class": "Mutual Fund", "Asset Name": "Fund B",
         "Ticker": "T2", "Units": 20.0, "Live NAV": 110.0, "Current Value": 2200.0},
        {"Portfolio Owner": "Komal",     "Asset Class": "STOCK",       "Asset Name": "Stock X",
         "Ticker": "SX", "Units": 5.0,  "Live NAV": 600.0, "Current Value": 3000.0},
        {"Portfolio Owner": "Champalal", "Asset Class": "EPF",         "Asset Name": "L&T EPF",
         "Ticker": "EPF_LNT", "Units": 300000.0, "Live NAV": 1.03, "Current Value": 309000.0},
        {"Portfolio Owner": "Champalal", "Asset Class": "FD",          "Asset Name": "Fixed Deposit",
         "Ticker": "FD_C_100000", "Units": 100000.0, "Live NAV": 1.065, "Current Value": 106500.0},
        {"Portfolio Owner": "Pankaj",    "Asset Class": "NPS",         "Asset Name": "NPS - Scheme E (Equity)",
         "Ticker": "NPS_E", "Units": 1000.0, "Live NAV": 55.0, "Current Value": 55000.0},
        {"Portfolio Owner": "Pankaj",    "Asset Class": "NPS",         "Asset Name": "NPS - Scheme C (Corp Debt)",
         "Ticker": "NPS_C", "Units": 800.0,  "Live NAV": 52.0, "Current Value": 41600.0},
    ])


@pytest.fixture()
def tmp_epf_config(tmp_path) -> Path:
    """Writes a minimal epf_config.csv to tmp_path and returns its path."""
    cfg = pd.DataFrame([{
        "Owner": "Pankaj", "Asset_Name": "L&T EPF",
        "Closing_Balance": 500000.0, "As_Of_Date": "2024-03-31",
        "Interest_Rate_Pct": 8.25,
    }])
    p = tmp_path / "epf_config.csv"
    cfg.to_csv(p, index=False)
    return p


@pytest.fixture()
def tmp_fd_excel(tmp_path) -> Path:
    """Writes a minimal FD_details.xlsx to tmp_path and returns its path."""
    fd = pd.DataFrame([{
        "Owner": "Pankaj", "FD Start Date": "2023-06-01",
        "Invested Amount": 200000.0, "Interest Rate": 7.5,
    }])
    p = tmp_path / "FD_details.xlsx"
    fd.to_excel(p, index=False)
    return p


