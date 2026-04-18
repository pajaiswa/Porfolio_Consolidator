"""
test_valuate_epf.py â€” Tests for pipeline/valuate_epf.py
========================================================
"""
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import pytest

from valuation.valuate_epf import calculate_epf_valuation


def _write_epf_config(path: Path, rows: list[dict]) -> Path:
    csv_path = path / "epf_config.csv"
    pd.DataFrame(rows).to_csv(csv_path, index=False)
    return csv_path


# ---------------------------------------------------------------------------
# Test 1: Simple interest accrual math
# ---------------------------------------------------------------------------
def test_epf_interest_accrual(tmp_path):
    # A balance of 100,000 at 8.25% for exactly 365.25 days should accrue ~8250 interest
    as_of = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    cfg = _write_epf_config(tmp_path, [{
        "Owner": "Pankaj", "Asset_Name": "L&T EPF",
        "Closing_Balance": 100000.0, "As_Of_Date": as_of, "Interest_Rate_Pct": 8.25,
    }])
    val_csv = tmp_path / "master_valuation.csv"

    calculate_epf_valuation(str(cfg), str(val_csv))

    df = pd.read_csv(val_csv)
    assert len(df) == 1
    live_value = df.iloc[0]["Current Value"]
    # Allow Â±200 tolerance for partial-day rounding
    assert 108000 <= live_value <= 108600, f"Expected ~108250, got {live_value}"


# ---------------------------------------------------------------------------
# Test 2: Zero days elapsed â†’ live value equals closing balance
# ---------------------------------------------------------------------------
def test_epf_zero_days_elapsed(tmp_path):
    today = datetime.now().strftime("%Y-%m-%d")
    cfg = _write_epf_config(tmp_path, [{
        "Owner": "Komal", "Asset_Name": "L&T EPF",
        "Closing_Balance": 250000.0, "As_Of_Date": today, "Interest_Rate_Pct": 8.25,
    }])
    val_csv = tmp_path / "master_valuation.csv"

    calculate_epf_valuation(str(cfg), str(val_csv))

    df = pd.read_csv(val_csv)
    assert float(df.iloc[0]["Current Value"]) == 250000.0


# ---------------------------------------------------------------------------
# Test 3: Missing config file returns False
# ---------------------------------------------------------------------------
def test_valuate_epf_missing_file(tmp_path):
    result = calculate_epf_valuation(
        config_path=str(tmp_path / "nonexistent.csv"),
        valuation_path=str(tmp_path / "val.csv"),
    )
    assert result is False


# ---------------------------------------------------------------------------
# Test 4: Re-running replaces EPF valuation rows, preserves others
# ---------------------------------------------------------------------------
def test_valuate_epf_deduplication(tmp_path):
    today = datetime.now().strftime("%Y-%m-%d")
    cfg = _write_epf_config(tmp_path, [{
        "Owner": "Pankaj", "Asset_Name": "L&T EPF",
        "Closing_Balance": 300000.0, "As_Of_Date": today, "Interest_Rate_Pct": 8.25,
    }])
    val_csv = tmp_path / "master_valuation.csv"

    # Pre-populate with existing MF and EPF rows
    existing = pd.DataFrame([
        {"Portfolio Owner": "Komal", "Asset Class": "Mutual Fund", "Asset Name": "Fund A",
         "Ticker": "123", "Units": 10, "Live NAV": 120, "Current Value": 1200},
        {"Portfolio Owner": "Pankaj", "Asset Class": "EPF", "Asset Name": "L&T EPF",
         "Ticker": "EPF_LNT", "Units": 200000, "Live NAV": 1.0, "Current Value": 200000},
    ])
    existing.to_csv(val_csv, index=False)

    calculate_epf_valuation(str(cfg), str(val_csv))

    df = pd.read_csv(val_csv)
    epf_rows = df[df["Asset Class"] == "EPF"]
    mf_rows = df[df["Asset Class"] == "Mutual Fund"]
    assert len(epf_rows) == 1, "Old EPF row should be replaced"
    assert float(epf_rows.iloc[0]["Current Value"]) == 300000.0
    assert len(mf_rows) == 1, "MF row must be preserved"

