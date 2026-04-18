"""
test_valuate_fd.py â€” Tests for pipeline/valuate_fd.py
======================================================
"""
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import pytest

from valuation.valuate_fd import calculate_fd_valuation


def _write_fd_excel(path: Path, rows: list[dict]) -> Path:
    xl_path = path / "FD_details.xlsx"
    pd.DataFrame(rows).to_excel(xl_path, index=False)
    return xl_path


# ---------------------------------------------------------------------------
# Test 1: Interest accrual math
# ---------------------------------------------------------------------------
def test_fd_interest_accrual_math(tmp_path):
    # 200,000 at 6% for ~365 days => ~212,000
    start = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    xl = _write_fd_excel(tmp_path, [{
        "Owner": "Pankaj", "FD Start Date": start,
        "Invested Amount": 200000.0, "Interest Rate": 6.0,
    }])
    val_csv = tmp_path / "master_valuation.csv"

    calculate_fd_valuation(str(xl), str(val_csv))

    df = pd.read_csv(val_csv)
    live_value = float(df.iloc[0]["Current Value"])
    # 200000 * 0.06 * 1 = 12000 interest, allow Â±200
    assert 211500 <= live_value <= 212500, f"Expected ~212000, got {live_value}"


# ---------------------------------------------------------------------------
# Test 2: Start date == today â†’ live value == invested amount
# ---------------------------------------------------------------------------
def test_fd_zero_elapsed(tmp_path):
    today = datetime.now().strftime("%Y-%m-%d")
    xl = _write_fd_excel(tmp_path, [{
        "Owner": "Komal", "FD Start Date": today,
        "Invested Amount": 150000.0, "Interest Rate": 7.0,
    }])
    val_csv = tmp_path / "master_valuation.csv"

    calculate_fd_valuation(str(xl), str(val_csv))

    df = pd.read_csv(val_csv)
    assert float(df.iloc[0]["Current Value"]) == 150000.0


# ---------------------------------------------------------------------------
# Test 3: Missing file returns False
# ---------------------------------------------------------------------------
def test_valuate_fd_missing_file(tmp_path):
    result = calculate_fd_valuation(
        fd_excel_path=str(tmp_path / "nonexistent.xlsx"),
        valuation_path=str(tmp_path / "val.csv"),
    )
    assert result is False


# ---------------------------------------------------------------------------
# Test 4: Re-running replaces FD valuation rows, preserves others
# ---------------------------------------------------------------------------
def test_valuate_fd_deduplication(tmp_path):
    today = datetime.now().strftime("%Y-%m-%d")
    xl = _write_fd_excel(tmp_path, [{
        "Owner": "Pankaj", "FD Start Date": today,
        "Invested Amount": 75000.0, "Interest Rate": 6.0,
    }])
    val_csv = tmp_path / "master_valuation.csv"

    existing = pd.DataFrame([
        {"Portfolio Owner": "Komal", "Asset Class": "Mutual Fund", "Asset Name": "Fund A",
         "Ticker": "123", "Units": 10, "Live NAV": 120, "Current Value": 1200},
        {"Portfolio Owner": "Pankaj", "Asset Class": "FD", "Asset Name": "Fixed Deposit",
         "Ticker": "FD_Pankaj_50000", "Units": 50000, "Live NAV": 1.0, "Current Value": 50000},
    ])
    existing.to_csv(val_csv, index=False)

    calculate_fd_valuation(str(xl), str(val_csv))

    df = pd.read_csv(val_csv)
    fd_rows = df[df["Asset Class"] == "FD"]
    mf_rows = df[df["Asset Class"] == "Mutual Fund"]
    assert len(fd_rows) == 1, "Old FD row replaced"
    assert float(fd_rows.iloc[0]["Current Value"]) == 75000.0
    assert len(mf_rows) == 1, "MF row preserved"

