"""
test_ingest_fd.py â€” Tests for pipeline/ingest_fd.py
====================================================
"""
from pathlib import Path
import pandas as pd
import pytest

from ingestion.ingest_fd import ingest_fd_data


def _write_fd_excel(path: Path, rows: list[dict]) -> Path:
    xl_path = path / "FD_details.xlsx"
    pd.DataFrame(rows).to_excel(xl_path, index=False)
    return xl_path


# ---------------------------------------------------------------------------
# Test 1: Basic FD ingestion creates correct ledger row
# ---------------------------------------------------------------------------
def test_ingest_fd_basic(tmp_path):
    xl = _write_fd_excel(tmp_path, [{
        "Owner": "Pankaj", "FD Start Date": "2023-06-01",
        "Invested Amount": 200000.0, "Interest Rate": 7.5,
    }])
    ledger = tmp_path / "master_ledger.csv"

    result = ingest_fd_data(str(xl), str(ledger))

    assert result is True
    df = pd.read_csv(ledger)
    assert len(df) == 1
    row = df.iloc[0]
    assert row["Portfolio Owner"] == "Pankaj"
    assert row["Asset Class"] == "FD"
    assert row["Asset Name"] == "Fixed Deposit"
    assert row["Ticker"] == "FD_Pankaj_200000"  # int formatting, no trailing .0
    assert float(row["Amount"]) == 200000.0
    assert row["Transaction Type"] == "Buy"


# ---------------------------------------------------------------------------
# Test 2: Re-running replaces FD rows, non-FD rows preserved
# ---------------------------------------------------------------------------
def test_ingest_fd_deduplication(tmp_path):
    xl = _write_fd_excel(tmp_path, [{
        "Owner": "Komal", "FD Start Date": "2024-01-15",
        "Invested Amount": 100000.0, "Interest Rate": 6.5,
    }])
    ledger = tmp_path / "master_ledger.csv"

    # Pre-populate with MF and old FD
    existing = pd.DataFrame([
        {"Portfolio Owner": "Pankaj", "Asset Class": "Mutual Fund", "Asset Name": "Fund A",
         "Ticker": "123", "Transaction Type": "Buy", "Units": 10, "Amount": 1000, "Date": "2023-01-01"},
        {"Portfolio Owner": "Komal", "Asset Class": "FD", "Asset Name": "Fixed Deposit",
         "Ticker": "FD_Komal_50000", "Transaction Type": "Buy", "Units": 50000, "Amount": 50000, "Date": "2022-01-01"},
    ])
    existing.to_csv(ledger, index=False)

    ingest_fd_data(str(xl), str(ledger))

    df = pd.read_csv(ledger)
    fd_rows = df[df["Asset Class"] == "FD"]
    mf_rows = df[df["Asset Class"] == "Mutual Fund"]
    assert len(fd_rows) == 1, "Old FD replaced by new"
    assert float(fd_rows.iloc[0]["Amount"]) == 100000.0
    assert len(mf_rows) == 1, "MF row preserved"


# ---------------------------------------------------------------------------
# Test 3: Missing file returns False
# ---------------------------------------------------------------------------
def test_ingest_fd_missing_file(tmp_path):
    result = ingest_fd_data(
        fd_excel_path=str(tmp_path / "nonexistent.xlsx"),
        ledger_path=str(tmp_path / "ledger.csv"),
    )
    assert result is False


# ---------------------------------------------------------------------------
# Test 4: Ledger is sorted by date after FD insertion
# ---------------------------------------------------------------------------
def test_ingest_fd_date_sort(tmp_path):
    xl = _write_fd_excel(tmp_path, [{
        "Owner": "Pankaj", "FD Start Date": "2020-06-01",
        "Invested Amount": 50000.0, "Interest Rate": 6.0,
    }])
    ledger = tmp_path / "master_ledger.csv"

    # Pre-existing ledger entry dated AFTER the FD start date
    existing = pd.DataFrame([{
        "Portfolio Owner": "Pankaj", "Asset Class": "Mutual Fund", "Asset Name": "Fund A",
        "Ticker": "123", "Transaction Type": "Buy", "Units": 10, "Amount": 1000, "Date": "2023-01-01",
    }])
    existing.to_csv(ledger, index=False)

    ingest_fd_data(str(xl), str(ledger))

    df = pd.read_csv(ledger)
    dates = pd.to_datetime(df["Date"])
    assert dates.is_monotonic_increasing, "Ledger should be sorted by date after FD insertion"

