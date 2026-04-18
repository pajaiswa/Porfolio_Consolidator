"""
test_ingest_epf.py â€” Tests for pipeline/ingest_epf.py
=======================================================
"""
from pathlib import Path
import pandas as pd
import pytest

from ingestion.ingest_epf import ingest_epf_config


def _write_epf_config(path: Path, rows: list[dict]) -> Path:
    """Helper to write a minimal epf_config.csv to a tmp file."""
    csv_path = path / "epf_config.csv"
    pd.DataFrame(rows).to_csv(csv_path, index=False)
    return csv_path


# ---------------------------------------------------------------------------
# Test 1: Basic ingestion creates correct ledger row
# ---------------------------------------------------------------------------
def test_ingest_epf_basic(tmp_path):
    cfg = _write_epf_config(tmp_path, [{
        "Owner": "Pankaj",
        "Asset_Name": "L&T EPF",
        "Closing_Balance": 500000.0,
        "As_Of_Date": "2024-03-31",
        "Interest_Rate_Pct": 8.25,
    }])
    ledger = tmp_path / "master_ledger.csv"

    result = ingest_epf_config(str(cfg), str(ledger))

    assert result is True
    assert ledger.exists()
    df = pd.read_csv(ledger)
    assert len(df) == 1
    row = df.iloc[0]
    assert row["Portfolio Owner"] == "Pankaj"
    assert row["Asset Class"] == "EPF"
    assert row["Asset Name"] == "L&T EPF"
    assert row["Ticker"] == "EPF_LNT"
    assert float(row["Units"]) == 500000.0
    assert float(row["Amount"]) == 500000.0
    assert row["Transaction Type"] == "Buy"
    assert row["Date"] == "2024-03-31"


# ---------------------------------------------------------------------------
# Test 2: Re-running replaces EPF rows but preserves other asset classes
# ---------------------------------------------------------------------------
def test_ingest_epf_deduplication(tmp_path):
    cfg = _write_epf_config(tmp_path, [{
        "Owner": "Pankaj", "Asset_Name": "L&T EPF",
        "Closing_Balance": 600000.0, "As_Of_Date": "2024-03-31",
        "Interest_Rate_Pct": 8.25,
    }])
    ledger = tmp_path / "master_ledger.csv"

    # Pre-populate ledger with a MF and an old EPF entry
    existing = pd.DataFrame([
        {"Portfolio Owner": "Komal", "Asset Class": "Mutual Fund", "Asset Name": "Fund A",
         "Ticker": "123", "Transaction Type": "Buy", "Units": 10, "Amount": 1000, "Date": "2023-01-01"},
        {"Portfolio Owner": "Pankaj", "Asset Class": "EPF", "Asset Name": "L&T EPF",
         "Ticker": "EPF_LNT", "Transaction Type": "Buy", "Units": 500000, "Amount": 500000, "Date": "2023-03-31"},
    ])
    existing.to_csv(ledger, index=False)

    ingest_epf_config(str(cfg), str(ledger))

    df = pd.read_csv(ledger)
    epf_rows = df[df["Asset Class"] == "EPF"]
    mf_rows = df[df["Asset Class"] == "Mutual Fund"]
    assert len(epf_rows) == 1, "Old EPF row should be replaced"
    assert float(epf_rows.iloc[0]["Amount"]) == 600000.0, "New EPF balance should be 600000"
    assert len(mf_rows) == 1, "MF row must be preserved"


# ---------------------------------------------------------------------------
# Test 3: Missing config file returns False gracefully
# ---------------------------------------------------------------------------
def test_ingest_epf_missing_file(tmp_path):
    result = ingest_epf_config(
        config_path=str(tmp_path / "nonexistent.csv"),
        ledger_path=str(tmp_path / "ledger.csv"),
    )
    assert result is False


# ---------------------------------------------------------------------------
# Test 4: Empty CSV returns False gracefully
# ---------------------------------------------------------------------------
def test_ingest_epf_empty_file(tmp_path):
    empty_csv = tmp_path / "epf_config.csv"
    empty_csv.write_text("Owner,Asset_Name,Closing_Balance,As_Of_Date,Interest_Rate_Pct\n")
    result = ingest_epf_config(str(empty_csv), str(tmp_path / "ledger.csv"))
    assert result is False

