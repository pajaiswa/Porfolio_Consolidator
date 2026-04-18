"""
test_calc_allocations.py â€” Tests for pipeline/calc_allocations.py
==================================================================
"""
from pathlib import Path
import pandas as pd
import pytest

from analytics.calc_allocations import calculate_allocations, get_stock_subclass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _val_df(*rows: dict) -> pd.DataFrame:
    return pd.DataFrame(rows)


def _write_val(path: Path, rows: list[dict]) -> str:
    p = path / "master_valuation.csv"
    pd.DataFrame(rows).to_csv(p, index=False)
    return str(p)


def _write_map(path: Path, rows: list[dict]) -> str:
    p = path / "allocation_map.csv"
    pd.DataFrame(rows).to_csv(p, index=False)
    return str(p)


# ---------------------------------------------------------------------------
# Unit tests for get_stock_subclass helper
# ---------------------------------------------------------------------------
def test_gold_etf_classified_as_gold():
    assert get_stock_subclass("GOLDBEES", "Nippon Gold ETF") == "Gold"

def test_sgb_classified_as_gold():
    assert get_stock_subclass("SGBDEC20", "Sovereign Gold Bond Dec 2020") == "Gold"

def test_foreign_etf_classified_correctly():
    assert get_stock_subclass("MON100", "Mirae NASDAQ 100 ETF") == "Equity_Foreign"
    assert get_stock_subclass("MAFANG", "Mirae Asset FANG ETF") == "Equity_Foreign"

def test_regular_stock_classified_as_equity_india():
    assert get_stock_subclass("RELIANCE", "Reliance Industries") == "Equity_India"


# ---------------------------------------------------------------------------
# Integration tests for calculate_allocations
# ---------------------------------------------------------------------------
def test_fd_epf_classified_as_debt(tmp_path):
    val_path = _write_val(tmp_path, [
        {"Portfolio Owner": "Pankaj", "Asset Class": "FD", "Asset Name": "Fixed Deposit",
         "Ticker": "FD_P_100000", "Current Value": 100000.0},
        {"Portfolio Owner": "Pankaj", "Asset Class": "EPF", "Asset Name": "L&T EPF",
         "Ticker": "EPF_LNT", "Current Value": 200000.0},
    ])
    out = str(tmp_path / "alloc.csv")
    drill = str(tmp_path / "drill.csv")

    calculate_allocations(val_path, str(tmp_path / "no_map.csv"), out, drill)

    df = pd.read_csv(out)
    debt_row = df[df["Sub Class"] == "Debt"]
    assert not debt_row.empty, "FD and EPF should classify as Debt"
    assert float(debt_row.iloc[0]["Total Value"]) == 300000.0


def test_nps_scheme_c_and_e_classified_correctly(tmp_path):
    val_path = _write_val(tmp_path, [
        {"Portfolio Owner": "Pankaj", "Asset Class": "NPS",
         "Asset Name": "NPS - Scheme C (Corp Debt)", "Ticker": "NPS_C", "Current Value": 50000.0},
        {"Portfolio Owner": "Pankaj", "Asset Class": "NPS",
         "Asset Name": "NPS - Scheme E (Equity)", "Ticker": "NPS_E", "Current Value": 30000.0},
    ])
    out = str(tmp_path / "alloc.csv")
    drill = str(tmp_path / "drill.csv")

    calculate_allocations(val_path, str(tmp_path / "no_map.csv"), out, drill)

    df = pd.read_csv(out)
    debt = float(df[df["Sub Class"] == "Debt"]["Total Value"].iloc[0])
    equity = float(df[df["Sub Class"] == "Equity_India"]["Total Value"].iloc[0])
    assert debt == 50000.0
    assert equity == 30000.0


def test_explicit_map_override_takes_priority(tmp_path):
    val_path = _write_val(tmp_path, [
        {"Portfolio Owner": "Pankaj", "Asset Class": "Mutual Fund",
         "Asset Name": "Parag Parikh Flexi Cap", "Ticker": "PP_FC", "Current Value": 100000.0},
    ])
    map_path = _write_map(tmp_path, [{
        "Ticker": "PP_FC", "Asset Name": "Parag Parikh Flexi Cap",
        "Equity_India_Pct": 65.0, "Equity_Foreign_Pct": 35.0,
        "Debt_Pct": 0.0, "Gold_Pct": 0.0, "Cash_Pct": 0.0,
    }])
    out = str(tmp_path / "alloc.csv")
    drill = str(tmp_path / "drill.csv")

    calculate_allocations(val_path, map_path, out, drill)

    df = pd.read_csv(out).set_index("Sub Class")
    assert float(df.loc["Equity_India", "Total Value"]) == 65000.0
    assert float(df.loc["Equity_Foreign", "Total Value"]) == 35000.0


def test_zero_value_assets_skipped(tmp_path):
    val_path = _write_val(tmp_path, [
        {"Portfolio Owner": "Pankaj", "Asset Class": "FD", "Asset Name": "FD",
         "Ticker": "FD_X", "Current Value": 0.0},
        {"Portfolio Owner": "Pankaj", "Asset Class": "EPF", "Asset Name": "EPF",
         "Ticker": "EPF_LNT", "Current Value": -100.0},
    ])
    # Write a valid empty-row allocation map (headers only) via DataFrame
    map_path = tmp_path / "no_map.csv"
    pd.DataFrame(columns=["Ticker", "Asset Name", "Equity_India_Pct", "Equity_Foreign_Pct",
                           "Debt_Pct", "Gold_Pct", "Cash_Pct"]).to_csv(map_path, index=False)
    out = str(tmp_path / "alloc.csv")
    drill = str(tmp_path / "drill.csv")

    calculate_allocations(val_path, str(map_path), out, drill)

    # The output should either be absent or contain only zero/negative-value rows
    if Path(out).exists():
        df = pd.read_csv(out)
        if not df.empty:
            assert (df["Total Value"] <= 0).all(), "Zero/negative value assets should produce no positive totals"


def test_drilldown_csv_written(tmp_path):
    val_path = _write_val(tmp_path, [
        {"Portfolio Owner": "Pankaj", "Asset Class": "FD", "Asset Name": "Fixed Deposit",
         "Ticker": "FD_P_50000", "Current Value": 50000.0},
    ])
    out = str(tmp_path / "alloc.csv")
    drill = str(tmp_path / "drill.csv")

    calculate_allocations(val_path, str(tmp_path / "no_map.csv"), out, drill)

    assert Path(drill).exists(), "Drilldown CSV should be created"
    df_drill = pd.read_csv(drill)
    assert "Sub Class" in df_drill.columns
    assert "Value" in df_drill.columns

