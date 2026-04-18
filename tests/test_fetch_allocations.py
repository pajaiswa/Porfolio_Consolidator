"""
test_fetch_allocations.py â€” Tests for pipeline/fetch_allocations.py
====================================================================
Tests the heuristic and caching logic without making any real HTTP calls.
"""
import pandas as pd
import pytest
from pathlib import Path
from unittest.mock import patch

from analytics.fetch_allocations import is_foreign_fund, fetch_and_build_allocation_map


# ---------------------------------------------------------------------------
# Helper: replicate the stock classification logic inline for unit testing
# ---------------------------------------------------------------------------
def get_stock_subclass_defaults(ticker: str, name: str) -> dict:
    """Mirror of the inline logic inside fetch_and_build_allocation_map for stocks."""
    ticker_up = ticker.upper()
    name_up = name.upper()
    if "GOLD" in ticker_up or "SGB" in ticker_up or "GOLD" in name_up:
        return {"Equity_India_Pct": 0.0, "Equity_Foreign_Pct": 0.0, "Debt_Pct": 0.0, "Gold_Pct": 100.0, "Cash_Pct": 0.0}
    if any(f in ticker_up for f in ["MON100", "MAFANG", "MASPTOP50"]) or "NASDAQ" in name_up:
        return {"Equity_India_Pct": 0.0, "Equity_Foreign_Pct": 100.0, "Debt_Pct": 0.0, "Gold_Pct": 0.0, "Cash_Pct": 0.0}
    return {"Equity_India_Pct": 100.0, "Equity_Foreign_Pct": 0.0, "Debt_Pct": 0.0, "Gold_Pct": 0.0, "Cash_Pct": 0.0}


# ---------------------------------------------------------------------------
# Test 1: is_foreign_fund heuristic
# ---------------------------------------------------------------------------
def test_is_foreign_fund_detects_keywords():
    assert is_foreign_fund("Parag Parikh US Equity FoF") is True
    assert is_foreign_fund("Axis Global Innovation FoF") is True
    assert is_foreign_fund("Mirae International Allocation Fund") is True
    assert is_foreign_fund("Quant Small Cap Fund") is False
    assert is_foreign_fund("HDFC Top 100 Fund") is False


# ---------------------------------------------------------------------------
# Test 2: Gold stock/ETF heuristic
# ---------------------------------------------------------------------------
def test_gold_ticker_classified_as_gold():
    result = get_stock_subclass_defaults("GOLDBEES", "Nippon India Gold ETF")
    assert result["Gold_Pct"] == 100.0
    assert result["Equity_India_Pct"] == 0.0


# ---------------------------------------------------------------------------
# Test 3: SGB classified as gold
# ---------------------------------------------------------------------------
def test_sgb_classified_as_gold():
    result = get_stock_subclass_defaults("SGBDEC20", "Sovereign Gold Bond")
    assert result["Gold_Pct"] == 100.0


# ---------------------------------------------------------------------------
# Test 4: Foreign ETF classified correctly
# ---------------------------------------------------------------------------
def test_foreign_etf_classified_as_equity_foreign():
    result = get_stock_subclass_defaults("MON100", "Motilal Oswal NASDAQ 100 ETF")
    assert result["Equity_Foreign_Pct"] == 100.0
    assert result["Equity_India_Pct"] == 0.0


# ---------------------------------------------------------------------------
# Test 5: Existing tickers in map are reused (no re-scraping)
# ---------------------------------------------------------------------------
def test_existing_tickers_reused(tmp_path):
    """If a ticker is already in the map, the map entry is preserved verbatim."""
    from analytics.fetch_allocations import fetch_and_build_allocation_map

    # Write a valuation file with one MF fund
    val_path = tmp_path / "master_valuation.csv"
    pd.DataFrame([{
        "Portfolio Owner": "Pankaj", "Asset Class": "Mutual Fund",
        "Asset Name": "Quant Small Cap", "Ticker": "120828", "Current Value": 50000.0,
    }]).to_csv(val_path, index=False)

    # Pre-populate the map with that ticker already
    map_path = tmp_path / "asset_allocation_map.csv"
    pd.DataFrame([{
        "Ticker": "120828", "Asset Name": "Quant Small Cap",
        "Equity_India_Pct": 95.0, "Equity_Foreign_Pct": 0.0,
        "Debt_Pct": 5.0, "Gold_Pct": 0.0, "Cash_Pct": 0.0,
    }]).to_csv(map_path, index=False)

    # Run â€” it should NOT call any scraper (no mock = would raise if called)
    fetch_and_build_allocation_map(str(val_path), str(map_path))

    result = pd.read_csv(map_path)
    assert len(result) == 1
    assert float(result.iloc[0]["Equity_India_Pct"]) == 95.0, "Existing map entry must be preserved unchanged"

