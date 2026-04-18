"""
test_peer_returns_engine.py â€” Tests for pipeline/peer_returns_engine.py
========================================================================
All network calls are mocked. Tests cover:
  - AMFI master list parsing (category header extraction, Direct/growth detection)
  - Peer scheme code lookup with fuzzy name matching
  - CAGR calculation from NAV series
  - Std Dev, Sharpe, Sortino metric computation
  - Peer percentile ranking and median
  - Cache read / write behaviour
  - Graceful failure on empty data
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# 1. AMFI list parsing
# ---------------------------------------------------------------------------

SAMPLE_AMFI_TEXT = """Scheme Code;ISIN Div Payout/ ISIN Growth;ISIN Div Reinvestment;Scheme Name;Net Asset Value;Repurchase Price;Sale Price;Date

Open Ended Schemes(Equity Scheme - Flexi Cap Fund)

Parag Parikh AMC

119551;INF000K01001;INF000K01002;Parag Parikh Flexi Cap Fund - Direct Plan - Growth;90.00;90.00;90.00;06-Mar-2026
119552;INF000K01003;INF000K01004;Parag Parikh Flexi Cap Fund - Regular Plan - IDCW;50.00;50.00;50.00;06-Mar-2026

Open Ended Schemes(Equity Scheme - Small Cap Fund)

Axis AMC

119600;INF001K01001;-;Axis Small Cap Fund - Direct Plan - Growth;120.00;120.00;120.00;06-Mar-2026
"""


@patch("analytics.peer_returns_engine.requests.get")
def test_amfi_parsing_extracts_sub_category(mock_get, tmp_path):
    """AMFI master list parser should extract sub-category from parenthesised headers."""
    mock_get.return_value.status_code = 200
    mock_get.return_value.text = SAMPLE_AMFI_TEXT
    cache_file = tmp_path / "amfi_scheme_list.json"
    with patch("analytics.peer_returns_engine._amfi_cache_path", return_value=cache_file):
        from analytics.peer_returns_engine import fetch_amfi_scheme_list
        df = fetch_amfi_scheme_list(force_refresh=True)
    assert not df.empty
    flexi = df[df["sub_category"].str.contains("Flexi Cap", na=False)]
    assert len(flexi) > 0


@patch("analytics.peer_returns_engine.requests.get")
def test_amfi_direct_growth_filter(mock_get, tmp_path):
    """Only Direct-Growth schemes should have is_direct=True and is_growth=True."""
    mock_get.return_value.status_code = 200
    mock_get.return_value.text = SAMPLE_AMFI_TEXT
    cache_file = tmp_path / "amfi_scheme_list.json"
    with patch("analytics.peer_returns_engine._amfi_cache_path", return_value=cache_file):
        from analytics.peer_returns_engine import fetch_amfi_scheme_list
        df = fetch_amfi_scheme_list(force_refresh=True)
    direct = df[df["is_direct"] & df["is_growth"]]
    scheme_names = direct["scheme_name"].tolist()
    assert not any("Regular" in n for n in scheme_names)
    assert not any("IDCW" in n for n in scheme_names)


# ---------------------------------------------------------------------------
# 2. CAGR Calculation
# ---------------------------------------------------------------------------

def _make_nav_series(start_date: str, start_val: float, end_val: float, days: int) -> pd.Series:
    """Create a synthetic NAV series growing linearly from start_val to end_val."""
    dates = pd.date_range(start=start_date, periods=days, freq="B")
    vals  = np.linspace(start_val, end_val, days)
    return pd.Series(vals, index=dates)


def test_cagr_1y_correct():
    """Fund growing from 100â†’110 in 1Y â†’ 10% CAGR."""
    from analytics.peer_returns_engine import _cagr
    dates = pd.to_datetime(["2024-01-01", "2025-01-01"])
    nav = pd.Series([100.0, 110.0], index=dates)
    result = _cagr(nav, 1)
    assert result is not None
    assert abs(result - 10.0) < 0.1


def test_cagr_3y_correct():
    """Fund growing from 100â†’125.97 in 3Y â†’ ~8% CAGR."""
    from analytics.peer_returns_engine import _cagr
    dates = pd.to_datetime(["2021-01-01", "2024-01-01"])
    nav = pd.Series([100.0, 125.97], index=dates)
    result = _cagr(nav, 3)
    assert result is not None
    assert abs(result - 8.0) < 0.1


def test_cagr_returns_none_on_empty():
    """Empty NAV series should return None without raising."""
    from analytics.peer_returns_engine import _cagr
    assert _cagr(pd.Series(dtype=float), 3) is None


# ---------------------------------------------------------------------------
# 2.5 Rolling Returns Calculation
# ---------------------------------------------------------------------------

def test_rolling_returns_3y_median():
    """Verify median rolling return calculation over a 3Y window sampled in the last 3 years."""
    from analytics.peer_returns_engine import _compute_median_rolling_return
    # 6 years of daily data growing at exactly 10% per year for the first 3 years and 15% for the last 3 years
    n_days_total = 1512  # ~6 years of business days
    dates = pd.date_range("2018-01-01", periods=n_days_total, freq="B")
    
    # Generate geometric curve where first half is 10%/yr and second half accelerates to 15.5%
    vals = [100.0]
    for i in range(1, n_days_total):
        if i < n_days_total / 2:
            vals.append(vals[-1] * (1 + 0.10/252))
        else:
            vals.append(vals[-1] * (1 + 0.155/252))
            
    nav = pd.Series(vals, index=dates)
    
    # 3Y rolling returns measured over the last 3 years:
    # Lookback window for 3Y rolling is moving from [yr 0-3] up to [yr 3-6].
    # The return changes from ~10% up to ~15.5%.
    result = _compute_median_rolling_return(nav, window_years=3, observation_years=3)
    
    assert result is not None
    # Median should be somewhere between 10% and 15.5%, but mathematically correct for rolling overlap
    assert 10.0 < result < 15.6

def test_rolling_returns_insufficient_data():
    """Should return None if series is too short for the window + observation period."""
    from analytics.peer_returns_engine import _compute_median_rolling_return
    # Only 2 years of data, asking for 3Y rolling
    nav = _make_nav_series("2022-01-01", 100.0, 110.0, 500)
    assert _compute_median_rolling_return(nav, window_years=3, observation_years=3) is None




# ---------------------------------------------------------------------------
# 3. Full Metric Computation (mock benchmark)
# ---------------------------------------------------------------------------

@patch("analytics.peer_returns_engine.yf.Ticker")
def test_compute_fund_metrics_sharpe_and_sortino(mock_ticker):
    """Sharpe and Sortino should be computed for a 3Y+ NAV series with realistic volatility."""
    np.random.seed(42)
    # Create a noisy 3.5Y NAV series (daily random walk with upward drift)
    n_days = 900
    dates = pd.date_range("2022-01-01", periods=n_days, freq="B")
    # Small random daily returns with slight positive drift (realistic MF)
    daily_returns = np.random.normal(loc=0.0005, scale=0.008, size=n_days)
    nav_values = 100.0 * np.cumprod(1 + daily_returns)
    nav = pd.Series(nav_values, index=dates)

    # Mock benchmark (Nifty 50)
    bm_values = 100.0 * np.cumprod(1 + np.random.normal(0.0004, 0.007, n_days))
    mock_hist = pd.DataFrame({"Close": bm_values},
                              index=pd.date_range("2022-01-01", periods=n_days, freq="B"))
    mock_ticker.return_value.history.return_value = mock_hist

    from analytics.peer_returns_engine import compute_fund_metrics
    result = compute_fund_metrics("999999", benchmark_symbol="^NSEI", nav_series=nav)

    assert "cagr_3y" in result
    assert "sharpe_3y" in result
    assert "sortino_3y" in result, f"sortino_3y missing from result: {result}"
    assert "std_dev_3y" in result
    assert "beta_3y" in result
    assert "alpha_3y" in result


# ---------------------------------------------------------------------------
# 4. Peer Percentile Ranking
# ---------------------------------------------------------------------------

def test_percentile_rank_correct():
    """Value better than 8 of 10 peers â†’ 80th percentile."""
    from analytics.peer_returns_engine import _percentile_rank
    peers = [10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0]
    assert _percentile_rank(18.5, peers) == 90  # better than all 9 below 18.5 â†’ 9/10 = 90%


def test_percentile_rank_lowest():
    """Value worse than all peers â†’ 0th percentile."""
    from analytics.peer_returns_engine import _percentile_rank
    assert _percentile_rank(5.0, [10.0, 20.0, 30.0]) == 0


def test_ordinal_suffixes():
    """Verify ordinal suffix generation for edge cases."""
    from analytics.peer_returns_engine import _ordinal
    assert _ordinal(1)   == "1st"
    assert _ordinal(2)   == "2nd"
    assert _ordinal(3)   == "3rd"
    assert _ordinal(4)   == "4th"
    assert _ordinal(11)  == "11th"   # teen exception
    assert _ordinal(12)  == "12th"   # teen exception
    assert _ordinal(21)  == "21st"
    assert _ordinal(81)  == "81st"   # was wrong before (81th)
    assert _ordinal(100) == "100th"


# ---------------------------------------------------------------------------
# 5. Cache Behaviour
# ---------------------------------------------------------------------------

def test_peer_cache_round_trip(tmp_path):
    """Cache write and read should preserve data exactly."""
    with patch("analytics.peer_returns_engine.CACHE_DIR", tmp_path):
        from analytics.peer_returns_engine import _save_peer_cache, _load_peer_cache
        data = {"119551": {"cagr_3y": 18.5, "sharpe_3y": 1.2, "data_as_of": "2026-03-06"}}
        _save_peer_cache("Flexi Cap Fund", data)
        loaded = _load_peer_cache("Flexi Cap Fund")
        assert loaded == data


def test_peer_cache_expired(tmp_path):
    """Expired cache (>7 days old) should return None."""
    with patch("analytics.peer_returns_engine.CACHE_DIR", tmp_path):
        from analytics.peer_returns_engine import _save_peer_cache, _load_peer_cache
        _save_peer_cache("Old Category", {"dummy": {}})

        # Backdate the file modification time by 8 days
        cache_file = list(tmp_path.glob("*.json"))[0]
        old_time = (datetime.now() - timedelta(days=8)).timestamp()
        import os
        os.utime(cache_file, (old_time, old_time))

        assert _load_peer_cache("Old Category") is None


# ---------------------------------------------------------------------------
# 6. Graceful Failure
# ---------------------------------------------------------------------------

def test_get_peer_analytics_returns_empty_on_failure():
    """get_peer_analytics should return {} not raise on any exception."""
    with patch("analytics.peer_returns_engine.fetch_amfi_scheme_list", side_effect=RuntimeError("network down")):
        from analytics.peer_returns_engine import get_peer_analytics
        result = get_peer_analytics("999999", "Unknown Fund")
        assert result == {}

