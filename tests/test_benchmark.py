"""
test_benchmark.py â€” Tests for pipeline/benchmark.py
====================================================
"""
from pathlib import Path
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock

from analytics.benchmark import fetch_nifty50_history


# ---------------------------------------------------------------------------
# Test 1: Normal flow â€” CSV written with correct columns
# ---------------------------------------------------------------------------
@patch("analytics.benchmark.yf.Ticker")
def test_benchmark_saves_csv(mock_yf_ticker, tmp_path):
    """Mocks yfinance and verifies the output CSV is written with correct columns."""
    import pandas as pd
    from datetime import date, timedelta

    mock_hist = pd.DataFrame({
        "Date": pd.date_range("2024-01-01", periods=5, freq="D", tz="UTC"),
        "Close": [21000.0, 21100.0, 21050.0, 21200.0, 21300.0],
    })
    mock_instance = MagicMock()
    mock_instance.history.return_value = mock_hist
    mock_yf_ticker.return_value = mock_instance

    out_file = str(tmp_path / "nifty50_history.csv")
    fetch_nifty50_history(out_file)

    assert Path(out_file).exists(), "Output CSV should be created"
    df = pd.read_csv(out_file)
    assert "Date" in df.columns
    assert "Nifty_Close" in df.columns
    assert len(df) == 5


# ---------------------------------------------------------------------------
# Test 2: Empty response â€” no file created, no crash
# ---------------------------------------------------------------------------
@patch("analytics.benchmark.yf.Ticker")
def test_benchmark_handles_empty_response(mock_yf_ticker, tmp_path):
    mock_instance = MagicMock()
    mock_instance.history.return_value = pd.DataFrame()
    mock_yf_ticker.return_value = mock_instance

    out_file = str(tmp_path / "nifty50_history.csv")
    fetch_nifty50_history(out_file)

    assert not Path(out_file).exists(), "No CSV should be written when yfinance returns empty DataFrame"


# ---------------------------------------------------------------------------
# Test 3: Exception â€” exits gracefully
# ---------------------------------------------------------------------------
@patch("analytics.benchmark.yf.Ticker")
def test_benchmark_handles_exception(mock_yf_ticker, tmp_path):
    mock_yf_ticker.side_effect = Exception("Network timeout")

    out_file = str(tmp_path / "nifty50_history.csv")
    # Should not raise â€” just print error and return
    fetch_nifty50_history(out_file)

    assert not Path(out_file).exists(), "No CSV should be written on exception"

