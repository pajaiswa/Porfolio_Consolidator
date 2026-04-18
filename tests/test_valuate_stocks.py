"""
test_valuate_stocks.py â€” Tests for pipeline/valuate_stocks.py
==============================================================
Tests live stock price fetching math using mocked yfinance/nsepython responses.
conftest.py ensures the project root is on sys.path.
"""
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock

from valuation.valuate_stocks import fetch_live_stock_valuations


@patch('valuation.valuate_stocks.nse_eq')
@patch('valuation.valuate_stocks.yf.Ticker')
def test_fetch_live_stock_valuations(mock_yf_ticker, mock_nse_eq):
    """Verifies that live stock prices are correctly multiplied by units."""
    mock_ledger = pd.DataFrame({
        'Portfolio Owner': ['TestUser', 'TestUser'],
        'Asset Class': ['STOCK', 'STOCK'],
        'Asset Name': ['Stock A', 'Gold Bond'],
        'Ticker': ['RELIANCE', 'SGBDEC20'],
        'Transaction Type': ['BUY', 'BUY'],
        'Units': [10.0, 5.0],
    })

    mock_stock_instance = MagicMock()
    mock_stock_instance.fast_info.last_price = 2500.0

    def yf_side_effect(ticker):
        if "SGB" in ticker:
            raise ValueError("No data found")
        return mock_stock_instance

    mock_yf_ticker.side_effect = yf_side_effect
    mock_nse_eq.return_value = {'priceInfo': {'lastPrice': '5000.00'}}

    df_val, totals = fetch_live_stock_valuations(mock_ledger)

    assert len(df_val) == 2, "Failed to fetch both stocks"

    reliance_val = df_val[df_val['Ticker'] == 'RELIANCE']['Current Value'].iloc[0]
    sgb_val = df_val[df_val['Ticker'] == 'SGBDEC20']['Current Value'].iloc[0]

    assert reliance_val == 25000.0, f"Math failed! Expected 25000, got {reliance_val}"
    assert sgb_val == 25000.0, f"Math failed! Expected 25000, got {sgb_val}"
    assert totals['TestUser'] == 50000.0

