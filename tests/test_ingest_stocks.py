"""
test_ingest_stocks.py â€” Tests for pipeline/ingest_stocks.py
============================================================
Tests stock order Excel parsing, ledger creation, and idempotent appending.
conftest.py ensures the project root is on sys.path.
"""
import pandas as pd
import pytest

from ingestion.ingest_stocks import ingest_stock_orders


def test_ingest_stock_orders(tmp_path):
    """Verifies that executed orders are parsed and written correctly."""
    input_dir = tmp_path / "input" / "stock"
    input_dir.mkdir(parents=True)
    master_ledger = tmp_path / "master_ledger.csv"

    mock_df = pd.DataFrame({
        'Stock name': ['Stock A', 'Stock B', 'Stock C'],
        'Symbol': ['TICKA', 'TICKB', 'TICKC'],
        'Type': ['Buy', 'Sell', 'Buy'],
        'Quantity': [10, 5, 20],
        'Value': [1000, 500, 2000],
        'Order status': ['Executed', 'Pending', 'EXECUTED'],
        'Execution date and time': ['25-02-2022 01:41 PM', '26-02-2022 10:00 AM', '01-03-2022 11:30 AM'],
    })

    data = [["Meta"] * len(mock_df.columns)] * 5
    data.append(mock_df.columns.tolist())
    data.extend(mock_df.values.tolist())
    pd.DataFrame(data).to_excel(input_dir / "TestUser_stocks.xlsx", index=False, header=False)

    ingest_stock_orders(str(input_dir), str(master_ledger))

    assert master_ledger.exists(), "Master ledger was not created"
    df_ledger = pd.read_csv(master_ledger)
    assert len(df_ledger) == 2, f"Expected 2 executed records, got {len(df_ledger)}"
    assert df_ledger.iloc[0]['Portfolio Owner'] == 'TestUser'
    assert df_ledger.iloc[0]['Asset Class'] == 'STOCK'
    assert df_ledger.iloc[0]['Asset Name'] == 'Stock A'
    assert df_ledger.iloc[0]['Ticker'] == 'TICKA'
    assert df_ledger.iloc[0]['Transaction Type'] == 'Buy'
    assert df_ledger.iloc[1]['Date'] == '2022-03-01'


def test_ingest_stock_orders_appending(tmp_path):
    """Verifies that re-running replaces STOCK rows but preserves other asset classes."""
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    master_ledger = tmp_path / "master_ledger.csv"

    existing_data = pd.DataFrame({
        'Portfolio Owner': ['OtherUser', 'TestUser'],
        'Asset Class': ['Mutual Fund', 'STOCK'],
        'Asset Name': ['MF A', 'Stock Z'],
        'Ticker': ['123', 'TICKZ'],
        'Transaction Type': ['Buy', 'Buy'],
        'Units': [10, 5],
        'Amount': [1000, 500],
        'Date': ['2021-01-01', '2021-01-02'],
    })
    existing_data.to_csv(master_ledger, index=False)

    mock_df = pd.DataFrame({
        'Stock name': ['Stock A'],
        'Symbol': ['TICKA'],
        'Type': ['Buy'],
        'Quantity': [10],
        'Value': [1000],
        'Order status': ['Executed'],
        'Execution date and time': ['25-02-2022 01:41 PM'],
    })
    data = [["Meta"] * len(mock_df.columns)] * 5
    data.append(mock_df.columns.tolist())
    data.extend(mock_df.values.tolist())
    pd.DataFrame(data).to_excel(input_dir / "TestUser_stocks.xlsx", index=False, header=False)

    ingest_stock_orders(str(input_dir), str(master_ledger))

    df_ledger = pd.read_csv(master_ledger)
    assert len(df_ledger) == 2, "Failed to appropriately replace STOCK rows and preserve MF rows"
    assert "Mutual Fund" in df_ledger['Asset Class'].values, "Accidentally wiped out Mutual Funds"

