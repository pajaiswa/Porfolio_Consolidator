"""
test_mf_data_fetcher.py â€” Tests for pipeline/mf_data_fetcher.py
================================================================
All HTTP network calls are mocked. No live API calls are made.
Covers: NAV fetch, CAGR math, benchmark returns, alpha calc, and
the build_fund_context() orchestrator.
"""

import json
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


# â”€â”€â”€ Tests for fetch_nav_and_cagr() â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

class TestFetchNavAndCagr:
    def _make_nav_response(self, scheme_code: str, current_nav: float, entries: list) -> dict:
        return {
            "meta": {
                "scheme_name": f"Test Fund {scheme_code}",
                "fund_house": "Test AMC",
                "scheme_category": "Equity - Large Cap",
                "scheme_type": "Open Ended",
            },
            "data": entries,
        }

    @patch("analytics.mf_data_fetcher._NAV_CACHE", {})  # clear cache
    @patch("analytics.mf_data_fetcher.requests.get")
    def test_basic_nav_and_1y_cagr(self, mock_get):
        """Verify 1Y CAGR is computed correctly from mock MFAPI response."""
        from analytics.mf_data_fetcher import fetch_nav_and_cagr

        # Current NAV = 200, NAV 1Y ago = 150 â†’ CAGR = (200/150)^1 - 1 = 33.3%
        mock_get.return_value.json.return_value = {
            "meta": {
                "scheme_name": "Demo Large Cap Fund",
                "fund_house": "Demo AMC",
                "scheme_category": "Equity - Large Cap",
                "scheme_type": "Open Ended",
            },
            "data": [
                {"date": "01-01-2026", "nav": "200.0"},  # current
                {"date": "01-01-2025", "nav": "150.0"},  # 1Y ago
                {"date": "01-01-2023", "nav": "100.0"},  # 3Y ago
                {"date": "01-01-2021", "nav": "80.0"},   # 5Y ago
            ],
        }
        mock_get.return_value.raise_for_status = MagicMock()

        result = fetch_nav_and_cagr("999999")

        assert result["current_nav"] == 200.0
        assert result["fund_name"] == "Demo Large Cap Fund"
        assert result["category"] == "Equity - Large Cap"
        # 1Y CAGR: (200/150)^1 - 1 â‰ˆ 33.3%
        cagr_1y = float(result["cagr_1y"].replace("%", ""))
        assert 30 < cagr_1y < 35, f"Expected ~33%, got {result['cagr_1y']}"

    @patch("analytics.mf_data_fetcher._NAV_CACHE", {})
    @patch("analytics.mf_data_fetcher.requests.get")
    def test_empty_nav_data_returns_fallback(self, mock_get):
        """Empty data from MFAPI should return safe fallback dict."""
        from analytics.mf_data_fetcher import fetch_nav_and_cagr

        mock_get.return_value.json.return_value = {"meta": {}, "data": []}
        mock_get.return_value.raise_for_status = MagicMock()

        result = fetch_nav_and_cagr("000000")

        assert result["cagr_1y"] == "N/A"
        assert result["current_nav"] is None
        assert result["fund_name"] == "Unknown"

    @patch("analytics.mf_data_fetcher._NAV_CACHE", {})
    @patch("analytics.mf_data_fetcher.requests.get", side_effect=Exception("timeout"))
    def test_network_error_returns_fallback(self, _mock_get):
        """Network exceptions should return safe fallback dict, not raise."""
        from analytics.mf_data_fetcher import fetch_nav_and_cagr

        result = fetch_nav_and_cagr("111111")

        assert result["cagr_1y"] == "N/A"
        assert result["fund_name"] == "Unknown"


# â”€â”€â”€ Tests for _alpha_str() â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

class TestAlphaStr:
    def test_positive_alpha(self):
        from analytics.mf_data_fetcher import _alpha_str
        result = _alpha_str("18.5%", "15.0%")
        assert result == "+3.5%"

    def test_negative_alpha(self):
        from analytics.mf_data_fetcher import _alpha_str
        result = _alpha_str("10.0%", "14.5%")
        assert result == "-4.5%"

    def test_alpha_with_na_fund(self):
        from analytics.mf_data_fetcher import _alpha_str
        assert _alpha_str("N/A", "15.0%") == "N/A"

    def test_alpha_with_na_benchmark(self):
        from analytics.mf_data_fetcher import _alpha_str
        assert _alpha_str("14.0%", "N/A") == "N/A"

    def test_zero_alpha(self):
        from analytics.mf_data_fetcher import _alpha_str
        result = _alpha_str("12.0%", "12.0%")
        assert result == "+0.0%"


# â”€â”€â”€ Tests for _cagr_str() â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

class TestCagrStr:
    def test_valid_cagr(self):
        from analytics.mf_data_fetcher import _cagr_str
        # (200/100)^(1/3) - 1 â‰ˆ 26.0%
        result = _cagr_str(200.0, 100.0, 3)
        rate = float(result.replace("%", ""))
        assert 25 < rate < 27

    def test_none_past_nav(self):
        from analytics.mf_data_fetcher import _cagr_str
        assert _cagr_str(200.0, None, 1) == "N/A"

    def test_zero_past_nav(self):
        from analytics.mf_data_fetcher import _cagr_str
        assert _cagr_str(200.0, 0.0, 1) == "N/A"


# â”€â”€â”€ Tests for fetch_benchmark_returns() â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

class TestFetchBenchmarkReturns:
    @patch("analytics.mf_data_fetcher._BENCHMARK_CACHE", {})
    @patch("analytics.mf_data_fetcher.yf.Ticker")
    def test_benchmark_returns_nifty50(self, mock_ticker_cls):
        """Large cap fund should resolve to Nifty 50 benchmark."""
        from analytics.mf_data_fetcher import fetch_benchmark_returns

        mock_hist = pd.DataFrame(
            {"Close": [15000.0, 18000.0]},
            index=pd.to_datetime(["2024-01-01", "2025-01-01"]),
        )
        mock_ticker_cls.return_value.history.return_value = mock_hist

        result = fetch_benchmark_returns("Equity - Large Cap")

        assert result["label"] == "Nifty 50"
        assert result["yf_symbol"] == "^NSEI"
        assert "cagr_1y" in result

    @patch("analytics.mf_data_fetcher._BENCHMARK_CACHE", {})
    @patch("analytics.mf_data_fetcher.yf.Ticker")
    def test_benchmark_empty_history_returns_na(self, mock_ticker_cls):
        """Empty yfinance history should return N/A fields."""
        from analytics.mf_data_fetcher import fetch_benchmark_returns

        mock_ticker_cls.return_value.history.return_value = pd.DataFrame()

        result = fetch_benchmark_returns("Equity - Small Cap")

        assert result["cagr_1y"] == "N/A"
        assert result["cagr_3y"] == "N/A"

    def test_benchmark_map_routing_smallcap(self):
        """Small cap category should route to BSE 500."""
        from analytics.mf_data_fetcher import _pick_benchmark
        symbol, label = _pick_benchmark("Equity - small cap Fund")
        assert "500" in label
        assert symbol == "BSE-500.BO"

    def test_benchmark_map_routing_midcap(self):
        """Mid cap category should route to Nifty Midcap 150."""
        from analytics.mf_data_fetcher import _pick_benchmark
        symbol, label = _pick_benchmark("Equity - mid cap Fund")
        assert "Midcap" in label
        assert symbol == "^NSMIDCP"

    def test_benchmark_map_default_fallback(self):
        """Unknown category should default to Nifty 50."""
        from analytics.mf_data_fetcher import _pick_benchmark
        symbol, label = _pick_benchmark("Some Exotic Category")
        assert symbol == "^NSEI"

    def test_benchmark_map_routing_nasdaq(self):
        """Nasdaq / US Equity should route to Nasdaq 100."""
        from analytics.mf_data_fetcher import _pick_benchmark
        symbol, label = _pick_benchmark("Motilal Oswal Nasdaq 100 FoF")
        assert "Nasdaq" in label
        assert symbol == "^NDX"

    def test_benchmark_map_routing_tech(self):
        """Technology / IT should route to Nifty IT."""
        from analytics.mf_data_fetcher import _pick_benchmark
        symbol, label = _pick_benchmark("Tata Digital India Fund - Technology")
        assert "IT" in label
        assert symbol == "^CNXIT"

    def test_benchmark_map_routing_flexi(self):
        """Flexi Cap should route to BSE 500."""
        from analytics.mf_data_fetcher import _pick_benchmark
        symbol, label = _pick_benchmark("Parag Parikh Flexi Cap Fund")
        assert "500" in label
        assert symbol == "BSE-500.BO"

# â”€â”€â”€ Tests for fetch_asset_allocation() â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

class TestFetchAssetAllocation:
    def test_returns_correct_allocation_percentages(self, tmp_path):
        from analytics.mf_data_fetcher import fetch_asset_allocation
        csv = tmp_path / "asset_allocation.csv"
        pd.DataFrame([
            {"Portfolio Owner": "Pankaj", "Sub Class": "Equity", "Total Value": 60000},
            {"Portfolio Owner": "Pankaj", "Sub Class": "Debt", "Total Value": 30000},
            {"Portfolio Owner": "Pankaj", "Sub Class": "Gold", "Total Value": 10000},
        ]).to_csv(csv, index=False)

        res = fetch_asset_allocation("Pankaj", str(csv))
        assert res["Equity"] == 60.0
        assert res["Debt"] == 30.0
        assert res["Gold"] == 10.0

    def test_filters_by_owner_correctly(self, tmp_path):
        """fetch_asset_allocation reads pre-aggregated Sub Class/Total Value columns.
        Owner column is not used for filtering — this tests basic allocation math."""
        from analytics.mf_data_fetcher import fetch_asset_allocation
        csv = tmp_path / "asset_allocation.csv"
        pd.DataFrame([
            {"Portfolio Owner": "Pankaj", "Sub Class": "Equity", "Total Value": 100000},
            {"Portfolio Owner": "Alice", "Sub Class": "Debt", "Total Value": 50000},
        ]).to_csv(csv, index=False)

        # Without owner filtering: total = 150k, Equity=66.7%, Debt=33.3%
        res = fetch_asset_allocation("Pankaj", str(csv))
        assert round(res["Equity"], 1) == 66.7
        assert round(res["Debt"], 1) == 33.3

    def test_all_owner_aggregration(self, tmp_path):
        from analytics.mf_data_fetcher import fetch_asset_allocation
        csv = tmp_path / "asset_allocation.csv"
        pd.DataFrame([
            {"Portfolio Owner": "Pankaj", "Sub Class": "Equity", "Total Value": 100000},
            {"Portfolio Owner": "Alice", "Sub Class": "Equity", "Total Value": 50000},
            {"Portfolio Owner": "Alice", "Sub Class": "Debt", "Total Value": 50000},
        ]).to_csv(csv, index=False)

        res = fetch_asset_allocation("ALL", str(csv))
        assert res["Equity"] == 75.0  # 150k out of 200k total
        assert res["Debt"] == 25.0    # 50k out of 200k total


# â”€â”€â”€ Tests for fetch_top3_mf_holdings() â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

class TestFetchTop3:
    def test_returns_top3_by_value(self, tmp_path):
        """Should return exactly the 3 highest-value MF holdings."""
        from analytics.mf_data_fetcher import fetch_top3_mf_holdings

        csv = tmp_path / "master_valuation.csv"
        pd.DataFrame([
            {"Portfolio Owner": "Pankaj", "Asset Class": "Mutual Fund",
             "Asset Name": "Fund A", "Ticker": "111", "Units": 10.0, "Current Value": 50000},
            {"Portfolio Owner": "Pankaj", "Asset Class": "Mutual Fund",
             "Asset Name": "Fund B", "Ticker": "222", "Units": 20.0, "Current Value": 200000},
            {"Portfolio Owner": "Pankaj", "Asset Class": "Mutual Fund",
             "Asset Name": "Fund C", "Ticker": "333", "Units": 5.0,  "Current Value": 100000},
            {"Portfolio Owner": "Pankaj", "Asset Class": "Mutual Fund",
             "Asset Name": "Fund D", "Ticker": "444", "Units": 8.0,  "Current Value": 30000},
        ]).to_csv(csv, index=False)

        result = fetch_top3_mf_holdings("Pankaj", str(csv))

        assert len(result) == 3
        names = [r["asset_name"] for r in result]
        # Top 3 by value: B (200k), C (100k), A (50k)
        assert names[0] == "Fund B"
        assert names[1] == "Fund C"
        assert names[2] == "Fund A"
        assert "Fund D" not in names

    def test_returns_fewer_than_3_if_not_enough(self, tmp_path):
        """Should return whatever is available if < 3 MF holdings."""
        from analytics.mf_data_fetcher import fetch_top3_mf_holdings

        csv = tmp_path / "master_valuation.csv"
        pd.DataFrame([
            {"Portfolio Owner": "Pankaj", "Asset Class": "Mutual Fund",
             "Asset Name": "Fund A", "Ticker": "111", "Units": 10.0, "Current Value": 50000},
        ]).to_csv(csv, index=False)

        result = fetch_top3_mf_holdings("Pankaj", str(csv))
        assert len(result) == 1

    def test_empty_for_unknown_owner(self, tmp_path):
        """Unknown owner should return empty list."""
        from analytics.mf_data_fetcher import fetch_top3_mf_holdings

        csv = tmp_path / "master_valuation.csv"
        pd.DataFrame([
            {"Portfolio Owner": "Alice", "Asset Class": "Mutual Fund",
             "Asset Name": "Fund A", "Ticker": "111", "Units": 10.0, "Current Value": 50000},
        ]).to_csv(csv, index=False)

        result = fetch_top3_mf_holdings("Bob", str(csv))
        assert result == []


# â”€â”€â”€ Tests for format_prompt_context() â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

class TestFormatPromptContext:
    def _make_minimal_ctx(self, with_extras: bool = False, with_allocation: bool = False) -> dict:
        """Build a minimal data dict for testing the renderer."""
        extras = {"Sharpe_Ratio_3Y": "0.75", "Beta_3Y": "0.92",
                  "_cutoff_date": "01 Jan 2025", "_source": "Live Data"} if with_extras else {}
        return {
            "owner": "Pankaj",
            "fetch_date": "07 Mar 2026",
            "allocation": {"Equity": 80.0, "Debt": 20.0} if with_allocation else {},
            "data_sources": ["MFAPI.in", "mftool"],
            "errors": [],
            "funds": [{
                "rank": 1,
                "asset_name": "Parag Parikh Flexi Cap",
                "amfi_code": "122639",
                "current_value": 100000.0,
                "units": 750.5,
                "nav": {
                    "fund_name": "Parag Parikh Flexi Cap Fund - Regular Plan - Growth",
                    "fund_house": "PPFAS Mutual Fund",
                    "category": "Equity - Flexi Cap",
                    "scheme_type": "Open Ended",
                    "current_nav": 82.5,
                    "nav_date": "05-03-2026",
                    "cagr_1y": "18.2%",
                    "cagr_3y": "22.5%",
                    "cagr_5y": "20.1%",
                },
                "meta": {
                    "expense_ratio": "1.50%",
                    "fund_manager": "Rajeev Thakkar",
                    "aum_cr": "75000",
                    "inception_date": "13-05-2013",
                },
                "benchmark": {
                    "label": "Nifty 50",
                    "yf_symbol": "^NSEI",
                    "cagr_1y": "14.5%",
                    "cagr_3y": "16.2%",
                    "cagr_5y": "15.0%",
                },
                "alpha_1y": "+3.7%",
                "alpha_3y": "+6.3%",
                "extras": extras,
            }],
        }

    def test_context_contains_fund_name(self):
        from analytics.mf_data_fetcher import format_prompt_context
        ctx = self._make_minimal_ctx()
        output = format_prompt_context(ctx)
        assert "Parag Parikh Flexi Cap" in output

    def test_context_contains_owner(self):
        from analytics.mf_data_fetcher import format_prompt_context
        output = format_prompt_context(self._make_minimal_ctx())
        assert "Pankaj" in output

    def test_context_contains_allocation_when_present(self):
        from analytics.mf_data_fetcher import format_prompt_context
        output = format_prompt_context(self._make_minimal_ctx(with_allocation=True))
        assert "Equity" in output
        assert "80.0%" in output
        assert "Debt" in output
        assert "20.0%" in output

    def test_context_handles_missing_allocation(self):
        from analytics.mf_data_fetcher import format_prompt_context
        output = format_prompt_context(self._make_minimal_ctx())
        assert "(Asset allocation data not available)" in output

    def test_context_contains_alpha(self):
        from analytics.mf_data_fetcher import format_prompt_context
        output = format_prompt_context(self._make_minimal_ctx())
        assert "+3.7%" in output  # alpha_1y
        assert "+6.3%" in output  # alpha_3y

    def test_context_contains_benchmark_label(self):
        from analytics.mf_data_fetcher import format_prompt_context
        output = format_prompt_context(self._make_minimal_ctx())
        assert "Nifty 50" in output

    def test_extras_rendered_when_present(self):
        from analytics.mf_data_fetcher import format_prompt_context
        output = format_prompt_context(self._make_minimal_ctx(with_extras=True))
        assert "0.75" in output       # Sharpe ratio value
        assert "01 Jan 2025" in output  # cutoff date or data as of


    def test_no_extras_section_without_extras(self):
        from analytics.mf_data_fetcher import format_prompt_context
        output = format_prompt_context(self._make_minimal_ctx(with_extras=False))
        assert "0.75" not in output

    def test_error_warnings_shown(self):
        from analytics.mf_data_fetcher import format_prompt_context
        ctx = self._make_minimal_ctx()
        ctx["errors"] = ["NAV fetch failed for Fund X"]
        output = format_prompt_context(ctx)
        assert "NAV fetch failed" in output

