"""
test_fetch_nps_navs.py â€” Tests for pipeline/fetch_nps_navs.py
==============================================================
"""
import json
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from valuation.fetch_nps_navs import fetch_live_nps_navs, HDFC_SCHEME_CODES


# ---------------------------------------------------------------------------
# Test 1: Successful API fetch writes correct NAV to JSON cache
# ---------------------------------------------------------------------------
@patch("valuation.fetch_nps_navs.requests.get")
def test_fetch_writes_cache(mock_get, tmp_path):
    """Verifies that a successful API response is saved to the JSON cache."""
    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        "NAV": "55.7639", "Last Updated": "27-02-2026",
        "Scheme Name": "HDFC SCHEME E - TIER I",
    }
    mock_resp.raise_for_status.return_value = None
    mock_get.return_value = mock_resp

    out = tmp_path / "nps_navs.json"
    navs = fetch_live_nps_navs(
        scheme_codes={"NPS - Scheme E (Equity)": "SM008001"},
        output_path=str(out),
    )

    assert out.exists(), "JSON cache should be written"
    result = navs["NPS - Scheme E (Equity)"]
    # fetch_live_nps_navs now returns {"nav": float, "date": str}
    nav_val = result["nav"] if isinstance(result, dict) else result
    assert nav_val == 55.7639

    with open(out) as f:
        cache = json.load(f)
    cached = cache["NPS - Scheme E (Equity)"]
    cached_val = cached["nav"] if isinstance(cached, dict) else cached
    assert cached_val == 55.7639


# ---------------------------------------------------------------------------
# Test 2: API failure falls back to existing cache value
# ---------------------------------------------------------------------------
@patch("valuation.fetch_nps_navs.requests.get")
def test_fetch_falls_back_to_cache_on_error(mock_get, tmp_path):
    """If the API call fails, the cached NAV is returned and preserved."""
    mock_get.side_effect = Exception("Network timeout")

    # Pre-seed the cache
    out = tmp_path / "nps_navs.json"
    out.write_text(json.dumps({"NPS - Scheme E (Equity)": 51.23}))

    navs = fetch_live_nps_navs(
        scheme_codes={"NPS - Scheme E (Equity)": "SM008001"},
        output_path=str(out),
    )

    result = navs["NPS - Scheme E (Equity)"]
    nav_val = result["nav"] if isinstance(result, dict) else result
    assert nav_val == 51.23, "Should use cached value on failure"


# ---------------------------------------------------------------------------
# Test 3: No cache + API failure â†’ 10.0 sentinel
# ---------------------------------------------------------------------------
@patch("valuation.fetch_nps_navs.requests.get")
def test_fetch_sentinel_when_no_cache_and_api_fails(mock_get, tmp_path):
    """If cache is absent AND API fails, returns 10.0 sentinel."""
    mock_get.side_effect = Exception("No internet")

    out = tmp_path / "nps_navs.json"
    navs = fetch_live_nps_navs(
        scheme_codes={"NPS - Scheme E (Equity)": "SM008001"},
        output_path=str(out),
    )

    result = navs["NPS - Scheme E (Equity)"]
    nav_val = result["nav"] if isinstance(result, dict) else result
    assert nav_val == 10.0


# ---------------------------------------------------------------------------
# Test 4: HDFC_SCHEME_CODES covers all 4 schemes used by the portfolio
# ---------------------------------------------------------------------------
def test_hdfc_scheme_codes_complete():
    """Verifies all 4 active schemes are in the map with correct codes."""
    assert HDFC_SCHEME_CODES["NPS - Scheme E (Equity)"]      == "SM008001"
    assert HDFC_SCHEME_CODES["NPS - Scheme C (Corp Debt)"]   == "SM008002"
    assert HDFC_SCHEME_CODES["NPS - Scheme G (Govt Debt)"]   == "SM008003"
    assert HDFC_SCHEME_CODES["NPS - Equity Advantage Fund"]  == "SM008013"
    # Scheme A (SM008008) must NOT be in the map â€” it was discontinued and merged into Scheme C
    assert "NPS - Scheme A (Alternative Inv)" not in HDFC_SCHEME_CODES

