"""
test_ingest_nps.py â€” Tests for pipeline/ingest_nps.py
======================================================
Tests the helper functions and business logic without requiring a real PDF.
"""
import json
from pathlib import Path
import pandas as pd
import pytest

# Import the internal helper directly
from ingestion.ingest_nps import _parse_number, get_last_nps_nav


# ---------------------------------------------------------------------------
# Test 1: _parse_number with commas
# ---------------------------------------------------------------------------
def test_parse_number_comma_formatted():
    assert _parse_number("1,234.56") == 1234.56
    assert _parse_number("10,00,000.00") == 1000000.0


# ---------------------------------------------------------------------------
# Test 2: _parse_number with negative bracket notation
# ---------------------------------------------------------------------------
def test_parse_number_negative_bracket():
    assert _parse_number("(1,234.56)") == -1234.56
    assert _parse_number("(500.00)") == -500.0


# ---------------------------------------------------------------------------
# Test 3: _parse_number plain integer
# ---------------------------------------------------------------------------
def test_parse_number_plain():
    assert _parse_number("12345") == 12345.0
    assert _parse_number("  9.90  ") == 9.9


# ---------------------------------------------------------------------------
# Test 4: get_last_nps_nav returns sentinel when cache absent
# ---------------------------------------------------------------------------
def test_nav_cache_missing_returns_sentinel(tmp_path):
    nav = get_last_nps_nav("NPS - Scheme E", str(tmp_path / "nonexistent.json"))
    assert nav == 10.0, "Should return 10.0 sentinel when cache file is absent"


# ---------------------------------------------------------------------------
# Test 5: get_last_nps_nav reads correct value from cache
# ---------------------------------------------------------------------------
def test_nav_cache_returns_correct_value(tmp_path):
    cache = tmp_path / "nps_latest_navs.json"
    cache.write_text(json.dumps({"NPS - Scheme E (Equity)": 42.5, "NPS - Scheme C (Corp Debt)": 18.3}))
    nav = get_last_nps_nav("NPS - Scheme E (Equity)", str(cache))
    assert nav == 42.5


# ---------------------------------------------------------------------------
# Test 6: get_last_nps_nav returns sentinel for unknown scheme key
# ---------------------------------------------------------------------------
def test_nav_cache_unknown_scheme_returns_sentinel(tmp_path):
    cache = tmp_path / "nps_latest_navs.json"
    cache.write_text(json.dumps({"NPS - Scheme E (Equity)": 42.5}))
    nav = get_last_nps_nav("NPS - Scheme G (Govt Debt)", str(cache))
    assert nav == 10.0, "Unknown scheme key should return 10.0 sentinel"

