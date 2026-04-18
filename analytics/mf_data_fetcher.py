"""
mf_data_fetcher.py — Consolidated MF Data Fetch Layer
=======================================================
Fetches ALL required Mutual Fund data in pure Python, with zero LLM calls.
Results are passed as a structured dict into the CrewAI advisor — agents
receive pre-computed context rather than making tool calls.

Data sources (all free, no paid API key needed):
  1. master_valuation.csv  — local portfolio data (top-3 MF by current value)
  2. MFAPI.in              — real-time NAV + 1Y/3Y/5Y CAGR
  3. mftool                — scheme metadata (expense ratio, AUM, fund type)
  4. yfinance              — benchmark index returns (Nifty 50, Midcap, Smallcap)

Design:
  - All fetch functions are self-contained and return safe fallback dicts on error.
  - _NAV_CACHE and _BENCHMARK_CACHE avoid duplicate network calls in one run.
  - build_fund_context() is the single public entry-point for the advisor.
  - format_prompt_context() renders the dict into a clean human-readable string
    for maximum LLM readability (table-like, not JSON).

Scalability hooks:
  - BENCHMARK_MAP is a list of tuples — append new category→index mappings.
  - Fund context dicts use an 'extras' key for future qualitative / risk metrics
    without breaking existing prompts.
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import requests
import yfinance as yf
from mftool import Mftool

logger = logging.getLogger(__name__)

# ─── In-memory caches (reset each Python process) ────────────────────────────
_NAV_CACHE: dict[str, dict] = {}
_META_CACHE: dict[str, dict] = {}
_BENCHMARK_CACHE: dict[str, dict] = {}

# ─── Benchmark routing map ────────────────────────────────────────────────────
# Format: ([category keywords], (yahoo_symbol, human_label))
# Lower-priority entries should go LAST (first match wins).
BENCHMARK_MAP: list[tuple[list[str], tuple[str, str]]] = [
    (["nasdaq", "us equity", "us tech"],          ("^NDX",      "Nasdaq 100")),
    (["s&p", "international", "global", "world"], ("^GSPC",     "S&P 500")),
    (["technology", "tech", "it"],                ("^CNXIT",    "Nifty IT")),
    (["pharma", "healthcare"],                    ("^CNXPHARMA","Nifty Pharma")),
    (["bank", "financial"],                       ("^NSEBANK",  "Nifty Bank")),
    (["small cap", "smallcap", "small-cap"],      ("BSE-500.BO", "BSE 500")),
    (["mid cap", "midcap", "mid-cap"],            ("^NSMIDCP",  "Nifty Midcap 150")),
    (["large & mid", "large and mid"],            ("^NSMIDCP",  "Nifty Midcap 150")),
    (["flexi", "multi cap", "multi-cap",
      "elss", "tax sav", "focused"],              ("BSE-500.BO","BSE 500")),
    (["large cap", "large-cap", "bluechip",
      "index"],                                   ("^NSEI",     "Nifty 50")),
    (["debt", "liquid", "overnight",
      "ultra short", "short dur", "credit",
      "banking and psu", "gilt", "money market"], ("BSE-500.BO","BSE 500")),
    (["hybrid", "balanced", "arbitrage"],         ("^NSEI",     "Nifty 50")),
    (["gold", "commodity"],                       ("GOLDBEES.NS","Gold ETF (GOLDBEES)")),
]

# ─── Internal helper functions ────────────────────────────────────────────────

def _pick_benchmark(category: str) -> tuple[str, str]:
    """Return (yahoo_symbol, human_label) for a given fund category string.
    Uses word-boundary matching to avoid false positives (e.g. 'it' in 'equity').
    """
    import re
    cat = category.lower()
    for keywords, benchmark in BENCHMARK_MAP:
        for kw in keywords:
            # Use word-boundary matching for short/ambiguous keywords to avoid
            # substring false positives (e.g. 'it' inside 'equity').
            if len(kw) <= 4:
                pattern = r"\b" + re.escape(kw) + r"\b"
                if re.search(pattern, cat):
                    return benchmark
            else:
                if kw in cat:
                    return benchmark
    return ("^NSEI", "Nifty 50")  # universal fallback


def _cagr_str(current: float, past: float | None, years: float) -> str:
    """Compute CAGR as a formatted percentage string, or 'N/A'."""
    if past is None or past <= 0:
        return "N/A"
    try:
        rate = ((current / past) ** (1 / years) - 1) * 100
        return f"{rate:.1f}%"
    except Exception:
        return "N/A"


def _alpha_str(fund_cagr: str, bm_cagr: str) -> str:
    """Return fund alpha vs benchmark, e.g. '+3.2%' or 'N/A'."""
    try:
        f = float(fund_cagr.replace("%", ""))
        b = float(bm_cagr.replace("%", ""))
        diff = f - b
        sign = "+" if diff >= 0 else ""
        return f"{sign}{diff:.1f}%"
    except Exception:
        return "N/A"

def _absolute_ret_str(current: float, past: float | None) -> str:
    """Compute absolute return as a formatted percentage string, or 'N/A'."""
    if past is None or past <= 0:
        return "N/A"
    try:
        rate = ((current / past) - 1) * 100
        return f"{rate:.1f}%"
    except Exception:
        return "N/A"


# ─── Public fetch functions ───────────────────────────────────────────────────

def fetch_top3_mf_holdings(owner_name: str, valuation_csv: str = "data/output/master_valuation.csv") -> list[dict]:
    """
    Read master_valuation.csv and return the top 3 Mutual Fund holdings
    by Current Value for the specified owner.

    Returns:
        List of up to 3 dicts: {asset_name, amfi_code, current_value, units}
    """
    try:
        df = pd.read_csv(valuation_csv)
        
        # Base filter: only looking at Mutual Funds
        mf_df = df[df["Asset Class"] == "Mutual Fund"].copy()
        
        if owner_name.upper() != "ALL":
            mf_df = mf_df[mf_df["Portfolio Owner"] == owner_name]

        if mf_df.empty:
            logger.warning("No MF holdings found for owner: %s", owner_name)
            return []

        mf_df["Current Value"] = pd.to_numeric(mf_df["Current Value"], errors="coerce").fillna(0)
        mf_df["Units"] = pd.to_numeric(mf_df["Units"], errors="coerce").fillna(0)
        
        # Aggregate across owners if "ALL" is selected, otherwise just normal grouping
        # Group by Ticker and Asset Name to merge split investments in the same fund
        grouped = mf_df.groupby(["Ticker", "Asset Name"], as_index=False).agg({
            "Units": "sum",
            "Current Value": "sum"
        })

        top3 = grouped.nlargest(3, "Current Value").to_dict("records")

        logger.info("Top 3 MF for %s: %s", owner_name, [r["Asset Name"] for r in top3])
        return [
            {
                "asset_name": r["Asset Name"],
                "amfi_code": str(r["Ticker"]).strip(),
                "current_value": round(float(r["Current Value"]), 2),
                "units": round(float(r["Units"]), 3),
            }
            for r in top3
        ]
    except Exception as exc:
        logger.error("fetch_top3_mf_holdings failed: %s", exc)
        return []

def fetch_asset_allocation(owner_name: str, allocation_csv: str = "data/output/asset_allocation.csv") -> dict[str, float]:
    """
    Fetch the overall portfolio asset allocation (e.g. Equity vs Debt vs Gold).
    Returns a mapping of asset class to percentage (0.0 to 100.0).
    """
    try:
        df = pd.read_csv(allocation_csv)

        # The CSV has columns: 'Sub Class', 'Total Value'
        if df.empty or "Total Value" not in df.columns or "Sub Class" not in df.columns:
            return {}

        df["Total Value"] = pd.to_numeric(df["Total Value"], errors="coerce").fillna(0)
        total_val = df["Total Value"].sum()
        if total_val <= 0:
            return {}

        # Group by the Sub Class
        grouped = df.groupby("Sub Class")["Total Value"].sum()
        alloc = (grouped / total_val) * 100.0
        
        return {str(k): round(float(v), 1) for k, v in alloc.items()}
    except Exception as exc:
        logger.warning("fetch_asset_allocation failed: %s", exc)
        return {}


def fetch_nav_and_cagr(scheme_code: str) -> dict[str, Any]:
    """
    Fetch NAV history from MFAPI.in and compute 1Y/3Y/5Y CAGR.

    Uses _NAV_CACHE to avoid duplicate calls within a single run.

    Returns:
        Dict with: fund_name, fund_house, category, scheme_type,
                   current_nav, nav_date, cagr_1y, cagr_3y, cagr_5y
        On failure, returns a safe dict with all fields set to 'Unknown'/'N/A'.
    """
    if scheme_code in _NAV_CACHE:
        return _NAV_CACHE[scheme_code]

    fallback = {
        "fund_name": "Unknown", "fund_house": "Unknown",
        "category": "Unknown", "scheme_type": "Unknown",
        "current_nav": None, "nav_date": "N/A",
        "ret_1m": "N/A", "ret_3m": "N/A", "ret_6m": "N/A",
        "cagr_1y": "N/A", "cagr_3y": "N/A", "cagr_5y": "N/A",
    }

    try:
        url = f"https://api.mfapi.in/mf/{scheme_code}"
        resp = requests.get(url, timeout=12)
        resp.raise_for_status()
        data = resp.json()

        meta = data.get("meta", {})
        nav_entries = data.get("data", [])  # newest-first list of {date, nav}

        if not nav_entries:
            logger.warning("MFAPI: no NAV entries for scheme %s", scheme_code)
            _NAV_CACHE[scheme_code] = fallback
            return fallback

        current_nav = float(nav_entries[0]["nav"])
        nav_date = nav_entries[0].get("date", "N/A")

        def _nav_n_days_ago(days: int) -> float | None:
            target = datetime.now() - timedelta(days=days)
            for entry in nav_entries:
                try:
                    d = datetime.strptime(entry["date"], "%d-%m-%Y")
                    if d <= target:
                        return float(entry["nav"])
                except Exception:
                    continue
            return None

        result = {
            "fund_name": meta.get("scheme_name", "Unknown"),
            "fund_house": meta.get("fund_house", "Unknown"),
            "category": meta.get("scheme_category", "Unknown"),
            "scheme_type": meta.get("scheme_type", "Unknown"),
            "current_nav": current_nav,
            "nav_date": nav_date,
            "ret_1m": _absolute_ret_str(current_nav, _nav_n_days_ago(30)),
            "ret_3m": _absolute_ret_str(current_nav, _nav_n_days_ago(91)),
            "ret_6m": _absolute_ret_str(current_nav, _nav_n_days_ago(182)),
            "cagr_1y": _cagr_str(current_nav, _nav_n_days_ago(365), 1),
            "cagr_3y": _cagr_str(current_nav, _nav_n_days_ago(1095), 3),
            "cagr_5y": _cagr_str(current_nav, _nav_n_days_ago(1825), 5),
        }
        _NAV_CACHE[scheme_code] = result
        return result

    except Exception as exc:
        logger.warning("fetch_nav_and_cagr failed for %s: %s", scheme_code, exc)
        _NAV_CACHE[scheme_code] = fallback
        return fallback


def fetch_scheme_metadata(scheme_code: str) -> dict[str, Any]:
    """
    Fetch scheme metadata from mftool (AMFI data):
      - Expense ratio, AUM, fund manager, scheme inception date.

    Uses _META_CACHE. Falls back gracefully if mftool is unavailable.

    Returns:
        Dict with: expense_ratio, fund_manager, aum_cr, inception_date
    """
    if scheme_code in _META_CACHE:
        return _META_CACHE[scheme_code]

    fallback = {
        "expense_ratio": "N/A",
        "fund_manager": "N/A",
        "aum_cr": "N/A",
        "inception_date": "N/A",
    }

    try:
        mf = Mftool()
        details = mf.get_scheme_details(scheme_code)
        if not details:
            _META_CACHE[scheme_code] = fallback
            return fallback

        result = {
            "expense_ratio": details.get("expense_ratio", "N/A"),
            "fund_manager": details.get("scheme_manager", "N/A") or "N/A",
            "aum_cr": details.get("aum", "N/A"),
            "inception_date": details.get("inception_date", "N/A") or details.get("scheme_start_date", {}).get("date", "N/A"),
        }
        _META_CACHE[scheme_code] = result
        return result

    except Exception as exc:
        logger.warning("fetch_scheme_metadata failed for %s: %s", scheme_code, exc)
        _META_CACHE[scheme_code] = fallback
        return fallback


def fetch_benchmark_returns(category: str) -> dict[str, Any]:
    """
    Fetch benchmark CAGR from yfinance for a given fund category string.

    The category is matched against BENCHMARK_MAP to select the correct
    index (e.g. Nifty 50, Midcap 150, Smallcap 250, Gold ETF).

    Uses _BENCHMARK_CACHE (keyed by the resolved yahoo symbol).

    Returns:
        Dict with: label, yf_symbol, cagr_1y, cagr_3y, cagr_5y
    """
    yf_symbol, bm_label = _pick_benchmark(category)

    if yf_symbol in _BENCHMARK_CACHE:
        return _BENCHMARK_CACHE[yf_symbol]

    fallback = {"label": bm_label, "yf_symbol": yf_symbol,
                "cagr_1y": "N/A", "cagr_3y": "N/A", "cagr_5y": "N/A"}

    try:
        hist = yf.Ticker(yf_symbol).history(period="5y")
        if hist.empty:
            logger.warning("yfinance: empty history for %s", yf_symbol)
            _BENCHMARK_CACHE[yf_symbol] = fallback
            return fallback

        current = float(hist["Close"].iloc[-1])

        def _bm_cagr(days: int, years: float) -> str:
            target = datetime.now() - timedelta(days=days)
            # yfinance index may be timezone-aware
            idx = hist.index.tz_localize(None) if hist.index.tzinfo else hist.index
            past_slice = hist[idx <= target]
            if past_slice.empty:
                return "N/A"
            past = float(past_slice["Close"].iloc[-1])
            return _cagr_str(current, past, years)

        result = {
            "label": bm_label,
            "yf_symbol": yf_symbol,
            "cagr_1y": _bm_cagr(365, 1),
            "cagr_3y": _bm_cagr(1095, 3),
            "cagr_5y": _bm_cagr(1825, 5),
        }
        _BENCHMARK_CACHE[yf_symbol] = result
        return result

    except Exception as exc:
        logger.warning("fetch_benchmark_returns failed for %s: %s", yf_symbol, exc)
        _BENCHMARK_CACHE[yf_symbol] = fallback
        return fallback


# ─── Master context builder ───────────────────────────────────────────────────

def build_fund_context(
    owner_name: str,
    valuation_csv: str = "data/output/master_valuation.csv",
) -> dict[str, Any]:
    """
    Fetch ALL data required for the AI Advisor in one call.

    Calls (in order):
      1. fetch_top3_mf_holdings()     — identifies the 3 funds
      2. fetch_nav_and_cagr()         — MFAPI.in per fund
      3. fetch_scheme_metadata()      — mftool per fund
      4. fetch_benchmark_returns()    — yfinance per unique category

    Returns:
        {
          "owner": str,
          "fetch_date": str,
          "allocation": dict[str, float],   # e.g. {"Mutual Fund": 80.5, "Stock": 15.0, ...}
          "funds": [
            {
              "rank": int,
              "asset_name": str,
              "amfi_code": str,
              "current_value": float,
              "units": float,
              "nav": { fund_name, fund_house, category, scheme_type,
                       current_nav, nav_date, cagr_1y, cagr_3y, cagr_5y },
              "meta": { expense_ratio, fund_manager, aum_cr, inception_date },
              "benchmark": { label, yf_symbol, cagr_1y, cagr_3y, cagr_5y },
              "alpha_1y": str,
              "alpha_3y": str,
              "extras": { ... },
            },
            ...
          ],
          "errors": [str],
          "data_sources": [str],
        }
    """
    logger.info("=== DATA FETCH PHASE: Portfolio for '%s' ===", owner_name)

    result: dict[str, Any] = {
        "owner": owner_name,
        "fetch_date": datetime.now().strftime("%d %b %Y"),
        "allocation": fetch_asset_allocation(owner_name),
        "funds": [],
        "errors": [],
        "data_sources": ["MFAPI.in", "mftool/AMFI", "Yahoo Finance / yfinance"],
    }

    # Step 1 — identify top 3 holdings
    holdings = fetch_top3_mf_holdings(owner_name, valuation_csv)
    if not holdings:
        result["errors"].append(f"No Mutual Fund holdings found for owner: {owner_name}")
        return result

    # Step 2-4 — fetch per-fund data
    for rank, holding in enumerate(holdings, start=1):
        code = holding["amfi_code"]
        name = holding["asset_name"]

        logger.info("[%d/3] Fetching NAV/CAGR for: %s (AMFI: %s)", rank, name, code)
        nav = fetch_nav_and_cagr(code)

        logger.info("[%d/3] Fetching scheme metadata via mftool for: %s", rank, code)
        meta = fetch_scheme_metadata(code)

        category = nav.get("category", "Unknown")
        logger.info("[%d/3] Fetching benchmark for category: %s", rank, category)
        benchmark = fetch_benchmark_returns(category)

        alpha_1y = _alpha_str(nav.get("cagr_1y", "N/A"), benchmark.get("cagr_1y", "N/A"))
        alpha_3y = _alpha_str(nav.get("cagr_3y", "N/A"), benchmark.get("cagr_3y", "N/A"))

        # Step 5 — Live Peer Analytics (peer_returns_engine — mfapi.in, Direct-Growth only)
        extras: dict[str, Any] = {}
        bm_symbol = benchmark.get("yf_symbol", "^NSEI")
        try:
            from analytics.peer_returns_engine import get_peer_analytics
            extras = get_peer_analytics(code, fund_name=name, benchmark_symbol=bm_symbol)
            if extras:
                logger.info("[%d/3] Peer engine: found %d metrics", rank, len(extras))
                if "Live mfapi.in + AMFI" not in result["data_sources"]:
                    result["data_sources"].append("Live mfapi.in + AMFI (Direct-Growth peers)")
        except Exception as exc:
            logger.warning("[%d/3] Peer engine failed (%s): %s", rank, name, exc)

        fund_entry: dict[str, Any] = {
            "rank": rank,
            "asset_name": name,
            "amfi_code": code,
            "current_value": holding["current_value"],
            "units": holding["units"],
            "nav": nav,
            "meta": meta,
            "benchmark": benchmark,
            "alpha_1y": alpha_1y,
            "alpha_3y": alpha_3y,
            "extras": extras,
        }

        logger.info(
            "[%d/3] Done %-45s | 3Y: %-6s | BM 3Y: %-6s | Alpha: %s",
            rank, name[:45], nav.get("cagr_3y"), benchmark.get("cagr_3y"), alpha_3y,
        )
        result["funds"].append(fund_entry)

    logger.info("=== DATA FETCH COMPLETE — %d funds ready for agents ===", len(result["funds"]))
    return result


# ─── Prompt renderer ──────────────────────────────────────────────────────────

def format_prompt_context(data: dict) -> str:
    """
    Render the build_fund_context() dict into a clean, human-readable string
    for injection into LLM prompts.

    Designed to be easy for smaller LLMs to parse:
      - Labeled fields, not raw JSON
      - Consistent indentation
      - Optional extras section appended if present

    Returns:
        Multi-line string suitable for @data_str@ substitution in prompts.
    """
    lines: list[str] = [
        f"Portfolio Owner    : {data['owner']}",
        f"Data as of         : {data['fetch_date']}",
        "Focus Area         : Top 3 Mutual Funds by current market value",
        f"Data sources       : {', '.join(data.get('data_sources', []))}",
        "",
        "MACRO ASSET ALLOCATION (Overall Portfolio):",
    ]
    
    alloc = data.get("allocation", {})
    if alloc:
        for asset_class, pct in alloc.items():
            lines.append(f"  - {asset_class:<15}: {pct}%")
    else:
        lines.append("  - (Asset allocation data not available)")
    lines.append("")

    for fund in data["funds"]:
        nav = fund["nav"]
        meta = fund["meta"]
        bm = fund["benchmark"]
        extras = fund.get("extras", {})

        lines += [
            f"{'─' * 60}",
            f"FUND #{fund['rank']}  {nav.get('fund_name', fund['asset_name'])}",
            f"{'─' * 60}",
            f"  AMFI Code      : {fund['amfi_code']}",
            f"  Fund House     : {nav.get('fund_house', 'Unknown')}",
            f"  Category       : {nav.get('category', 'Unknown')}",
            f"  Scheme Type    : {nav.get('scheme_type', 'Unknown')}",
            f"  Inception Date : {meta.get('inception_date', 'N/A')}",
            f"  Fund Manager   : {meta.get('fund_manager', 'N/A')}",
            "",
            "  Portfolio Data :",
            f"    Units Held   : {fund['units']}",
            f"    Current Val  : ₹{fund['current_value']:,.2f}",
            f"    Current NAV  : ₹{nav.get('current_nav') or 'N/A'}"
            + (f"  (as of {nav.get('nav_date', '')})" if nav.get("nav_date") != "N/A" else ""),
            "",
            f"  Expense Ratio  : {meta.get('expense_ratio', 'N/A')}",
            f"  AUM (Cr)       : {meta.get('aum_cr', 'N/A')}",
            "",
            f"  Performance vs Benchmark ({bm['label']}) :",
            "    Period  | Fund CAGR | Benchmark | Alpha",
            "    --------|-----------|-----------|-------",
            f"    1 Year  | {nav.get('cagr_1y', 'N/A'):>9} | {bm.get('cagr_1y', 'N/A'):>9} | {fund['alpha_1y']}",
            f"    3 Year  | {nav.get('cagr_3y', 'N/A'):>9} | {bm.get('cagr_3y', 'N/A'):>9} | {fund['alpha_3y']}",
            f"    5 Year  | {nav.get('cagr_5y', 'N/A'):>9} | {bm.get('cagr_5y', 'N/A'):>9} | {'N/A (BM 5Y not shown)'}",
        ]

        if extras:
            lines.append("")
            # ─ Live Peer Comparison ─
            peer_keys = {"Peer_Rank_3Y", "Peer_Rank_5Y", "Peer_Median_3Y", "Peer_Median_5Y", "Peer_Category"}
            peer_items = {k: v for k, v in extras.items() if k in peer_keys}
            if peer_items:
                lines.append(f"  Category Peer Comparison ({extras.get('_source', 'Live')}) :")
                lines.append(f"    Sub Category  : {peer_items.get('Peer_Category', 'N/A')}")
                # We show rolling or point-in-time metrics based on what the engine provided
                lines.append(f"    3Y Peer Rank  : {peer_items.get('Peer_Rank_3Y', 'N/A')}")
                lines.append(f"    3Y Peer Median: {peer_items.get('Peer_Median_3Y', 'N/A')} (category-median return)")
                lines.append(f"    5Y Peer Rank  : {peer_items.get('Peer_Rank_5Y', 'N/A')}")
                lines.append(f"    5Y Peer Median: {peer_items.get('Peer_Median_5Y', 'N/A')} (category-median return)")
                lines.append("")

            # ─ Advanced & Risk Metrics ─
            risk_keys = {
                "Rolling_CAGR_3Y", "Rolling_CAGR_5Y",
                "Sharpe_Ratio_3Y", "Sortino_3Y", "Alpha_3Y", "Beta_3Y", "Std_Dev_3Y",
                "Sharpe_Ratio_5Y", "Sortino_5Y", "Alpha_5Y", "Beta_5Y", "Std_Dev_5Y",
            }
            # Only include risk elements that actually exist
            risk_items = {k: v for k, v in extras.items() if k in risk_keys}
            if risk_items:
                lines.append(f"  Advanced & Risk Metrics ({extras.get('_source', 'Live')}) :")
                for k in sorted(risk_items.keys()):
                    lines.append(f"    {k:<20}: {risk_items[k]}")
            if "_data_as_of" in extras:
                lines.append(f"    [Data as-of: {extras['_data_as_of']}]")

        lines.append("")

    if data.get("errors"):
        lines += ["DATA WARNINGS:", *[f"  ⚠  {e}" for e in data["errors"]], ""]

    return "\n".join(lines)


# ─── CLI smoke test ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    owner = sys.argv[1] if len(sys.argv) > 1 else "ALL"
    ctx = build_fund_context(owner)
    logger.info(format_prompt_context(ctx))
