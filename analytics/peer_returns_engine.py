"""
peer_returns_engine.py — Live Peer Comparison Engine for Indian Mutual Funds
=============================================================================
Computes risk metrics and peer rankings in real-time by:
  1. Parsing the free AMFI master scheme list to get sub-category for every scheme.
  2. Fetching 3Y+ NAV history from mfapi.in for all Direct-Growth peers in the category.
  3. Computing CAGR (1Y/3Y/5Y), Std Dev, Sharpe, Sortino, Beta, Alpha from raw NAVs.
  4. Caching peer group metrics locally for 7 days to avoid re-fetching every run.

Key Design Decisions:
  - DIRECT PLANS ONLY: All peer comparisons use Direct-Growth NAVs (lowest expense).
  - WEEKLY CACHE: JSON cache per sub-category, invalidated after 7 days.
  - GRACEFUL FAILURES: Every external call returns an empty dict on failure — never
    crashes the advisor pipeline.
  - Rate limiting: 0.3s sleep between mfapi.in calls to respect the free service.
"""
from __future__ import annotations

import json
import logging
import math
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests
import yfinance as yf

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
AMFI_MASTER_URL  = "https://www.amfiindia.com/spages/NAVAll.txt"
MFAPI_NAV_URL    = "https://api.mfapi.in/mf/{code}"
CACHE_DIR        = Path("data/cache/peer_engine")
AMFI_CACHE_TTL   = timedelta(days=1)       # refresh AMFI list daily
PEER_CACHE_TTL   = timedelta(days=7)       # refresh peer metrics weekly
RISK_FREE_RATE   = 0.065                   # 6.5% (approx RBI 91-day T-bill)
MIN_PEERS        = 5                       # skip ranking if too few peers
REQUEST_DELAY    = 0.3                     # seconds between mfapi.in calls
REQUEST_TIMEOUT  = 10                      # seconds per request

CACHE_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# AMFI Master Scheme List
# ---------------------------------------------------------------------------

def _amfi_cache_path() -> Path:
    return CACHE_DIR / "amfi_scheme_list.json"


def fetch_amfi_scheme_list(force_refresh: bool = False) -> pd.DataFrame:
    """
    Download and parse the AMFI master NAV file.
    Returns a DataFrame with columns:
        scheme_code, scheme_name, sub_category, is_direct, is_growth

    The AMFI file format is:
        Open Ended Schemes(Category Name)   ← category header line
        AMC Name                            ← AMC header (no semicolons)
        code;isin1;isin2;name;type;nav;date ← data row (7 semicolon-separated fields)

    Only Direct-Growth schemes are retained for peer comparisons.
    """
    cache_path = _amfi_cache_path()

    # Return cached version if fresh
    if not force_refresh and cache_path.exists():
        age = datetime.now() - datetime.fromtimestamp(cache_path.stat().st_mtime)
        if age < AMFI_CACHE_TTL:
            df = pd.read_json(cache_path, orient="records", dtype={"scheme_code": str})
            logger.debug("AMFI scheme list loaded from cache (%d schemes)", len(df))
            return df

    logger.info("Fetching AMFI master scheme list from %s", AMFI_MASTER_URL)
    try:
        resp = requests.get(AMFI_MASTER_URL, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.error("AMFI fetch failed: %s", exc)
        if cache_path.exists():
            logger.warning("Returning stale AMFI cache")
            return pd.read_json(cache_path, orient="records", dtype={"scheme_code": str})
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []
    current_sub_category: str = ""

    for line in resp.text.splitlines():
        line = line.strip()
        if not line:
            continue

        # Category header: "Open Ended Schemes(Equity Scheme - Flexi Cap Fund)"
        if line.startswith("Open Ended Schemes(") or line.startswith("Close Ended"):
            # Extract the category inside parentheses e.g. "Equity Scheme - Flexi Cap Fund"
            start = line.find("(")
            end   = line.rfind(")")
            if start != -1 and end != -1:
                current_sub_category = line[start + 1:end].strip()
            continue

        # Data row: 7 semicolon-separated fields
        parts = line.split(";")
        if len(parts) < 5:
            continue
        try:
            int(parts[0])  # first field must be numeric scheme code
        except ValueError:
            continue

        scheme_code  = parts[0].strip()
        scheme_name  = parts[3].strip() if len(parts) > 3 else ""
        scheme_type  = parts[4].strip() if len(parts) > 4 else ""

        name_lower = scheme_name.lower()
        type_lower = scheme_type.lower()

        is_direct = "direct" in name_lower or "direct" in type_lower
        is_growth = (
            "growth" in name_lower
            or "growth" in type_lower
            or ("idcw" not in name_lower and "dividend" not in name_lower)
        )

        rows.append({
            "scheme_code":    scheme_code,
            "scheme_name":    scheme_name,
            "sub_category":   current_sub_category,
            "is_direct":      is_direct,
            "is_growth":      is_growth,
        })

    df = pd.DataFrame(rows)
    df.to_json(cache_path, orient="records", indent=2)
    logger.info("AMFI scheme list cached: %d total schemes", len(df))
    return df


def get_peer_scheme_codes(amfi_code: str, fund_name: str = "") -> tuple[str, list[str]]:
    """
    Find the sub-category for a given scheme and return:
        (sub_category, [list of Direct-Growth peer scheme codes])

    Returns ("", []) if the scheme cannot be identified.
    """
    df = fetch_amfi_scheme_list()
    if df.empty:
        return ("", [])

    # Locate the target fund
    row = df[df["scheme_code"] == str(amfi_code).strip()]
    if row.empty and fund_name:
        # Fuzzy fallback: 3+ word overlap on scheme_name
        words = set(fund_name.lower().split())
        df["_score"] = df["scheme_name"].str.lower().apply(
            lambda n: len(set(n.split()) & words)
        )
        best = df.nlargest(1, "_score")
        if not best.empty and best.iloc[0]["_score"] >= 3:
            row = best
        df.drop(columns=["_score"], inplace=True, errors="ignore")

    if row.empty:
        logger.warning("peer_engine: scheme not found for code=%s name=%s", amfi_code, fund_name)
        return ("", [])

    sub_category = str(row.iloc[0]["sub_category"])

    # All Direct-Growth peers in same sub-category
    peers = df[
        (df["sub_category"] == sub_category)
        & (df["is_direct"] == True)
        & (df["is_growth"] == True)
    ]
    peer_codes = peers["scheme_code"].tolist()
    logger.debug(
        "peer_engine: sub_category='%s', %d Direct-Growth peers found",
        sub_category, len(peer_codes)
    )
    return (sub_category, peer_codes)


# ---------------------------------------------------------------------------
# NAV History Fetch
# ---------------------------------------------------------------------------

def _fetch_nav_history(scheme_code: str) -> pd.Series:
    """
    Fetch 5Y+ historical NAVs for a scheme from mfapi.in.
    Returns a pd.Series indexed by date (descending), values are float NAVs.
    Returns empty Series on failure.
    """
    try:
        url = MFAPI_NAV_URL.format(code=scheme_code)
        resp = requests.get(url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json().get("data", [])
        if not data:
            return pd.Series(dtype=float)

        records = {
            datetime.strptime(d["date"], "%d-%m-%Y"): float(d["nav"])
            for d in data
            if d.get("nav") and d["nav"] not in ("", "N.A.", "-")
        }
        series = pd.Series(records).sort_index()
        return series

    except Exception as exc:
        logger.debug("NAV fetch failed for %s: %s", scheme_code, exc)
        return pd.Series(dtype=float)


# ---------------------------------------------------------------------------
# Metric Computation
# ---------------------------------------------------------------------------

def _cagr(series: pd.Series, years: float) -> float | None:
    """Compute CAGR over `years` from the most recent NAV."""
    if series.empty:
        return None
    latest_date = series.index[-1]
    target_date = latest_date - timedelta(days=int(years * 365.25))
    past = series.asof(target_date)
    current = series.iloc[-1]
    if past is None or math.isnan(past) or past <= 0:
        return None
    try:
        return ((current / past) ** (1 / years) - 1) * 100
    except Exception:
        return None


def _absolute_return(series: pd.Series, months: int) -> float | None:
    """Compute absolute percentage return over `months` from the most recent NAV."""
    if series.empty:
        return None
    latest_date = series.index[-1]
    target_date = latest_date - timedelta(days=int(months * 30.436875))
    past = series.asof(target_date)
    current = series.iloc[-1]
    if past is None or math.isnan(past) or past <= 0:
        return None
    try:
        return ((current / past) - 1) * 100
    except Exception:
        return None

def _compute_median_rolling_return(series: pd.Series, window_years: int, observation_years: int = 3) -> float | None:
    """
    Compute the median rolling CAGR for a given `window_years` (e.g., 3Y or 5Y),
    sampled daily over the last `observation_years` (e.g., looking at rolling returns over the last 3 years).
    """
    if series.empty:
        return None
        
    latest_date = series.index[-1]
    # We want to observe rolling returns generated between (today - observation_years) and today.
    # To compute a return on a date D, we need data from (D - window_years).
    # So we need data going back (observation_years + window_years).
    start_observation_date = latest_date - timedelta(days=int(observation_years * 365.25))
    
    # Filter series to observation window for iteration
    obs_series = series[series.index >= start_observation_date]
    if obs_series.empty:
        return None
        
    rolling_returns = []
    
    # Iterate through each daily date in the observation period
    for current_date, current_val in obs_series.items():
        if pd.isna(current_val) or current_val <= 0:
            continue
            
        past_date = current_date - timedelta(days=int(window_years * 365.25))
        past_val = series.asof(past_date)
        
        if past_val is not None and not pd.isna(past_val) and past_val > 0:
            # We have a valid window
            cagr = ((current_val / past_val) ** (1 / window_years) - 1) * 100
            rolling_returns.append(cagr)
            
    if not rolling_returns:
        return None
        
    return float(np.median(rolling_returns))


def compute_fund_metrics(
    scheme_code: str,
    benchmark_symbol: str = "^NSEI",
    nav_series: pd.Series | None = None,
) -> dict[str, Any]:
    """
    Compute all fund metrics from NAV history.

    Returns dict with:
        cagr_1y, cagr_3y, cagr_5y     (%, float)
        std_dev_3y                      (annualised std dev of daily returns, %)
        sharpe_3y                       (Sharpe Ratio over 3Y)
        sortino_3y                      (Sortino Ratio over 3Y)
        beta_3y                         (Beta vs benchmark)
        alpha_3y                        (Jensen's Alpha, %)
        data_as_of                      (ISO date string)

    Returns {} on insufficient data.
    """
    if nav_series is None:
        nav_series = _fetch_nav_history(scheme_code)

    if nav_series.empty or len(nav_series) < 30:
        return {}

    out: dict[str, Any] = {
        "data_as_of": nav_series.index[-1].strftime("%Y-%m-%d"),
    }

    # --- Point-in-time CAGRs (kept for legacy/comparison) ---
    for yr_label, yrs in [("cagr_1y", 1), ("cagr_3y", 3), ("cagr_5y", 5)]:
        v = _cagr(nav_series, yrs)
        if v is not None:
            out[yr_label] = round(v, 2)
            
    # --- Short-term Point-in-time Returns (Absolute) ---
    for mo_label, mos in [("ret_1m", 1), ("ret_3m", 3), ("ret_6m", 6)]:
        v = _absolute_return(nav_series, mos)
        if v is not None:
            out[mo_label] = round(v, 2)
            
    # --- Rolling Median CAGRs (The new standard for robust comparison) ---
    for yr_label, yrs in [("rolling_cagr_3y", 3), ("rolling_cagr_5y", 5)]:
        # Calculate over a 3-year observation window 
        # (i.e. median of all 3Y returns observed in the last 3 years)
        v = _compute_median_rolling_return(nav_series, window_years=yrs, observation_years=3)
        if v is not None:
            out[yr_label] = round(v, 2)

    # --- Risk Metrics & Beta (3Y and 5Y) ---
    # Fetch 5Y benchmark data once
    bm_data_5y = pd.Series(dtype=float)
    try:
        bm_data_5y = yf.Ticker(benchmark_symbol).history(period="5y")["Close"]
    except Exception as exc:
        logger.debug("Benchmark fetch failed for %s: %s", benchmark_symbol, exc)

    for years, yr_label in [(3, "_3y"), (5, "_5y")]:
        cutoff = nav_series.index[-1] - timedelta(days=years * 365)
        series_window = nav_series[nav_series.index >= cutoff]

        # Require roughly enough data for the window (e.g. 60 days minimum for any stat, but ideally 80% of trading days)
        # We enforce 60 days as absolute minimum for 3Y, 200 for 5Y
        min_days = 60 if years == 3 else 200
        if len(series_window) < min_days:
            continue

        daily_ret = series_window.pct_change().dropna()

        # Annualised Std Dev
        std_daily = daily_ret.std()
        std_annual = std_daily * math.sqrt(252) * 100
        out[f"std_dev{yr_label}"] = round(std_annual, 2)

        # Sharpe Ratio (annualised)
        daily_rf = RISK_FREE_RATE / 252
        excess_ret = daily_ret - daily_rf
        if std_daily > 0:
            sharpe = (excess_ret.mean() / std_daily) * math.sqrt(252)
            out[f"sharpe{yr_label}"] = round(sharpe, 2)

        # Sortino Ratio
        downside = daily_ret[daily_ret < 0]
        if not downside.empty:
            sortino_std = downside.std()
            if sortino_std > 0:
                sortino = (daily_ret.mean() * 252 - RISK_FREE_RATE) / (sortino_std * math.sqrt(252))
                out[f"sortino{yr_label}"] = round(sortino, 2)

        # Beta & Alpha
        if not bm_data_5y.empty:
            bm_window = bm_data_5y[bm_data_5y.index >= pd.to_datetime(cutoff).tz_localize(bm_data_5y.index.tz)]
            if not bm_window.empty:
                try:
                    bm_daily_ret = bm_window.pct_change().dropna()
                    
                    fund_aligned = daily_ret.copy()
                    fund_aligned.index = pd.to_datetime(fund_aligned.index)
                    bm_aligned = bm_daily_ret.copy()
                    bm_aligned.index = pd.to_datetime(bm_aligned.index.date)

                    merged = pd.DataFrame({"fund": fund_aligned, "bm": bm_aligned}).dropna()
                    if len(merged) >= min_days:
                        cov_matrix = merged.cov()
                        beta = cov_matrix.loc["fund", "bm"] / merged["bm"].var()
                        out[f"beta{yr_label}"] = round(beta, 2)

                        fund_cagr = out.get(f"cagr{yr_label}")
                        bm_cagr = _cagr(bm_window, years)
                        if fund_cagr is not None and bm_cagr is not None:
                            alpha = fund_cagr - (RISK_FREE_RATE * 100 + beta * (bm_cagr - RISK_FREE_RATE * 100))
                            out[f"alpha{yr_label}"] = round(alpha, 2)
                except Exception as exc:
                    logger.debug("Beta/Alpha computation failed for %s (%s): %s", scheme_code, yr_label, exc)

    return out

def compute_benchmark_metrics(benchmark_symbol: str) -> dict[str, Any]:
    """Compute risk metrics for a benchmark index itself, treating it like a fund."""
    out = {}
    try:
        bm_data_5y = yf.Ticker(benchmark_symbol).history(period="5y")["Close"]
        if bm_data_5y.empty:
            return out
            
        # Absolute and CAGRs
        for mo_label, mos in [("ret_1m", 1), ("ret_3m", 3), ("ret_6m", 6)]:
            v = _absolute_return(bm_data_5y, mos)
            if v is not None: out[mo_label] = round(v, 2)
            
        for yr_label, yrs in [("cagr_1y", 1), ("cagr_3y", 3), ("cagr_5y", 5)]:
            v = _cagr(bm_data_5y, yrs)
            if v is not None: out[yr_label] = round(v, 2)
            
        # Risk Metrics (3Y and 5Y)
        for years, yr_label in [(3, "_3y"), (5, "_5y")]:
            cutoff = bm_data_5y.index[-1] - timedelta(days=years * 365)
            series_window = bm_data_5y[bm_data_5y.index >= cutoff]
            
            min_days = 60 if years == 3 else 200
            if len(series_window) < min_days:
                continue
                
            daily_ret = series_window.pct_change().dropna()
            
            std_daily = daily_ret.std()
            std_annual = std_daily * math.sqrt(252) * 100
            out[f"std_dev{yr_label}"] = round(std_annual, 2)
            
            daily_rf = RISK_FREE_RATE / 252
            excess_ret = daily_ret - daily_rf
            if std_daily > 0:
                sharpe = (excess_ret.mean() / std_daily) * math.sqrt(252)
                out[f"sharpe{yr_label}"] = round(sharpe, 2)
                
            downside = daily_ret[daily_ret < 0]
            if not downside.empty:
                sortino_std = downside.std()
                if sortino_std > 0:
                    sortino = (daily_ret.mean() * 252 - RISK_FREE_RATE) / (sortino_std * math.sqrt(252))
                    out[f"sortino{yr_label}"] = round(sortino, 2)
                    
            # A benchmark has Beta 1.0 and Alpha 0.0 to itself
            out[f"beta{yr_label}"] = 1.00
            out[f"alpha{yr_label}"] = 0.00
            
    except Exception as exc:
        logger.debug("Benchmark compute failed for %s: %s", benchmark_symbol, exc)
    return out


# ---------------------------------------------------------------------------
# Peer-Group Cache
# ---------------------------------------------------------------------------

def _peer_cache_path(sub_category: str) -> Path:
    safe_name = sub_category.lower().replace(" ", "_").replace("/", "_").replace("(", "").replace(")", "")
    return CACHE_DIR / f"peers_{safe_name}.json"


def _load_peer_cache(sub_category: str) -> dict[str, Any] | None:
    path = _peer_cache_path(sub_category)
    if not path.exists():
        return None
    age = datetime.now() - datetime.fromtimestamp(path.stat().st_mtime)
    if age > PEER_CACHE_TTL:
        logger.info("Peer cache expired for '%s' (age: %s)", sub_category, age)
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _save_peer_cache(sub_category: str, data: dict[str, Any]) -> None:
    path = _peer_cache_path(sub_category)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    logger.debug("Peer cache saved for '%s'", sub_category)


def build_peer_group_metrics(
    sub_category: str,
    peer_codes: list[str],
    benchmark_symbol: str = "^NSEI",
) -> dict[str, Any]:
    """
    Fetch NAV history and compute metrics for all peers in a sub-category.
    Results are persisted in a JSON weekly cache.

    Returns a dict of {scheme_code: {cagr_1y, cagr_3y, ..., sharpe_3y, ...}}
    """
    cached = _load_peer_cache(sub_category)
    if cached is not None:
        logger.info("Loaded peer metrics from cache for '%s' (%d peers)", sub_category, len(cached))
        return cached

    logger.info(
        "Computing live peer metrics for '%s' — %d Direct-Growth schemes (this may take 1-2 min)...",
        sub_category, len(peer_codes)
    )

    results: dict[str, Any] = {}
    for i, code in enumerate(peer_codes, start=1):
        logger.debug("[%d/%d] Fetching NAV for scheme %s", i, len(peer_codes), code)
        nav = _fetch_nav_history(code)
        metrics = compute_fund_metrics(code, benchmark_symbol=benchmark_symbol, nav_series=nav)
        if metrics:
            results[code] = metrics
        time.sleep(REQUEST_DELAY)

    _save_peer_cache(sub_category, results)
    logger.info("Peer metrics computed and cached for '%s' (%d valid)", sub_category, len(results))
    return results


# ---------------------------------------------------------------------------
# Percentile Helper
# ---------------------------------------------------------------------------

def _ordinal(n: int) -> str:
    """Return proper English ordinal (1st, 2nd, 3rd, 4th...)."""
    if 11 <= (n % 100) <= 13:
        return f"{n}th"
    return {1: f"{n}st", 2: f"{n}nd", 3: f"{n}rd"}.get(n % 10, f"{n}th")


def _percentile_rank(value: float, peer_values: list[float]) -> int:
    """Return the percentile rank (0-100) of value among peer_values."""
    if not peer_values:
        return 0
    return int(sum(1 for v in peer_values if v < value) / len(peer_values) * 100)


# ---------------------------------------------------------------------------
# Main Public API — drop-in replacement for enrich_from_kaggle()
# ---------------------------------------------------------------------------

def get_peer_analytics(
    amfi_code: str,
    fund_name: str = "",
    benchmark_symbol: str = "^NSEI",
) -> dict[str, Any]:
    """
    Compute live peer analytics for a fund.

    Returns a dict with risk output:
        Peer_Category, Peer_Rank_3Y, Peer_Median_3Y, Peer_Rank_5Y, Peer_Median_5Y,
        Sharpe_Ratio_3Y, Sortino_3Y, Beta_3Y, Alpha_3Y, Std_Dev_3Y,
        CAGR_1Y_Live, CAGR_3Y_Live, CAGR_5Y_Live,
        _source, _data_as_of

    Returns {} on failure (never raises).
    """
    try:
        sub_category, peer_codes = get_peer_scheme_codes(amfi_code, fund_name)
        if not sub_category or len(peer_codes) < MIN_PEERS:
            logger.warning("peer_engine: insufficient peers for code=%s (%d found)", amfi_code, len(peer_codes))
            return {}

        # Compute metrics for all peers (cached weekly)
        peer_metrics = build_peer_group_metrics(sub_category, peer_codes, benchmark_symbol)

        # Get this specific fund's metrics
        fund_data = peer_metrics.get(str(amfi_code).strip())
        if fund_data is None:
            # Fund might not be in peer list (e.g. code mismatch) — compute directly
            logger.info("peer_engine: computing metrics directly for %s", amfi_code)
            fund_data = compute_fund_metrics(amfi_code, benchmark_symbol=benchmark_symbol)

        if not fund_data:
            return {}

        out: dict[str, Any] = {
            "_source":     "Live mfapi.in + AMFI (Direct-Growth peers)",
            "_data_as_of": fund_data.get("data_as_of", "Unknown"),
            "Peer_Category": sub_category,
        }

        # Individual fund metrics
        for field_map in [
            ("ret_1m",          "Ret_1M_Live"),
            ("ret_3m",          "Ret_3M_Live"),
            ("ret_6m",          "Ret_6M_Live"),
            ("cagr_1y",         "CAGR_1Y_Live"),
            ("cagr_3y",         "CAGR_3Y_Live"),
            ("cagr_5y",         "CAGR_5Y_Live"),
            ("rolling_cagr_3y", "Rolling_CAGR_3Y"),
            ("rolling_cagr_5y", "Rolling_CAGR_5Y"),
            ("std_dev_3y",      "Std_Dev_3Y"),
            ("sharpe_3y",       "Sharpe_Ratio_3Y"),
            ("sortino_3y",      "Sortino_3Y"),
            ("beta_3y",         "Beta_3Y"),
            ("alpha_3y",        "Alpha_3Y"),
            ("std_dev_5y",      "Std_Dev_5Y"),
            ("sharpe_5y",       "Sharpe_Ratio_5Y"),
            ("sortino_5y",      "Sortino_5Y"),
            ("beta_5y",         "Beta_5Y"),
            ("alpha_5y",        "Alpha_5Y"),
        ]:
            src_key, out_key = field_map
            if src_key in fund_data:
                out[out_key] = fund_data[src_key]

        # Peer-group percentile ranks & medians
        # We now calculate percentiles based on ROLLING returns if available, otherwise fallback to point-in-time
        peer_metrics_maps = [
            ("rolling_cagr_3y", "cagr_3y", "Peer_Rank_3Y", "Peer_Median_3Y"),
            ("rolling_cagr_5y", "cagr_5y", "Peer_Rank_5Y", "Peer_Median_5Y"),
        ]
        
        for primary_key, fallback_key, rank_key, median_key in peer_metrics_maps:
            metric_key = primary_key if primary_key in fund_data else fallback_key
            fund_val = fund_data.get(metric_key)
            if fund_val is None:
                continue

            peer_vals = [
                m[metric_key]
                for m in peer_metrics.values()
                if metric_key in m and m[metric_key] is not None
            ]

            if len(peer_vals) >= MIN_PEERS:
                pct = _percentile_rank(fund_val, peer_vals)
                median = round(float(np.median(peer_vals)), 1)
                year_label = "3Y" if "3y" in metric_key else "5Y"
                out[rank_key]   = f"{_ordinal(pct)} Percentile in '{sub_category}' (Out of {len(peer_vals)} Direct funds)"
                out[median_key] = f"{median}%"

        return out

    except Exception as exc:
        logger.error("peer_engine: get_peer_analytics failed for %s: %s", amfi_code, exc, exc_info=True)
        return {}
