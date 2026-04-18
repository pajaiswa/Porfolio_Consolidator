"""
fetch_allocations.py — Asset Allocation Scraper (Extended)
===========================================================
Searches Moneycontrol for each mutual fund and scrapes its
equity/debt/gold/cash percentage breakdown, PLUS market-cap split
(Large/Mid/Small) and domestic vs international geography split.

Results are cached per-row for 30 days (via `last_scraped_date` column).
A manual override file (`data/input/allocation_overrides.csv`) takes
priority over both scraped and heuristic values.

Inputs  : data/output/master_valuation.csv
          data/input/asset_allocation_map.csv  (existing map — incrementally updated)
          data/input/allocation_overrides.csv  (optional user overrides)

Output  : data/input/asset_allocation_map.csv  (updated)
          data/input/allocation_overrides.csv  (created with headers if missing)

NOTE: The `source` column tracks provenance: "scraped", "heuristic", or "manual_override".
"""
import logging
import os
import re
import time
from datetime import datetime, timedelta
from pathlib import Path

import cloudscraper
import pandas as pd
from googlesearch import search

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CACHE_DAYS = 30  # Re-scrape after this many days

# AMFI category keyword → (LargeCap%, MidCap%, SmallCap%, Intl%)
# Used as heuristic fallback when Moneycontrol scraping fails.
CATEGORY_HEURISTICS: list[tuple[list[str], tuple[float, float, float, float]]] = [
    (["index", "nifty 50", "sensex", "nifty50"],              (100.0, 0.0,  0.0,  0.0)),
    (["large cap", "large-cap", "bluechip", "blue chip"],     (80.0,  15.0, 5.0,  0.0)),
    (["large & mid", "large and mid", "large & midcap"],      (50.0,  45.0, 5.0,  0.0)),
    (["mid cap", "midcap", "mid-cap"],                        (10.0,  75.0, 15.0, 0.0)),
    (["small cap", "smallcap", "small-cap"],                  (5.0,   15.0, 80.0, 0.0)),
    (["micro cap"],                                            (0.0,   10.0, 90.0, 0.0)),
    (["flexi cap", "flexi-cap", "flexicap",
      "multi cap", "multicap", "multi-cap",
      "focused", "elss", "tax sav"],                          (45.0,  35.0, 20.0, 0.0)),
    (["hybrid", "balanced", "dynamic"],                       (50.0,  30.0, 20.0, 0.0)),
    (["international", "global", "world", "overseas",
      "u.s.", "us equity", "nasdaq", "fof"],                  (0.0,   0.0,  0.0,  100.0)),
    (["debt", "liquid", "overnight", "ultra short",
      "short dur", "credit", "gilt", "money market",
      "banking and psu"],                                     (0.0,   0.0,  0.0,  0.0)),
    (["gold", "commodity"],                                   (0.0,   0.0,  0.0,  0.0)),
]

MAP_COLS = [
    "Ticker", "Asset Name",
    "Equity_India_Pct", "Equity_Foreign_Pct", "Debt_Pct", "Gold_Pct", "Cash_Pct",
    "LargeCap_Pct", "MidCap_Pct", "SmallCap_Pct",
    "Domestic_Eq_Pct", "Intl_Eq_Pct",
    "last_scraped_date", "source",
]

OVERRIDE_COLS = MAP_COLS  # Same schema


# ---------------------------------------------------------------------------
# Helpers — scraping
# ---------------------------------------------------------------------------

def get_moneycontrol_url(query: str) -> str | None:
    """Search for the fund's Moneycontrol NAV page URL."""
    try:
        search_query = f"{query} moneycontrol mutual funds nav"
        for url in search(search_query, num_results=3):
            if "moneycontrol.com/mutual-funds/nav/" in url:
                return url
    except Exception as e:
        logger.error("Error searching for %s: %s", query, e)
    return None


def scrape_mc_allocations(url: str) -> dict | None:
    """
    Scrape equity/debt/cash/gold + market-cap + geography from a Moneycontrol fund page.
    Returns a dict with all fields, or None on failure.
    """
    scraper = cloudscraper.create_scraper()
    try:
        res = scraper.get(url, timeout=15)
        if res.status_code != 200:
            return None
        html = res.text

        result: dict = {
            "equity": 0.0, "debt": 0.0, "cash": 0.0, "gold": 0.0,
            "large_cap": None, "mid_cap": None, "small_cap": None,
            "domestic": None, "international": None,
        }

        # --- 1. Broad asset class (Equity / Debt / Cash / Gold) ---
        for key in ["Equity", "Debt", "Cash", "Gold"]:
            m = re.search(
                rf'{key}</span><span[^>]*>([\d\.]+)', html, re.IGNORECASE
            )
            if not m:
                m = re.search(
                    rf'{key}.*?class="[^"]*percent[^"]*".*?>([\d\.]+)', html, re.IGNORECASE
                )
            if m:
                result[key.lower()] = float(m.group(1))

        # --- 2. Market-cap breakdown (Large / Mid / Small Cap) ---
        # Moneycontrol renders these in a "Market Cap" section
        for cap_key, patterns in [
            ("large_cap",  [r"Large Cap[^<]*</[^>]+>\s*<[^>]+>([\d\.]+)",
                            r"Large\s*Cap.*?([\d\.]+)\s*%"]),
            ("mid_cap",    [r"Mid Cap[^<]*</[^>]+>\s*<[^>]+>([\d\.]+)",
                            r"Mid\s*Cap.*?([\d\.]+)\s*%"]),
            ("small_cap",  [r"Small Cap[^<]*</[^>]+>\s*<[^>]+>([\d\.]+)",
                            r"Small\s*Cap.*?([\d\.]+)\s*%"]),
        ]:
            for pattern in patterns:
                m = re.search(pattern, html, re.IGNORECASE | re.DOTALL)
                if m:
                    result[cap_key] = float(m.group(1))
                    break

        # --- 3. Geographic breakdown (Domestic / International) ---
        for geo_key, patterns in [
            ("domestic",      [r"Domestic[^<]*</[^>]+>\s*<[^>]+>([\d\.]+)",
                               r"Domestic.*?([\d\.]+)\s*%"]),
            ("international", [r"International[^<]*</[^>]+>\s*<[^>]+>([\d\.]+)",
                               r"International.*?([\d\.]+)\s*%"]),
        ]:
            for pattern in patterns:
                m = re.search(pattern, html, re.IGNORECASE | re.DOTALL)
                if m:
                    result[geo_key] = float(m.group(1))
                    break

        return result

    except Exception as e:
        logger.error("Error scraping %s: %s", url, e)
    return None


def is_foreign_fund(name: str) -> bool:
    """Returns True if fund name implies foreign equity exposure."""
    keywords = ["u.s.", "us equity", "nasdaq", "overseas", "global",
                "international", "foreign", "world", "fof"]
    return any(kw in name.lower() for kw in keywords)


# ---------------------------------------------------------------------------
# Helpers — heuristics
# ---------------------------------------------------------------------------

def _heuristic_cap_split(category: str, name: str) -> tuple[float, float, float, float]:
    """
    Return (LargeCap%, MidCap%, SmallCap%, Intl%) from AMFI category keyword matching.
    Falls back to (50, 30, 20, 0) for unmapped categories.
    """
    text = (category + " " + name).lower()
    for keywords, split in CATEGORY_HEURISTICS:
        if any(kw in text for kw in keywords):
            return split
    return (50.0, 30.0, 20.0, 0.0)  # generic equity fallback


def _build_row(
    ticker: str,
    name: str,
    eq_ind: float, eq_for: float, debt: float, gold: float, cash: float,
    large_cap: float, mid_cap: float, small_cap: float,
    domestic_eq: float, intl_eq: float,
    source: str,
) -> dict:
    return {
        "Ticker": ticker, "Asset Name": name,
        "Equity_India_Pct": round(eq_ind, 2),
        "Equity_Foreign_Pct": round(eq_for, 2),
        "Debt_Pct": round(debt, 2),
        "Gold_Pct": round(gold, 2),
        "Cash_Pct": round(cash, 2),
        "LargeCap_Pct": round(large_cap, 2),
        "MidCap_Pct": round(mid_cap, 2),
        "SmallCap_Pct": round(small_cap, 2),
        "Domestic_Eq_Pct": round(domestic_eq, 2),
        "Intl_Eq_Pct": round(intl_eq, 2),
        "last_scraped_date": datetime.now().strftime("%Y-%m-%d"),
        "source": source,
    }


def _is_stale(row: dict) -> bool:
    """Return True if the cached row is older than CACHE_DAYS days."""
    date_str = row.get("last_scraped_date", "")
    if not date_str:
        return True
    try:
        scraped = datetime.strptime(str(date_str), "%Y-%m-%d")
        return (datetime.now() - scraped) > timedelta(days=CACHE_DAYS)
    except Exception:
        return True


# ---------------------------------------------------------------------------
# Override CSV management
# ---------------------------------------------------------------------------

def _load_overrides(override_path: Path) -> dict[str, dict]:
    """Load manual overrides. Creates the file with headers if missing."""
    if not override_path.exists():
        # Create template
        pd.DataFrame(columns=OVERRIDE_COLS).to_csv(override_path, index=False)
        logger.info(
            "Created override template at %s — add rows here to override scraped values.", override_path
        )
        return {}
    try:
        df = pd.read_csv(override_path)
        return {str(row["Ticker"]): row.to_dict() for _, row in df.iterrows()
                if pd.notna(row.get("Ticker"))}
    except Exception as e:
        logger.warning("Could not read overrides file: %s", e)
        return {}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def fetch_and_build_allocation_map(
    val_path: str = "data/output/master_valuation.csv",
    map_path: str = "data/input/asset_allocation_map.csv",
    override_path: str = "data/input/allocation_overrides.csv",
) -> None:
    """
    Builds or incrementally updates the asset_allocation_map.csv.

    Priority order (highest first):
      1. manual_override  — data/input/allocation_overrides.csv
      2. scraped          — Moneycontrol (cached ≤ 30 days)
      3. heuristic        — AMFI category keyword lookup
    """
    logger.info("SCRAPING ASSET ALLOCATIONS")

    if not Path(val_path).exists():
        logger.error("Error: %s not found. Run previous pipeline steps first.", val_path)
        return

    df_val = pd.read_csv(val_path)
    assets_to_map = df_val[
        df_val["Asset Class"].str.upper().isin(["MUTUAL FUND", "STOCK"])
    ].drop_duplicates(subset=["Ticker"])

    # Load existing map (with backwards-compat for old CSV without new columns)
    existing_map: dict[str, dict] = {}
    map_file = Path(map_path)
    if map_file.exists():
        try:
            for _, row in pd.read_csv(map_file).iterrows():
                existing_map[str(row["Ticker"])] = row.to_dict()
        except Exception as e:
            logger.warning("Could not load existing map: %s", e)

    # Load manual overrides
    overrides = _load_overrides(Path(override_path))
    if overrides:
        logger.info("Loaded %d manual overrides.", len(overrides))

    new_rows: list[dict] = []

    for _, asset_row in assets_to_map.iterrows():
        ticker = str(asset_row["Ticker"])
        name = str(asset_row["Asset Name"])
        aclass = str(asset_row["Asset Class"]).upper()

        # ── 1. Manual override (always wins) ──────────────────────────────
        if ticker in overrides:
            ov = overrides[ticker]
            logger.info("  [OVERRIDE] %s", name)
            row_dict = {k: ov.get(k, 0.0) for k in MAP_COLS}
            row_dict["source"] = "manual_override"
            row_dict["last_scraped_date"] = datetime.now().strftime("%Y-%m-%d")
            new_rows.append(row_dict)
            continue

        # ── 2. Valid cache (< 30 days old) ────────────────────────────────
        if ticker in existing_map and not _is_stale(existing_map[ticker]):
            logger.info("  [CACHE] %s (cached on %s)", name, existing_map[ticker].get("last_scraped_date"))
            cached = existing_map[ticker]
            # Ensure new columns exist with defaults if this is an old-format row
            for col in MAP_COLS:
                cached.setdefault(col, 0.0)
            new_rows.append(cached)
            continue

        # ── 3. Stocks & ETFs — fast heuristic (no scrape) ─────────────────
        if aclass == "STOCK":
            ticker_up = ticker.upper()
            name_up = name.upper()
            if "GOLD" in ticker_up or "SGB" in ticker_up or "GOLD" in name_up:
                eq_ind, eq_for, debt, gold, cash = 0.0, 0.0, 0.0, 100.0, 0.0
                lc, mc, sc, intl_pct = 0.0, 0.0, 0.0, 0.0
                dom_eq = 0.0
            elif any(f in ticker_up for f in ["MON100", "MAFANG", "MASPTOP50"]) or "NASDAQ" in name_up:
                eq_ind, eq_for, debt, gold, cash = 0.0, 100.0, 0.0, 0.0, 0.0
                lc, mc, sc, intl_pct = 0.0, 0.0, 0.0, 100.0
                dom_eq = 0.0
            else:
                # Default stock → Large Cap India equity
                eq_ind, eq_for, debt, gold, cash = 100.0, 0.0, 0.0, 0.0, 0.0
                lc, mc, sc, intl_pct = 100.0, 0.0, 0.0, 0.0
                dom_eq = 100.0
            logger.info("  [HEURISTIC] Stock/ETF: %s", name)
            new_rows.append(_build_row(
                ticker, name, eq_ind, eq_for, debt, gold, cash,
                lc, mc, sc, dom_eq, intl_pct, "heuristic"
            ))
            continue

        # ── 4. Mutual Fund — attempt Moneycontrol scrape ──────────────────
        logger.info("  [SCRAPING] %s", name)
        url = get_moneycontrol_url(name)

        # Determine heuristic cap split from mftool category (passed via Asset Name heuristic)
        h_lc, h_mc, h_sc, h_intl = _heuristic_cap_split("", name)

        def _fallback_row(src: str = "heuristic") -> dict:
            is_foreign = is_foreign_fund(name)
            eq_ind_ = 0.0 if is_foreign else 100.0
            eq_for_ = 100.0 if is_foreign else 0.0
            lc_ = h_lc if not is_foreign else 0.0
            mc_ = h_mc if not is_foreign else 0.0
            sc_ = h_sc if not is_foreign else 0.0
            intl_ = h_intl if is_foreign else 0.0
            dom_ = 100.0 if not is_foreign else 0.0
            return _build_row(
                ticker, name, eq_ind_, eq_for_, 0.0, 0.0, 0.0,
                lc_, mc_, sc_, dom_, intl_, src
            )

        if not url:
            logger.warning("  Could not find Moneycontrol URL for %s", name)
            new_rows.append(_fallback_row())
            continue

        logger.info("     URL: %s", url)
        alloc = scrape_mc_allocations(url)

        if alloc:
            logger.info("     Scraped: %s", alloc)
            is_foreign = is_foreign_fund(name)
            eq_val = alloc.get("equity", 0.0)
            eq_ind = 0.0 if is_foreign else eq_val
            eq_for = eq_val if is_foreign else 0.0

            # Cap-size: use scraped if available, else heuristic
            large_cap = alloc["large_cap"] if alloc["large_cap"] is not None else h_lc
            mid_cap   = alloc["mid_cap"]   if alloc["mid_cap"]   is not None else h_mc
            small_cap = alloc["small_cap"] if alloc["small_cap"] is not None else h_sc

            # Geography: use scraped if available, else derive from foreign/domestic flags
            intl_eq = alloc["international"] if alloc["international"] is not None else (
                h_intl if is_foreign else 0.0
            )
            dom_eq  = alloc["domestic"] if alloc["domestic"] is not None else (
                0.0 if is_foreign else 100.0
            )

            new_rows.append(_build_row(
                ticker, name, eq_ind, eq_for,
                alloc.get("debt", 0.0), alloc.get("gold", 0.0), alloc.get("cash", 0.0),
                large_cap, mid_cap, small_cap, dom_eq, intl_eq, "scraped"
            ))
        else:
            logger.warning("  Could not parse allocations for %s — using heuristic", name)
            new_rows.append(_fallback_row())

        time.sleep(2)  # polite delay between requests

    if new_rows:
        pd.DataFrame(new_rows, columns=MAP_COLS).to_csv(map_file, index=False)
        logger.info("Asset allocation map saved to %s (%d rows)", map_path, len(new_rows))
        logger.info(
            "Tip: Edit data/input/allocation_overrides.csv to manually fix any fund's values."
        )
    else:
        logger.warning("No assets found to process.")


if __name__ == "__main__":
    import logging as _logging
    _logging.basicConfig(
        level=_logging.INFO,
        format="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    fetch_and_build_allocation_map()
