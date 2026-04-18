"""
compute_equity_lookthrough.py — Equity Look-Through Analysis
=============================================================
Aggregates the true market-cap and geographic equity exposure across the
whole portfolio using the extended asset_allocation_map.csv.

For Mutual Funds: uses LargeCap_Pct / MidCap_Pct / SmallCap_Pct /
                  Domestic_Eq_Pct / Intl_Eq_Pct from the allocation map.
For Stocks:       Nifty 50 constituents → Large Cap
                  Nifty Midcap 150 constituents → Mid Cap
                  Everything else → Small Cap (with override support)

Inputs  : data/output/master_valuation.csv
          data/input/asset_allocation_map.csv
          data/input/allocation_overrides.csv  (optional)

Output  : data/output/equity_lookthrough.csv
"""
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Nifty 50 + Midcap 150 static constituents (as of March 2025)
# Used to classify direct stock holdings into Large / Mid / Small cap.
# Update this annually or override via allocation_overrides.csv.
# ---------------------------------------------------------------------------

NIFTY50_TICKERS: frozenset[str] = frozenset({
    "RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK", "HINDUNILVR",
    "LTIM", "SBIN", "BHARTIARTL", "KOTAKBANK", "AXISBANK", "LT",
    "ITC", "BAJFINANCE", "HCLTECH", "MARUTI", "SUNPHARMA", "TITAN",
    "NTPC", "TATAMOTORS", "POWERGRID", "ULTRACEMCO", "ONGC", "ADANIENT",
    "WIPRO", "NESTLEIND", "JSWSTEEL", "TECHM", "COALINDIA", "INDUSINDBK",
    "BAJAJFINSV", "TATACONSUM", "HINDALCO", "GRASIM", "BPCL", "CIPLA",
    "APOLLOHOSP", "ADANIPORTS", "DRREDDY", "DIVISLAB", "SBILIFE",
    "HDFCLIFE", "SHREECEM", "EICHERMOT", "HEROMOTOCO", "TATASTEEL",
    "BRITANNIA", "UPL", "BAJAJ-AUTO", "MM",
})

NIFTY_MIDCAP150_TICKERS: frozenset[str] = frozenset({
    "ABCAPITAL", "ABFRL", "ABIRLANUVO", "ACC", "APLAPOLLO", "ASTRAL",
    "AUROPHARMA", "BAJAJHFL", "BANKBARODA", "BATAINDIA", "BERGEPAINT",
    "BIOCON", "BOSCHLTD", "CANBK", "CHOLAFIN", "COLPAL", "CONCOR",
    "CUMMINSIND", "DABUR", "DALBHARAT", "DEEPAKFERT", "DELHIVERY",
    "DIXON", "FEDERALBNK", "FORTIS", "GLENMARK", "GMRINFRA", "GODREJCP",
    "GODREJIND", "GODREJPROP", "HAVELLS", "HFCL", "HINDPETRO", "IDFC",
    "IDFCFIRSTB", "INDHOTEL", "INDUSTOWER", "KPITTECH", "LALPATHLAB",
    "LICHSGFIN", "LTTS", "LUPIN", "MFSL", "MPHASIS", "MUTHOOTFIN",
    "NATIONALUM", "NMDC", "OBEROIRLTY", "OFSS", "PAGEIND", "PERSISTENT",
    "PETRONET", "PIIND", "POLYCAB", "PRINCEPIPE", "PVR INOX", "RAMCOCEM",
    "RECLTD", "SBICARD", "SIEMENS", "SUPREMEIND", "TATACOMM", "TATACHEM",
    "TATAELXSI", "TATAPOWER", "TORNTPHARM", "TORNTPOWER", "TRENT",
    "IPCALAB", "VOLTAS", "WHIRLPOOL", "YESBANK", "ZYDUSLIFE",
    "CRISIL", "ESCORTS", "EXIDEIND", "ICICIGI", "ICICIPRULI",
})


def _classify_stock(ticker: str, name: str) -> str:
    """Return 'large_cap', 'mid_cap', or 'small_cap' for a direct stock holding."""
    base = ticker.upper().replace(".NS", "").replace(".BO", "")
    if base in NIFTY50_TICKERS:
        return "large_cap"
    if base in NIFTY_MIDCAP150_TICKERS:
        return "mid_cap"
    return "small_cap"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def compute_equity_lookthrough(
    val_path: str = "data/output/master_valuation.csv",
    map_path: str = "data/input/asset_allocation_map.csv",
    override_path: str = "data/input/allocation_overrides.csv",
    out_path: str = "data/output/equity_lookthrough.csv",
) -> None:
    """
    Compute per-owner equity look-through (cap-size + geography).
    Writes equity_lookthrough.csv.
    """
    logger.info("COMPUTING EQUITY LOOK-THROUGH")

    if not Path(val_path).exists():
        logger.error("%s not found. Run the pipeline first.", val_path)
        return

    df_val = pd.read_csv(val_path)
    df_val["Current Value"] = pd.to_numeric(df_val["Current Value"], errors="coerce").fillna(0)

    # Load allocation map
    alloc_map: dict[str, dict] = {}
    if Path(map_path).exists():
        try:
            for _, row in pd.read_csv(map_path).iterrows():
                alloc_map[str(row["Ticker"])] = row.to_dict()
        except Exception as e:
            logger.warning("Could not load allocation map: %s", e)

    # Apply overrides on top
    if Path(override_path).exists():
        try:
            for _, row in pd.read_csv(override_path).iterrows():
                if pd.notna(row.get("Ticker")):
                    alloc_map[str(row["Ticker"])] = row.to_dict()
        except Exception as e:
            logger.warning("Could not load overrides: %s", e)

    owners = df_val["Portfolio Owner"].unique()
    result_rows: list[dict] = []

    for owner in owners:
        df_owner = df_val[df_val["Portfolio Owner"] == owner]

        totals = dict(
            large_cap=0.0, mid_cap=0.0, small_cap=0.0,
            domestic_eq=0.0, intl_eq=0.0,
            geo_eq_total=0.0,  # independent denominator for geo %
        )

        # Track top international contributors for the AI tool
        intl_contributors: list[dict] = []

        for _, asset in df_owner.iterrows():
            ticker = str(asset.get("Ticker", "")).strip()
            name   = str(asset.get("Asset Name", ""))
            aclass = str(asset.get("Asset Class", "")).upper()
            val    = float(asset.get("Current Value", 0.0))

            if val <= 0:
                continue

            # Only process equity-bearing assets
            if aclass not in ["MUTUAL FUND", "STOCK", "NPS"]:
                continue

            if aclass == "STOCK":
                # Direct stock classification
                cap_bucket = _classify_stock(ticker, name)
                if "GOLD" in ticker.upper() or "SGB" in ticker.upper():
                    continue  # Gold instruments — not equity
                totals[cap_bucket] += val
                totals["domestic_eq"] += val   # Stocks are domestic India by default
                totals["geo_eq_total"] += val
                continue

            if aclass == "NPS":
                # NPS Scheme E is domestic large-cap-heavy equity
                if "Scheme E" in name or "Equity" in name:
                    totals["large_cap"] += val * 0.75
                    totals["mid_cap"]   += val * 0.25
                    totals["domestic_eq"] += val
                    totals["geo_eq_total"] += val
                continue  # Scheme C/G are debt — skip

            # Mutual Fund — look up allocation map
            map_row = alloc_map.get(ticker)
            if not map_row:
                logger.debug("No map entry for %s (%s) — skipping from lookthrough", name, ticker)
                continue

            def _pct(key: str) -> float:
                try:
                    v = map_row.get(key, 0.0)
                    return float(v) / 100.0 if v and str(v) not in ("", "nan") else 0.0
                except Exception:
                    return 0.0

            # Equity portion of the fund's total value
            eq_india_pct = _pct("Equity_India_Pct")
            eq_for_pct   = _pct("Equity_Foreign_Pct")
            total_eq_pct = eq_india_pct + eq_for_pct

            if total_eq_pct == 0:
                continue  # Pure debt/gold fund — skip

            eq_value = val * total_eq_pct

            # Cap-size split (applied on the equity portion)
            lc_pct = _pct("LargeCap_Pct")
            mc_pct = _pct("MidCap_Pct")
            sc_pct = _pct("SmallCap_Pct")

            # Normalise cap pcts to sum to 1
            cap_total = lc_pct + mc_pct + sc_pct
            if cap_total > 0:
                lc_pct, mc_pct, sc_pct = lc_pct / cap_total, mc_pct / cap_total, sc_pct / cap_total
            else:
                # Cap columns missing (old CSV format) — apply heuristics from fund name inline
                name_lower = name.lower()
                if any(k in name_lower for k in ["large cap", "bluechip", "nifty 50", "index", "nifty50"]):
                    lc_pct, mc_pct, sc_pct = 0.80, 0.15, 0.05
                elif any(k in name_lower for k in ["large & mid", "large and mid"]):
                    lc_pct, mc_pct, sc_pct = 0.50, 0.45, 0.05
                elif any(k in name_lower for k in ["mid cap", "midcap"]):
                    lc_pct, mc_pct, sc_pct = 0.10, 0.75, 0.15
                elif any(k in name_lower for k in ["small cap", "smallcap"]):
                    lc_pct, mc_pct, sc_pct = 0.05, 0.15, 0.80
                elif any(k in name_lower for k in ["international", "global", "world", "overseas", "u.s.", "nasdaq", "fof"]):
                    lc_pct, mc_pct, sc_pct = 0.0, 0.0, 0.0  # no cap split for foreign funds
                else:
                    # generic flexi/multi/elss fallback
                    lc_pct, mc_pct, sc_pct = 0.45, 0.35, 0.20


            totals["large_cap"] += eq_value * lc_pct
            totals["mid_cap"]   += eq_value * mc_pct
            totals["small_cap"] += eq_value * sc_pct

            # Geography split (applied on the equity portion)
            dom_pct  = _pct("Domestic_Eq_Pct")
            intl_pct = _pct("Intl_Eq_Pct")

            # If geography data is missing, infer from equity_india vs equity_foreign
            if dom_pct == 0 and intl_pct == 0:
                dom_frac  = eq_india_pct / total_eq_pct if total_eq_pct > 0 else 1.0
                intl_frac = eq_for_pct  / total_eq_pct if total_eq_pct > 0 else 0.0
            else:
                geo_total = dom_pct + intl_pct
                dom_frac  = dom_pct  / geo_total if geo_total > 0 else 1.0
                intl_frac = intl_pct / geo_total if geo_total > 0 else 0.0

            totals["domestic_eq"] += eq_value * dom_frac
            intl_value = eq_value * intl_frac
            totals["intl_eq"]    += intl_value
            totals["geo_eq_total"] += eq_value  # always track geo denominator

            if intl_value > 1000:
                intl_contributors.append({
                    "fund": name,
                    "intl_value": round(intl_value, 2),
                    "intl_pct_of_fund": round(intl_frac * 100, 1),
                })

        total_eq  = totals["large_cap"] + totals["mid_cap"] + totals["small_cap"]
        total_geo = totals["geo_eq_total"] or total_eq  # separate geo denominator

        def _cap_pct(v: float) -> float:
            return round(v / total_eq * 100, 2) if total_eq > 0 else 0.0

        def _geo_pct(v: float) -> float:
            return round(v / total_geo * 100, 2) if total_geo > 0 else 0.0

        result_rows.append({
            "Owner":                  owner,
            "LargeCap_Value":         round(totals["large_cap"], 2),
            "MidCap_Value":           round(totals["mid_cap"], 2),
            "SmallCap_Value":         round(totals["small_cap"], 2),
            "Domestic_Eq_Value":      round(totals["domestic_eq"], 2),
            "International_Eq_Value": round(totals["intl_eq"], 2),
            "Total_Equity_Value":     round(total_eq, 2),
            "LargeCap_Pct":           _cap_pct(totals["large_cap"]),
            "MidCap_Pct":             _cap_pct(totals["mid_cap"]),
            "SmallCap_Pct":           _cap_pct(totals["small_cap"]),
            "Domestic_Pct":           _geo_pct(totals["domestic_eq"]),
            "Intl_Pct":               _geo_pct(totals["intl_eq"]),
            "as_of_date":             datetime.now().strftime("%d %b %Y"),
        })

        logger.info(
            "  %-10s | Equity: Rs%s | Large: %.1f%% | Mid: %.1f%% | Small: %.1f%% | Intl: %.1f%%",
            owner, f"{total_eq:,.0f}",
            _cap_pct(totals["large_cap"]),
            _cap_pct(totals["mid_cap"]),
            _cap_pct(totals["small_cap"]),
            _geo_pct(totals["intl_eq"]),
        )

    if result_rows:
        df_out = pd.DataFrame(result_rows)
        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
        df_out.to_csv(out_path, index=False)
        logger.info("Equity look-through saved to %s", out_path)
    else:
        logger.warning("No equity look-through data computed (check valuation and allocation map).")


if __name__ == "__main__":
    import logging as _logging
    _logging.basicConfig(
        level=_logging.INFO,
        format="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    compute_equity_lookthrough()
