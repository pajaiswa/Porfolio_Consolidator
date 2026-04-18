"""
fetch_nps_navs.py — Live NPS NAV Fetcher
==========================================
Fetches the latest NAVs for NPS schemes from the npsnav.in public JSON API
and saves them to data/output/nps_latest_navs.json.

This replaces the previous approach of parsing NAVs from the PDF statement,
which produced stale values whenever the PDF was old.

API: https://npsnav.in/api/detailed/<scheme_code>
     https://npsnav.in/api/latest  (full dump for all schemes)

Inputs  : (none — uses the API directly)
Outputs : data/output/nps_latest_navs.json  (used by valuate_mf_nps.py)

Scheme code reference (HDFC Pension Fund — Tier I):
  SM008001 → NPS - Scheme E (Equity)
  SM008002 → NPS - Scheme C (Corp Debt)
  SM008003 → NPS - Scheme G (Govt Debt)
  SM008008 → NPS - Scheme A (Alternative Inv) / Equity Advantage Fund post-merger
"""
import json
import logging
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Scheme code map: our internal name → npsnav.in scheme code (HDFC Tier I)
# ---------------------------------------------------------------------------
HDFC_SCHEME_CODES: dict[str, str] = {
    "NPS - Scheme E (Equity)":      "SM008001",  # HDFC Scheme E Tier I (Equity)
    "NPS - Scheme C (Corp Debt)":   "SM008002",  # HDFC Scheme C Tier I (Corp Debt)
    "NPS - Scheme G (Govt Debt)":   "SM008003",  # HDFC Scheme G Tier I (Govt Debt)
    "NPS - Equity Advantage Fund":  "SM008013",  # HDFC NPS Equity Advantage Fund Tier I
    # NOTE: Scheme A (SM008008) is intentionally excluded — it was discontinued
    # and its historical cash flows were transferred into Scheme C (Corp Debt).
    # The merger adjustment is handled in pipeline/ingest_nps.py.
}

BASE_URL = "https://npsnav.in/api/detailed/{code}"
OUTPUT_FILE = Path("data/output/nps_latest_navs.json")
REQUEST_TIMEOUT = 10  # seconds


def fetch_live_nps_navs(
    scheme_codes: dict[str, str] | None = None,
    output_path: str | None = None,
) -> dict[str, dict]:
    """
    Fetches live NAVs for each NPS scheme from npsnav.in.

    Args:
        scheme_codes: Mapping of internal scheme name → npsnav.in scheme code.
                      Defaults to HDFC_SCHEME_CODES.
        output_path:  Path to write the JSON cache.  Defaults to OUTPUT_FILE.

    Returns:
        Dict of { internal_scheme_name: {"nav": float, "date": str} }.
        Falls back to the existing cache value (or sentinel) on any error.
    """
    if scheme_codes is None:
        scheme_codes = HDFC_SCHEME_CODES

    out = Path(output_path) if output_path else OUTPUT_FILE

    # Load existing cache for fallback
    existing: dict[str, dict] = {}
    if out.exists():
        try:
            with open(out, encoding="utf-8") as f:
                existing = json.load(f)
        except Exception:
            pass

    navs: dict[str, dict] = {}

    for scheme_name, code in scheme_codes.items():
        url = BASE_URL.format(code=code)
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            nav = float(data["NAV"])
            last_updated = data.get("Last Updated", "unknown")
            navs[scheme_name] = {"nav": nav, "date": last_updated}
            logger.info("%-35s NAV: ₹%.4f (as of %s)", scheme_name, nav, last_updated)
        except Exception as e:
            fallback = existing.get(scheme_name, {"nav": 10.0, "date": "unknown"})
            if isinstance(fallback, (int, float)):
                fallback = {"nav": float(fallback), "date": "unknown"}
            navs[scheme_name] = fallback
            logger.warning("Failed to fetch %s (%s): %s. Using fallback: %s",
                           scheme_name, code, e, fallback)

    # Always write the cache so valuate_mf_nps.py can read it
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(navs, f, indent=2)

    logger.info("NPS NAV cache updated: %s", out)
    return navs


def main() -> None:
    logger.info("Fetching live NPS NAVs...")
    navs = fetch_live_nps_navs()
    logger.info("Final NAVs: %s", navs)


if __name__ == "__main__":
    main()
