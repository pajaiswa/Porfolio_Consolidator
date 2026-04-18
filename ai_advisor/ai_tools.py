import json
import os

import pandas as pd
import yfinance as yf
from crewai.tools import tool

@tool("Get Portfolio Holdings")
def get_portfolio_holdings(owner_name: str) -> str:
    """Returns the current portfolio holdings, invested amount, and current value for the given owner.
    Use this to see exactly what mutual funds, stocks, and assets the user owns.
    If investigating the entire family, pass 'All'.
    """
    try:
        val = pd.read_csv('data/output/master_valuation.csv')
        if owner_name and owner_name.lower() != "all":
            val = val[val['Portfolio Owner'] == owner_name]
        
        # Summarize by Asset Name
        summary = val.groupby(['Asset Class', 'Asset Name']).agg({
            'Units': 'sum',
            'Current Value': 'sum'
        }).reset_index()
        
        summary['Units'] = summary['Units'].round(3)
        total_value = float(val['Current Value'].sum())
        records = summary.to_dict(orient='records')
        for r in records:
            r['Current Value'] = round(float(r['Current Value']), 2) if pd.notnull(r['Current Value']) else 0.0
            r['Units'] = round(float(r['Units']), 3)

        return json.dumps({
            "owner": owner_name,
            "total_current_value": round(total_value, 2),
            "holdings": records
        }, indent=2)
    except Exception as e:
        return f"Error reading portfolio: {e}"

@tool("Get Asset Allocation")
def get_asset_allocation(owner_name: str) -> str:
    """Returns the look-through asset allocation (Equity, Debt, Gold, Cash) for the given owner.
    Use this to understand their risk exposure and sector concentration.
    If investigating the entire family, pass 'All'.
    """
    try:
        alloc = pd.read_csv('data/output/asset_allocation_drilldown.csv')
        if owner_name and owner_name.lower() != "all":
            alloc = alloc[alloc['Owner'] == owner_name]
            
        summary = alloc.groupby('Sub Class')['Value'].sum().reset_index()
        total = summary['Value'].sum()
        records = summary.to_dict(orient='records')
        for r in records:
            r['Value'] = round(float(r['Value']), 2)
            r['Percentage'] = round(float(r['Value']) / float(total) * 100, 2) if total > 0 else 0.0

        return json.dumps({
            "owner": owner_name,
            "total_value": round(float(total), 2),
            "allocation": records
        }, indent=2)
    except Exception as e:
        return f"Error reading allocation: {e}"

@tool("Get Stock Fundamentals")
def get_stock_fundamentals(ticker: str) -> str:
    """Returns fundamental analysis data for a given Indian stock ticker using yfinance.
    Always pass the ticker with '.NS' or '.BO' suffix (e.g., 'TCS.NS' or 'RELIANCE.NS').
    """
    try:
        if not ticker.endswith('.NS') and not ticker.endswith('.BO'):
            ticker += '.NS'
        stock = yf.Ticker(ticker)
        info = stock.info
        
        # Guard against missing data
        if 'regularMarketPrice' not in info and 'previousClose' not in info:
            return f"No fundamental data found for ticker {ticker}."
            
        details = [
            f"Sector: {info.get('sector', 'N/A')}",
            f"Industry: {info.get('industry', 'N/A')}",
            f"P/E Ratio (Trailing): {info.get('trailingPE', 'N/A')}",
            f"P/E Ratio (Forward): {info.get('forwardPE', 'N/A')}",
            f"Price to Book (P/B): {info.get('priceToBook', 'N/A')}",
            f"52-Week High: ₹{info.get('fiftyTwoWeekHigh', 'N/A')}",
            f"52-Week Low: ₹{info.get('fiftyTwoWeekLow', 'N/A')}",
            f"Dividend Yield: {info.get('dividendYield', 'N/A')}",
            f"Analyst Recommendation: {info.get('recommendationKey', 'N/A')}"
        ]
        return "\n".join(details)
    except Exception as e:
        return f"Could not fetch fundamentals for {ticker}: {e}"


@tool("Get Equity Look-Through Analysis")
def get_equity_lookthrough(owner_name: str) -> str:
    """Returns the true equity look-through analysis: market-cap breakdown (Large/Mid/Small Cap)
    and geographic breakdown (Domestic vs International) for the given owner.
    This is computed by decomposing each mutual fund's actual holdings using the allocation map.
    Use this tool to understand the real cap-size exposure and international diversification.
    If investigating the entire family, pass 'All'.
    """
    try:
        lt_path = "data/output/equity_lookthrough.csv"
        if not pd.read_csv.__module__:  # imports check
            pass
        lt = pd.read_csv(lt_path)

        if owner_name and owner_name.lower() != "all":
            lt = lt[lt["Owner"] == owner_name]

        if lt.empty:
            return json.dumps({
                "error": f"No equity look-through data found for {owner_name}. "
                         "Run the pipeline (including compute_equity_lookthrough.py) first."
            })

        # Aggregate across owners if "All" requested
        numeric_cols = [
            "LargeCap_Value", "MidCap_Value", "SmallCap_Value",
            "Domestic_Eq_Value", "International_Eq_Value", "Total_Equity_Value"
        ]
        agg = lt[numeric_cols].sum()
        total_eq = float(agg["Total_Equity_Value"])

        def _pct(v: float) -> float:
            return round(v / total_eq * 100, 2) if total_eq > 0 else 0.0

        # Top international contributors (per-owner breakdown if available)
        intl_by_fund: list[dict] = []
        try:
            alloc = pd.read_csv("data/input/asset_allocation_map.csv")
            val   = pd.read_csv("data/output/master_valuation.csv")
            if owner_name and owner_name.lower() != "all":
                val = val[val["Portfolio Owner"] == owner_name]
            mf = val[val["Asset Class"].str.upper() == "MUTUAL FUND"]
            for _, row in mf.iterrows():
                ticker = str(row.get("Ticker", ""))
                m = alloc[alloc["Ticker"].astype(str) == ticker]
                if m.empty:
                    continue
                ar = m.iloc[0]
                intl_pct = float(ar.get("Intl_Eq_Pct", 0) or 0) / 100.0
                eq_pct   = (float(ar.get("Equity_Foreign_Pct", 0) or 0)) / 100.0
                if intl_pct == 0 and eq_pct > 0:
                    intl_pct = eq_pct
                cur_val  = float(row.get("Current Value", 0) or 0)
                intl_val = cur_val * intl_pct
                if intl_val > 1000:
                    intl_by_fund.append({
                        "fund": row.get("Asset Name", ticker),
                        "intl_value": round(intl_val, 2),
                        "intl_pct_of_fund": round(intl_pct * 100, 1),
                    })
            intl_by_fund.sort(key=lambda x: x["intl_value"], reverse=True)
        except Exception:
            pass

        return json.dumps({
            "owner": owner_name,
            "data_as_of": lt["as_of_date"].iloc[0] if "as_of_date" in lt.columns else "unknown",
            "total_equity_value": round(total_eq, 2),
            "cap_size_split": {
                "large_cap": {"value": round(float(agg["LargeCap_Value"]), 2), "pct": _pct(float(agg["LargeCap_Value"]))},
                "mid_cap":   {"value": round(float(agg["MidCap_Value"]),   2), "pct": _pct(float(agg["MidCap_Value"]))},
                "small_cap": {"value": round(float(agg["SmallCap_Value"]), 2), "pct": _pct(float(agg["SmallCap_Value"]))},
            },
            "geography_split": {
                "domestic":      {"value": round(float(agg["Domestic_Eq_Value"]),      2), "pct": _pct(float(agg["Domestic_Eq_Value"]))},
                "international": {"value": round(float(agg["International_Eq_Value"]), 2), "pct": _pct(float(agg["International_Eq_Value"]))},
            },
            "top_international_contributors": intl_by_fund[:5],
        }, indent=2)
    except FileNotFoundError:
        return json.dumps({
            "error": "equity_lookthrough.csv not found. Run the pipeline first."
        })
    except Exception as e:
        return f"Error reading equity look-through data: {e}"

