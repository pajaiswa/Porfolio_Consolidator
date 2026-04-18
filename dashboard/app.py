import sys
from pathlib import Path
from dotenv import load_dotenv

# Ensure the project root is on sys.path so all packages resolve correctly
# whether Streamlit is launched from the root or from inside dashboard/
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# Load .env from project root — makes CAMS_PASSWORD, NPS_PASSWORD,
# GEMINI_API_KEY etc. available regardless of how the app is launched.
load_dotenv(_PROJECT_ROOT / ".env")

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pyxirr import xirr
from datetime import datetime
from streamlit_extras.metric_cards import style_metric_cards

st.set_page_config(page_title="Family Wealth Dashboard", layout="wide", page_icon="📈")

# -------------------------------------------------------------------
# 0. THEME OVERRIDES
# -------------------------------------------------------------------
# Style the metric cards to look like modern Fintech stat boards
style_metric_cards(
    background_color="#1e2329",
    border_size_px=1,
    border_color="#2c3e50",
    border_radius_px=10,
    border_left_color="#10b981", # Emerald green accent
    box_shadow=False
)

# -------------------------------------------------------------------
# 1. LOAD DATA
# -------------------------------------------------------------------
import os as _os


def _val_mtime() -> float:
    """Return master_valuation.csv modification time for cache-busting."""
    p = 'data/output/master_valuation.csv'
    return _os.path.getmtime(p) if _os.path.exists(p) else 0.0


@st.cache_data
def load_data(mtime: float = 0.0):
    """Load all pipeline outputs. Cache is invalidated when master_valuation.csv changes."""
    ledger = pd.read_csv('data/output/master_ledger.csv')
    val = pd.read_csv('data/output/master_valuation.csv')

    # Clean Ticker mappings
    ledger['Ticker'] = ledger['Ticker'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
    val['Ticker'] = val['Ticker'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)

    from analytics.calculate_xirr import normalize_cash_flows
    ledger = normalize_cash_flows(ledger)

    alloc_summary = pd.DataFrame()
    alloc_drill = pd.DataFrame()
    if _os.path.exists('data/output/asset_allocation.csv'):
        alloc_summary = pd.read_csv('data/output/asset_allocation.csv')
    if _os.path.exists('data/output/asset_allocation_drilldown.csv'):
        alloc_drill = pd.read_csv('data/output/asset_allocation_drilldown.csv')

    return ledger, val, alloc_summary, alloc_drill


def _get_as_of_dates(val_df: pd.DataFrame) -> dict[str, str]:
    """
    Extract the latest Value Date for MF, STOCK, and NPS from master_valuation.
    Returns a dict like {'MF': '06-Mar-2026', 'Stocks': '06-Mar-2026', 'NPS': '06-Mar-2026'}.
    """
    result = {}
    cls_map = {
        'MF NAV':  ['Mutual Fund'],
        'Stocks':  ['STOCK'],
        'NPS':     ['NPS'],
    }
    if 'Value Date' not in val_df.columns:
        return result
    for label, classes in cls_map.items():
        subset = val_df[val_df['Asset Class'].isin(classes)]['Value Date'].dropna()
        subset = subset[subset.astype(str).str.strip().str.lower() != 'unknown']
        if not subset.empty:
            # Pick the most-recent date string across all rows
            try:
                latest = subset.astype(str).apply(
                    lambda d: pd.to_datetime(d, dayfirst=True, errors='coerce')
                ).max()
                result[label] = latest.strftime('%d-%b-%Y') if pd.notna(latest) else subset.iloc[0]
            except Exception:
                result[label] = str(subset.iloc[0])
    return result


df_ledger, df_val, df_alloc_sum, df_alloc_drill = load_data(mtime=_val_mtime())

# -------------------------------------------------------------------
# 2. XIRR CALCULATION ENGINE
# -------------------------------------------------------------------
def calculate_dynamic_xirr(ledger_subset, val_subset):
    """Calculates Invested, Current Value, and XIRR for dynamic slices of the portfolio."""
    if ledger_subset.empty:
        return 0.0, 0.0, 0.0, 0.0
        
    cf_daily = ledger_subset.groupby('Date')['Net_Cash_Flow'].sum().reset_index()
    cf_daily = cf_daily[cf_daily['Net_Cash_Flow'] != 0]
    
    if cf_daily.empty:
        return 0.0, 0.0, 0.0, 0.0
        
    from analytics.calculate_xirr import calculate_fifo_invested
    invested = calculate_fifo_invested(ledger_subset)
    
    current_val = val_subset['Current Value'].sum() if not val_subset.empty else 0.0
    
    # Terminal Value
    if current_val > 0:
        terminal_row = pd.DataFrame({'Date': [datetime.now()], 'Net_Cash_Flow': [current_val]})
        xirr_data = pd.concat([cf_daily, terminal_row], ignore_index=True)
    else:
        xirr_data = cf_daily.copy()
        
    xirr_data = xirr_data.groupby('Date')['Net_Cash_Flow'].sum().reset_index()
    dates = xirr_data['Date'].tolist()
    amounts = xirr_data['Net_Cash_Flow'].tolist()
    
    xirr_pct = 0.0
    if amounts and any(a < 0 for a in amounts) and any(a > 0 for a in amounts):
        try:
            val = xirr(dates, amounts)
            if val is not None:
                xirr_pct = val * 100
        except Exception:
            pass
            
    abs_return = current_val - invested
    
    return invested, current_val, abs_return, xirr_pct

# -------------------------------------------------------------------
# 3. SIDEBAR NAVIGATION & PRIVACY
# -------------------------------------------------------------------
with st.sidebar:
    st.image("https://cdn-icons-png.flaticon.com/512/3135/3135715.png", width=80)
    st.title("Wealth Manager")
    
    app_mode = st.radio("App Mode", ["Dashboard", "Data Management", "🤖 AI Advisor"], index=0)
    
    if app_mode == "Dashboard":
        show_values = st.toggle("👁️ Show Amounts", value=False)
        st.markdown("---")
        
        st.markdown("Global Portfolio Filters")
        
        owners = df_ledger['Portfolio Owner'].unique().tolist()
        asset_classes = df_ledger['Asset Class'].unique().tolist()
        
        selected_owners = st.multiselect("👥 Portfolio Owners", owners, default=owners)
        selected_assets = st.multiselect("📊 Asset Classes", asset_classes, default=asset_classes)
        
        st.markdown("---")
        _ao = _get_as_of_dates(df_val)
        for _lbl, _dt in _ao.items():
            st.caption(f"{_lbl}: {_dt}")
        
        # Placeholder allowing export button to reference figures created later in the script
        export_container = st.sidebar.container()

def fmt_amt(amount):
    """Formats amount as currency or masks it based on privacy toggle."""
    if not show_values:
        return r"\*\*\*"
    try:
        is_neg = amount < 0
        val = str(abs(round(float(amount))))
        if len(val) > 3:
            head = val[:-3]
            tail = val[-3:]
            head = ",".join([head[max(0, i-2):i] for i in range(len(head), 0, -2)][::-1])
            res = f"{head},{tail}"
        else:
            res = val
        res = f"₹ {res}"
        return f"-{res}" if is_neg else res
    except:
        return f"₹ {amount:,.0f}"

def fmt_pct(pct):
    return f"{pct:,.2f}%"

if app_mode == "Data Management":
    from dashboard.ui_data_management import render_data_management
    owners = df_ledger['Portfolio Owner'].unique().tolist() if not df_ledger.empty else []
    render_data_management(owners)
    st.stop()

if app_mode == "🤖 AI Advisor":
    from ai_advisor.ui import render_ai_advisor
    owners = df_ledger['Portfolio Owner'].unique().tolist() if not df_ledger.empty else []
    render_ai_advisor(owners)
    st.stop()
    
# -------------------------------------------------------------------
# 4. FILTERING LOGIC (DASHBOARD ONLY)
# -------------------------------------------------------------------
filtered_ledger = df_ledger[df_ledger['Portfolio Owner'].isin(selected_owners) & df_ledger['Asset Class'].isin(selected_assets)]
filtered_val = df_val[df_val['Portfolio Owner'].isin(selected_owners) & df_val['Asset Class'].isin(selected_assets)]

st.title("📈 Family Wealth Dashboard")

# ─── As-Of Date Status Bar ───────────────────────────────────────────────────
_as_of = _get_as_of_dates(df_val)
if _as_of:
    _badges = "  &nbsp;|&nbsp;  ".join(
        f"<span style='color:#10b981;font-weight:600;'>{lbl}</span>&nbsp;"
        f"<code style='background:#1e2329;padding:1px 6px;border-radius:4px;font-size:0.8rem;color:#93c5fd;'>{dt}</code>"
        for lbl, dt in _as_of.items()
    )
    st.markdown(
        f"<p style='color:#a0aec0;margin-top:-0.6rem;margin-bottom:1.4rem;font-size:0.875rem;'>"
        f"📅 &nbsp;Price dates:&nbsp;&nbsp;{_badges}</p>",
        unsafe_allow_html=True,
    )
else:
    st.markdown(
        "<p style='color:#a0aec0;margin-bottom:2rem;'>Interactive visualizations built on real-time market data.</p>",
        unsafe_allow_html=True,
    )

# -------------------------------------------------------------------
# 5. TABS LAYOUT
# -------------------------------------------------------------------
tab_overview, tab_allocation, tab_portfolios, tab_retirement = st.tabs(["🌎 Consolidated Overview", "🥧 True Asset Allocation", "👤 Individual Portfolios", "👴 Retirement Portfolios"])

with tab_allocation:
    st.subheader("Granular Asset Allocation (Look-through)")
    st.markdown("This view breaks down mutual funds into their underlying **Equity**, **Debt**, **Gold**, and **Cash** exposures based on live scraping, combined with direct stocks and deposits.", unsafe_allow_html=True)
    
    if df_alloc_sum.empty or df_alloc_drill.empty:
        st.info("⚠️ Allocation data missing. Please run `run_all.py` to generate the latest look-through data.")
    else:
        # Filter by selected owners
        if selected_owners:
            f_drill = df_alloc_drill[df_alloc_drill['Owner'].isin(selected_owners)]
            f_sum = f_drill.groupby('Sub Class')['Value'].sum().reset_index()
            f_sum.rename(columns={'Value': 'Total Value'}, inplace=True)
        else:
            f_sum = df_alloc_sum.copy()
            f_drill = df_alloc_drill.copy()
            
        alloc_col1, alloc_col2 = st.columns([1, 1.5])
        
        with alloc_col1:
            if not f_sum.empty:
                # Custom color mapping for deep financial aesthetic
                color_map = {
                    'Equity_India': '#10b981',   # Emerald
                    'Equity_Foreign': '#3b82f6', # Blue
                    'Debt': '#8b5cf6',           # Purple
                    'Gold': '#f59e0b',           # Amber
                    'Cash': '#64748b'            # Slate
                }
                
                fig_alloc = px.pie(
                    f_sum,
                    values='Total Value',
                    names='Sub Class',
                    hole=0.65,
                    color='Sub Class',
                    color_discrete_map=color_map,
                    title="True Net Worth Exposure"
                )
                
                # Mask visuals inside pie chart if privacy is on
                t_info = 'percent+label'
                h_temp_pie = "<b>%{label}</b><br>Value: Rs. %{value}<br>Percentage: %{percent}<extra></extra>" if show_values else "<b>%{label}</b><br>Value: ***<br>Percentage: %{percent}<extra></extra>"
                
                fig_alloc.update_traces(
                    textposition='inside', 
                    textinfo=t_info, 
                    hovertemplate=h_temp_pie,
                    marker=dict(line=dict(color='#0e1117', width=2))
                )
                fig_alloc.update_layout(
                    paper_bgcolor='rgba(0,0,0,0)', 
                    plot_bgcolor='rgba(0,0,0,0)', 
                    margin=dict(t=40, b=20, l=20, r=20),
                    showlegend=False
                )
                st.plotly_chart(fig_alloc, use_container_width=True)
                
                # Render clean metric KPIs below pie chart
                tot_val = f_sum['Total Value'].sum()
                if tot_val > 0:
                    st.markdown("### Total Allocation Values")
                    for _, row in f_sum.sort_values(by="Total Value", ascending=False).iterrows():
                        pct = (row['Total Value'] / tot_val) * 100
                        val_str = fmt_amt(row['Total Value'])
                        pct_str = f"{pct:.1f}%"
                        st.markdown(f"**{row['Sub Class']}**: {val_str}  `{pct_str}`")

        with alloc_col2:
            st.markdown("### Detailed Component Drill-Down")
            
            # Interactive Sunburst Chart to visually show logic
            fig_sun = px.sunburst(
                f_drill, 
                path=['Sub Class', 'Asset Class', 'Asset Name'], 
                values='Value',
                title="Hierarchical Wealth Breakdown",
                color='Sub Class',
                color_discrete_map=color_map
            )
            fig_sun.update_layout(
                paper_bgcolor='rgba(0,0,0,0)', 
                plot_bgcolor='rgba(0,0,0,0)', 
                margin=dict(t=40, b=20, l=10, r=10)
            )
            # Remove giant labels for neatness inside sunburst, rely on hover
            h_temp_sun = '<b>%{label}</b><br>Value: Rs. %{value}<br>Parent %: %{percentParent:.1%}<extra></extra>' if show_values else '<b>%{label}</b><br>Value: ***<br>Parent %: %{percentParent:.1%}<extra></extra>'
            fig_sun.update_traces(
                textinfo="label+percent parent",
                insidetextorientation='radial',
                hovertemplate=h_temp_sun
            )
            # To fix crowded tiny wedges: Hide text for segments smaller than a certain percentage if possible, 
            # Plotly does this automatically but we can force it cleaner:
            fig_sun.update_layout(uniformtext=dict(minsize=9, mode='hide'))
            st.plotly_chart(fig_sun, use_container_width=True)

with tab_overview:
    # -------------------------------------------------------------------
    # EXECUTIVE SUMMARY (KPIs)
    # -------------------------------------------------------------------
    inv, cur, ret, x_pct = calculate_dynamic_xirr(filtered_ledger, filtered_val)
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Invested (FIFO)", fmt_amt(inv))
    
    delta_cur = f"₹ {ret:,.0f}" if show_values else r"\*\*\*"
    col2.metric("Current Value", fmt_amt(cur), delta_cur)
    
    abs_pct = (ret / inv * 100) if inv > 0 else 0
    col3.metric("Absolute Return", fmt_pct(abs_pct), fmt_pct(abs_pct))
    col4.metric("Annualized XIRR", fmt_pct(x_pct), fmt_pct(x_pct))
    
    # -------------------------------------------------------------------
    # EXTRA KPIs (Best Performer & Dominant Class)
    # -------------------------------------------------------------------
    best_asset_name = "N/A"
    best_ret_pct = 0.0
    dominant_class = "N/A"
    dominant_pct = 0.0
    
    if not filtered_val.empty and not filtered_ledger.empty and cur > 0:
        # Dominant Asset Class
        cls_grp = filtered_val.groupby('Asset Class')['Current Value'].sum()
        dominant_class = cls_grp.idxmax()
        dominant_pct = (cls_grp.max() / cur) * 100
        
        # Best Performer (Absolute %)
        # Fastest way without re-importing is to group ledger by Ticker to find net cash flows, 
        # but since fifo is complex, we just loop the calculation wrapper
        for t in filtered_val['Ticker'].unique():
            t_val = filtered_val[filtered_val['Ticker'] == t]
            t_ledger = filtered_ledger[filtered_ledger['Ticker'] == t]
            if not t_ledger.empty:
                t_inv, t_cur, t_ret, _ = calculate_dynamic_xirr(t_ledger, t_val)
                if t_inv > 0:
                    pct = (t_cur - t_inv) / t_inv * 100
                    if pct > best_ret_pct:
                        best_ret_pct = pct
                        best_asset_name = t_val.iloc[0]['Asset Name']
                        
    col5, col6 = st.columns(2)
    # Truncate length so it fits neatly
    short_best = best_asset_name[:30] + '...' if len(best_asset_name) > 30 else best_asset_name
    col5.metric("🏆 Best Performer (Abs %)", short_best, f"{fmt_pct(best_ret_pct)} Return")
    col6.metric("📊 Dominant Asset Class", dominant_class, f"{fmt_pct(dominant_pct)} of Portfolio")
    
    st.markdown("<br>", unsafe_allow_html=True)

    # -------------------------------------------------------------------
    # VISUAL CHARTS
    # -------------------------------------------------------------------
    chart_col1, chart_col2 = st.columns(2)
    
    with chart_col1:
        st.subheader("Asset Allocation")
        if not filtered_val.empty:
            pie_data = filtered_val.groupby('Asset Class')['Current Value'].sum().reset_index()
            fig_pie = px.pie(
                pie_data, 
                values='Current Value', 
                names='Asset Class', 
                hole=0.6, 
                color_discrete_sequence=['#10b981', '#3b82f6', '#8b5cf6', '#f59e0b']
            )
            fig_pie.update_traces(textposition='inside', textinfo='percent+label', marker=dict(line=dict(color='#0e1117', width=2)))
            fig_pie.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', margin=dict(t=20, b=20, l=20, r=20), showlegend=False)
            st.plotly_chart(fig_pie, use_container_width=True)
        else:
            st.info("No data available for the selected filters.")
    
    with chart_col2:
        st.subheader("Holdings by Owner")
        if not selected_owners:
            st.info("Please select an owner.")
        else:
            bar_data = []
            for owner in selected_owners:
                fl = filtered_ledger[filtered_ledger['Portfolio Owner'] == owner]
                fv = filtered_val[filtered_val['Portfolio Owner'] == owner]
                o_inv, o_cur, _, _ = calculate_dynamic_xirr(fl, fv)
                bar_data.append({'Owner': owner, 'Type': 'Invested', 'Amount': o_inv})
                bar_data.append({'Owner': owner, 'Type': 'Current Value', 'Amount': o_cur})
                
            df_bar = pd.DataFrame(bar_data)
            if not df_bar.empty:
                if not show_values:
                    st.info("📊 Chart hidden in privacy mode.")
                else:
                    fig_bar = px.bar(
                        df_bar, 
                        x='Owner', 
                        y='Amount', 
                        color='Type', 
                        barmode='group', 
                        color_discrete_map={'Invested': '#334155', 'Current Value': '#10b981'}
                    )
                    fig_bar.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font=dict(color='#a0aec0'), yaxis_title="Amount (₹)", margin=dict(t=20))
                    st.plotly_chart(fig_bar, use_container_width=True)
    
    # -------------------------------------------------------------------
    # CONSOLIDATED HOLDINGS TABLE
    # -------------------------------------------------------------------
    st.markdown("---")
    st.subheader("Consolidated Holdings")
    
    if not filtered_val.empty:
        # Group by Asset Name and Asset Class
        agg_dict = {
            'Units': 'sum',
            'Current Value': 'sum'
        }
        if 'Value Date' in filtered_val.columns:
            agg_dict['Value Date'] = lambda x: ', '.join(sorted(list(set(x.dropna().astype(str))))) if not x.dropna().empty else '-'

        con_val = filtered_val.groupby(['Asset Name', 'Asset Class']).agg(agg_dict).reset_index()
        
        con_val = con_val.sort_values('Current Value', ascending=False)
        
        # Calculate % of Portfolio
        total_val = con_val['Current Value'].sum()
        if total_val > 0:
            con_val['% of Portfolio'] = (con_val['Current Value'] / total_val) * 100
        else:
            con_val['% of Portfolio'] = 0.0
        
        # Prepare for display
        disp_cols = ['Asset Name', 'Asset Class', 'Units', 'Current Value', '% of Portfolio']
        if 'Value Date' in con_val.columns:
            disp_cols.append('Value Date')
            
        df_disp = con_val[disp_cols].copy()
        
        if not show_values:
            df_disp["Current Value"] = "***"
            cur_col = st.column_config.TextColumn("Current Value (₹)")
        else:
            cur_col = st.column_config.NumberColumn("Current Value (₹)", format="₹ %.0f")
            
        col_config_dict = {
                "Asset Name": st.column_config.TextColumn("Asset Name", width="large"),
                "Asset Class": st.column_config.TextColumn("Asset Class"),
                "Units": st.column_config.NumberColumn("Total Units", format="%.2f"),
                "Current Value": cur_col,
                "% of Portfolio": st.column_config.ProgressColumn("% of Portfolio", format="%.2f%%", min_value=0, max_value=max(100, df_disp['% of Portfolio'].max()) if not df_disp.empty else 100)
            }
        if 'Value Date' in df_disp.columns:
            col_config_dict["Value Date"] = st.column_config.TextColumn("Value Date")

        st.data_editor(
            df_disp,
            width="stretch",
            hide_index=True,
            column_config=col_config_dict,
            disabled=True
        )

    # -------------------------------------------------------------------
    # FUND PERFORMANCE DEEP DIVE (ValueResearch Style)
    # -------------------------------------------------------------------
    st.markdown("---")
    st.subheader("Performance vs Category (Deep Dive)")
    st.markdown("Live short-term and long-term return metrics for your held Mutual Funds.")

    @st.cache_data(ttl=3600)
    def fetch_fund_deep_dive_data(mf_holdings: list) -> dict[str, pd.DataFrame]:
        """Fetch and compile live NAV returns AND risk metrics, grouped by category."""
        from analytics.mf_data_fetcher import fetch_nav_and_cagr, _pick_benchmark
        from analytics.peer_returns_engine import build_peer_group_metrics, get_peer_scheme_codes, compute_benchmark_metrics
        
        # Group funds by category
        cat_map = {}
        target_tickers = {}
        
        for ticker in mf_holdings:
            nav_data = fetch_nav_and_cagr(str(ticker))
            fund_name = nav_data.get('fund_name', str(ticker))
            category = nav_data.get('category', 'Unknown')
            
            fund_dict = {
                "Fund Name": fund_name,
                "Type": "Fund",
                "1M": nav_data.get("ret_1m", None),
                "3M": nav_data.get("ret_3m", None),
                "6M": nav_data.get("ret_6m", None),
                "1Y": nav_data.get("cagr_1y", None),
                "3Y": nav_data.get("cagr_3y", None),
                "5Y": nav_data.get("cagr_5y", None),
                "_sort": 1
            }
            if category not in cat_map:
                cat_map[category] = []
                target_tickers[category] = str(ticker)
            cat_map[category].append((str(ticker), fund_dict))
            
        # Add category medians and benchmarks
        result_dfs = {}
        for category, fund_tuples in cat_map.items():
            funds = [f[1] for f in fund_tuples]
            if category != "Unknown":
                ticker = target_tickers[category]
                sub_cat, codes = get_peer_scheme_codes(ticker)
                
                if sub_cat and codes:
                    metrics = build_peer_group_metrics(sub_cat, codes)
                    
                    # 1. Update the funds with their risk metrics & rank
                    for f_ticker, f_dict in fund_tuples:
                        f_metrics = metrics.get(f_ticker, {})
                        f_cagr_3y = f_metrics.get("cagr_3y")
                        f_dict.update({
                            "Mean Return (%)": f_cagr_3y if f_cagr_3y is not None else None,
                            "Std Dev (%)": f_metrics.get("std_dev_3y", None),
                            "Sharpe": f_metrics.get("sharpe_3y", None),
                            "Sortino": f_metrics.get("sortino_3y", None),
                            "Beta": f_metrics.get("beta_3y", None),
                            "Alpha (%)": f_metrics.get("alpha_3y", None),
                        })
                        # Calculate rank for 3Y CAGR
                        peer_cagrs = [m.get("cagr_3y") for m in metrics.values() if isinstance(m.get("cagr_3y"), (int, float))]
                        if f_cagr_3y is not None and peer_cagrs:
                            rank = sorted(peer_cagrs, reverse=True).index(f_cagr_3y) + 1
                            f_dict["Rank"] = f"{rank}/{len(peer_cagrs)}"
                        else:
                            f_dict["Rank"] = "N/A"

                    # 2. Add Benchmark Row
                    bm_sym, bm_label = _pick_benchmark(category)
                    bm = compute_benchmark_metrics(bm_sym)
                    funds.append({
                        "Fund Name": f"Benchmark: {bm_label}",
                        "Type": "Benchmark",
                        "1M": bm.get("ret_1m", None),
                        "3M": bm.get("ret_3m", None),
                        "6M": bm.get("ret_6m", None),
                        "1Y": bm.get("cagr_1y", None),
                        "3Y": bm.get("cagr_3y", None),
                        "5Y": bm.get("cagr_5y", None),
                        "Mean Return (%)": bm.get("cagr_3y", None),
                        "Std Dev (%)": bm.get("std_dev_3y", None),
                        "Sharpe": bm.get("sharpe_3y", None),
                        "Sortino": bm.get("sortino_3y", None),
                        "Beta": bm.get("beta_3y", None),
                        "Alpha (%)": bm.get("alpha_3y", None),
                        "Rank": "-",
                        "_sort": 2
                    })

                    # 3. Add Category Median Row
                    def _med(key):
                        vals = [m.get(key) for m in metrics.values() if isinstance(m.get(key), (int, float))]
                        if not vals: return None
                        import numpy as np
                        return round(float(np.median(vals)), 2)
                        
                    funds.append({
                        "Fund Name": f"Category Median: {sub_cat}",
                        "Type": "Category Median",
                        "1M": _med("ret_1m"),
                        "3M": _med("ret_3m"),
                        "6M": _med("ret_6m"),
                        "1Y": _med("cagr_1y"),
                        "3Y": _med("cagr_3y"),
                        "5Y": _med("cagr_5y"),
                        "Mean Return (%)": _med("cagr_3y"),
                        "Std Dev (%)": _med("std_dev_3y"),
                        "Sharpe": _med("sharpe_3y"),
                        "Sortino": _med("sortino_3y"),
                        "Beta": _med("beta_3y"),
                        "Alpha (%)": _med("alpha_3y"),
                        "Rank": "-",
                        "_sort": 3
                    })
                    
            df = pd.DataFrame(funds)
            # Reorder columns slightly perfectly
            cols = ["Type", "Fund Name", "1M", "3M", "6M", "1Y", "3Y", "5Y", "Mean Return (%)", "Std Dev (%)", "Sharpe", "Sortino", "Beta", "Alpha (%)", "Rank"]
            avail_cols = [c for c in cols if c in df.columns] + ["_sort"]
            result_dfs[category] = df[avail_cols].sort_values(by=["_sort", "Fund Name"], ascending=[True, True]).drop(columns=["_sort"])
            
        return result_dfs

    if not filtered_val.empty:
        mf_holdings = filtered_val[filtered_val['Asset Class'] == 'Mutual Fund']['Ticker'].unique().tolist()
        
        if len(mf_holdings) > 0:
            with st.spinner("Fetching live NAV performance data (cached for 1hr)..."):
                cat_dfs = fetch_fund_deep_dive_data(mf_holdings)

                if cat_dfs:
                    # Color formatting logic
                    def style_positive_negative(val):
                        num = None
                        if isinstance(val, (int, float)):
                            num = val
                        elif isinstance(val, str):
                            try:
                                num = float(val.replace("%", "").replace("+", "").strip())
                            except ValueError:
                                pass
                        
                        if num is not None:
                            if num > 0:
                                return "color: #10b981;" # Green
                            elif num < 0:
                                return "color: #ef4444;" # Red
                        return ""
                    
                    for category, df_perf in sorted(cat_dfs.items()):
                        st.markdown(f"**{category}**")
                        
                        def format_float_safely(val):
                            import math
                            if pd.isna(val):
                                return "N/A"
                            if isinstance(val, (int, float)):
                                return f"{val:.2f}"
                            return str(val)

                        styled_df = df_perf.style.map(
                            style_positive_negative, 
                            subset=["1M", "3M", "6M", "1Y", "3Y", "5Y", "Mean Return (%)", "Alpha (%)"]
                        ).format(
                            format_float_safely,
                            subset=["1M", "3M", "6M", "1Y", "3Y", "5Y", "Mean Return (%)", "Std Dev (%)", "Sharpe", "Sortino", "Beta", "Alpha (%)"]
                        )
                        
                        st.dataframe(
                            styled_df,
                            use_container_width=True,
                            hide_index=True
                        )
        else:
             st.info("No Mutual Fund holdings found in the current selection.")

with tab_portfolios:
    # -------------------------------------------------------------------
    # INTERACTIVE DRILL-DOWN TABLES (MODERN DATA EDITOR)
    # -------------------------------------------------------------------
    view_owner = st.selectbox("Select Portfolio to View:", selected_owners)
    
    if view_owner:
        o_ledger = filtered_ledger[filtered_ledger['Portfolio Owner'] == view_owner]
        o_val = filtered_val[filtered_val['Portfolio Owner'] == view_owner]
        
        p_inv, p_cur, p_ret, p_xirr = calculate_dynamic_xirr(o_ledger, o_val)
        
        tcol1, tcol2, tcol3, tcol4, tcol5 = st.columns(5)
        tcol1.metric("Total Invested (FIFO)", fmt_amt(p_inv))
        delta_p_cur = f"₹ {p_ret:,.0f}" if show_values else r"\*\*\*"
        tcol2.metric("Current Value", fmt_amt(p_cur), delta_p_cur)
        abs_p_pct = (p_ret/p_inv * 100) if p_inv else 0
        tcol3.metric("Absolute Return", fmt_pct(abs_p_pct), fmt_pct(abs_p_pct))
        tcol4.metric("Annualized XIRR", fmt_pct(p_xirr), fmt_pct(p_xirr))
        tcol5.metric("Assets Held", len(o_val['Ticker'].unique()))
        
        st.markdown("<br>", unsafe_allow_html=True)
        st.subheader("Holdings Ledger")
        
        if o_val.empty:
            st.info("No active holdings for this selection.")
        else:
            drill_data = []
            for ticker in o_val['Ticker'].unique():
                t_val = o_val[o_val['Ticker'] == ticker].iloc[0]
                t_ledger = o_ledger[o_ledger['Ticker'] == ticker]
                
                t_inv, t_cur, t_ret, t_xirr = calculate_dynamic_xirr(t_ledger, o_val[o_val['Ticker'] == ticker])
                
                drill_data.append({
                    "Asset Name": t_val['Asset Name'],
                    "Asset Class": t_val['Asset Class'],
                    "Units Held": float(t_val['Units']),
                    "Live NAV": float(t_val['Live NAV']),
                    "Invested Base": float(t_inv),
                    "Market Value": float(t_cur),
                    "Absolute %": float(f"{(t_ret/t_inv * 100) if t_inv else 0:.2f}"),
                    "XIRR %": float(f"{t_xirr:.2f}"),
                    "Value Date": str(t_val.get('Value Date', '-'))
                })
                
            df_drill = pd.DataFrame(drill_data)
            
            if not show_values:
                df_drill["Invested Base"] = "***"
                df_drill["Market Value"] = "***"
                inv_col = st.column_config.TextColumn("Invested (₹)")
                cur_col = st.column_config.TextColumn("Current Value (₹)")
            else:
                inv_col = st.column_config.NumberColumn("Invested (₹)", format="₹ %.0f")
                cur_col = st.column_config.NumberColumn("Current Value (₹)", format="₹ %.0f")
            
            col_config_drill = {
                    "Asset Name": st.column_config.TextColumn("Asset Name", width="large"),
                    "Units Held": st.column_config.NumberColumn("Units Held", format="%.2f"),
                    "Live NAV": st.column_config.NumberColumn("Live NAV (₹)", format="₹ %.2f"),
                    "Invested Base": inv_col,
                    "Market Value": cur_col,
                    "Absolute %": st.column_config.ProgressColumn("Return %", format="%.2f%%", min_value=0, max_value=max(100, df_drill['Absolute %'].max())),
                    "XIRR %": st.column_config.NumberColumn("XIRR", format="%.2f%%"),
                    "Value Date": st.column_config.TextColumn("Value Date")
                }
            
            # Use sleek data_editor instead of dataframe
            st.data_editor(
                df_drill,
                width="stretch",
                hide_index=True,
                column_config=col_config_drill,
                disabled=True # Keep it read-only but beautiful
            )

with tab_retirement:
    # -------------------------------------------------------------------
    # RETIREMENT ASSET ISOLATION (NPS/EPF)
    # -------------------------------------------------------------------
    st.subheader("Long-Term Retirement Holdings")
    st.markdown("This section isolates pure retirement vehicles like the National Pension System (NPS) and Employees' Provident Fund (EPF).", unsafe_allow_html=True)
    
    # Filter for Retirement Assets (Currently just NPS)
    ret_ledger = filtered_ledger[filtered_ledger['Asset Class'].str.contains('NPS|EPF', case=False, na=False)]
    ret_val = filtered_val[filtered_val['Asset Class'].str.contains('NPS|EPF', case=False, na=False)]
    
    if ret_val.empty:
        st.info("No retirement assets (NPS/EPF) found in the current selection.")
    else:
        r_inv, r_cur, r_ret, r_xirr = calculate_dynamic_xirr(ret_ledger, ret_val)
        
        tcol1, tcol2, tcol3, tcol4, tcol5 = st.columns(5)
        tcol1.metric("Total Invested (FIFO)", fmt_amt(r_inv))
        delta_r_cur = f"₹ {r_ret:,.0f}" if show_values else r"\*\*\*"
        tcol2.metric("Current Value", fmt_amt(r_cur), delta_r_cur)
        abs_r_pct = (r_ret/r_inv * 100) if r_inv else 0
        tcol3.metric("Absolute Return", fmt_pct(abs_r_pct), fmt_pct(abs_r_pct))
        tcol4.metric("Annualized XIRR", fmt_pct(r_xirr), fmt_pct(r_xirr))
        tcol5.metric("Active Schemes", len(ret_val['Ticker'].unique()))
        
        st.markdown("<br>", unsafe_allow_html=True)
        st.subheader("Scheme Breakdown")
        
        ret_data = []
        for ticker in ret_val['Ticker'].unique():
            t_val = ret_val[ret_val['Ticker'] == ticker].iloc[0]
            t_ledger = ret_ledger[ret_ledger['Ticker'] == ticker]
            
            t_inv, t_cur, t_ret, t_xirr = calculate_dynamic_xirr(t_ledger, ret_val[ret_val['Ticker'] == ticker])
            
            ret_data.append({
                "Scheme Name": t_val['Asset Name'],
                "Owner": t_val['Portfolio Owner'],
                "Units Held": float(t_val['Units']),
                "Live NAV": float(t_val['Live NAV']),
                "Invested Base": float(t_inv),
                "Market Value": float(t_cur),
                "Absolute %": float(f"{(t_ret/t_inv * 100) if t_inv else 0:.2f}"),
                "XIRR %": float(f"{t_xirr:.2f}"),
                "Value Date": str(t_val.get('Value Date', '-'))
            })
            
        df_ret = pd.DataFrame(ret_data)
        
        if not show_values:
            df_ret["Invested Base"] = "***"
            df_ret["Market Value"] = "***"
            inv_col = st.column_config.TextColumn("Invested (₹)")
            cur_col = st.column_config.TextColumn("Current Value (₹)")
        else:
            inv_col = st.column_config.NumberColumn("Invested (₹)", format="₹ %.0f")
            cur_col = st.column_config.NumberColumn("Current Value (₹)", format="₹ %.0f")
        
        st.data_editor(
            df_ret,
            width="stretch",
            hide_index=True,
            column_config={
                "Scheme Name": st.column_config.TextColumn("Scheme Name", width="large"),
                "Owner": st.column_config.TextColumn("Owner"),
                "Units Held": st.column_config.NumberColumn("Units Held", format="%.2f"),
                "Live NAV": st.column_config.NumberColumn("Live NAV (₹)", format="₹ %.2f"),
                "Invested Base": inv_col,
                "Market Value": cur_col,
                "Absolute %": st.column_config.ProgressColumn("Return %", format="%.2f%%", min_value=0, max_value=max(100, df_ret['Absolute %'].max())),
                "XIRR %": st.column_config.NumberColumn("XIRR", format="%.2f%%"),
                "Value Date": st.column_config.TextColumn("Value Date")
            },
            disabled=True
        )

# -------------------------------------------------------------------
# 6. PDF EXPORT FEATURE
# -------------------------------------------------------------------
if app_mode == "Dashboard":
    with export_container:
        st.markdown("---")
        st.markdown("### 📥 Export Report")
        
        if 'pdf_export_data' not in st.session_state:
            st.session_state.pdf_export_data = None
            
        if st.button("Generate Dashboard PDF", use_container_width=True):
            with st.spinner("Compiling PDF and rendering charts..."):
                try:
                    from dashboard.export_pdf import generate_pdf_report
                    
                    owners_str = ", ".join(selected_owners) if selected_owners else "None"
                    metrics = {
                        "Total Invested (FIFO)": fmt_amt(inv),
                        "Current Value": fmt_amt(cur),
                        "Absolute Return": fmt_pct(abs_pct),
                        "Annualized XIRR": fmt_pct(x_pct)
                    }
                    
                    figures = {}
                    if 'fig_alloc' in locals():
                        figures['Asset Allocation'] = fig_alloc
                    if 'fig_pie' in locals():
                        figures['Portfolio Sectors'] = fig_pie
                    if 'fig_bar' in locals():
                        figures['Holdings by Owner'] = fig_bar
                        
                    pdf_path = generate_pdf_report(
                        owner_title=owners_str,
                        metrics=metrics,
                        figures=figures,
                        df_raw=filtered_val
                    )
                    
                    with open(pdf_path, "rb") as f:
                        st.session_state.pdf_export_data = f.read()
                        
                except Exception as e:
                    st.error(f"Error generating PDF: {e}")
                    
        if st.session_state.pdf_export_data:
            st.download_button(
                label="Download Generated PDF",
                data=st.session_state.pdf_export_data,
                file_name="Portfolio_Analysis_Report.pdf",
                mime="application/pdf",
                type="primary",
                use_container_width=True
            )
