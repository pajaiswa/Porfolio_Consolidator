import logging
from pathlib import Path

import pandas as pd
import streamlit as st

logger = logging.getLogger(__name__)


def _render_lookthrough_card(owner: str) -> None:
    """
    Renders the Equity Look-Through preview card from pre-computed CSV.
    Shown before the 'Generate Report' button so the user sees data without
    needing to run the LLM.
    """
    lt_path = Path("data/output/equity_lookthrough.csv")
    if not lt_path.exists():
        st.warning(
            "⚠️ Equity look-through data not found. Run the full pipeline first "
            "(Step 4b: `analytics/compute_equity_lookthrough.py`)."
        )
        return

    try:
        df = pd.read_csv(lt_path)
        if owner.lower() != "all":
            df = df[df["Owner"] == owner]
        else:
            numeric_cols = [
                "LargeCap_Value", "MidCap_Value", "SmallCap_Value",
                "Domestic_Eq_Value", "International_Eq_Value", "Total_Equity_Value",
            ]
            agg = df[numeric_cols].sum()
            total = float(agg["Total_Equity_Value"])
            if total <= 0:
                st.info("No equity holdings found for look-through analysis.")
                return
            df = pd.DataFrame([{
                "Owner": "All",
                "LargeCap_Pct":  round(float(agg["LargeCap_Value"])  / total * 100, 1),
                "MidCap_Pct":    round(float(agg["MidCap_Value"])    / total * 100, 1),
                "SmallCap_Pct":  round(float(agg["SmallCap_Value"])  / total * 100, 1),
                "Domestic_Pct":  round(float(agg["Domestic_Eq_Value"]) / total * 100, 1),
                "Intl_Pct":      round(float(agg["International_Eq_Value"]) / total * 100, 1),
                "Total_Equity_Value": total,
                "as_of_date": pd.read_csv(lt_path)["as_of_date"].iloc[0],
            }])

        if df.empty:
            st.info(f"No equity look-through data for {owner}.")
            return

        row = df.iloc[0]
        total_eq = float(row.get("Total_Equity_Value", 0))
        as_of = row.get("as_of_date", "")

        st.markdown("### 🔍 Portfolio Look-Through Preview")
        st.caption(
            f"Computed from fund-level allocation data · as of {as_of} · "
            "feeds directly into the AI report"
        )

        col_cap, col_geo = st.columns(2)

        with col_cap:
            st.markdown("**📊 By Market Cap**")
            lc = float(row.get("LargeCap_Pct", 0))
            mc = float(row.get("MidCap_Pct", 0))
            sc = float(row.get("SmallCap_Pct", 0))
            c1, c2, c3 = st.columns(3)
            c1.metric("Large Cap", f"{lc:.1f}%",
                      delta="✅ Ideal" if 50 <= lc <= 70 else ("⚠️ High" if lc > 70 else "⚠️ Low"))
            c2.metric("Mid Cap",   f"{mc:.1f}%",
                      delta="✅ Ideal" if 20 <= mc <= 30 else "ℹ️ Check")
            c3.metric("Small Cap", f"{sc:.1f}%",
                      delta="⚠️ High risk" if sc > 35 else ("✅ OK" if sc >= 10 else "ℹ️ Low"))

        with col_geo:
            st.markdown("**🌍 By Geography**")
            dom  = float(row.get("Domestic_Pct", 0))
            intl = float(row.get("Intl_Pct", 0))
            g1, g2 = st.columns(2)
            g1.metric("Domestic",      f"{dom:.1f}%")
            g2.metric("International", f"{intl:.1f}%",
                      delta="⚠️ Low — consider global funds" if intl < 5 else
                            ("⚠️ High — currency risk" if intl > 30 else "✅ Ideal range"))

        st.caption(f"Total Equity Value: ₹{total_eq:,.0f}")

        with st.expander("ℹ️ How is this calculated?"):
            st.markdown(
                """
                **Market Cap split** is derived by applying each fund's Large/Mid/Small cap allocation
                (scraped from Moneycontrol, refreshed every 30 days) to its current portfolio value.
                Direct stock holdings are classified using the Nifty 50 / Midcap 150 constituency lists.

                **Geographic split** uses each fund's Domestic vs International equity percentage,
                also scraped from Moneycontrol. Funds like Parag Parikh Flexi Cap show a real
                ~12% international allocation which flows through here.

                To override any fund's values, edit `data/input/allocation_overrides.csv`.
                """
            )

    except Exception as e:
        st.warning(f"Could not load look-through data: {e}")
        logger.warning("Look-through card error: %s", e)


def render_ai_advisor(owners: list):
    st.title("🤖 AI Portfolio Advisor")
    st.markdown(
        "A 4-agent AI framework (powered by Gemini) analyses your portfolio using live data, "
        "computes a look-through equity breakdown, and generates professional actionable recommendations."
    )

    if not owners:
        st.warning("No portfolio owners found. Please manage data first to build the ledger.")
        return

    selected_owner = st.selectbox("Select Portfolio to Analyse", ["All"] + owners)

    st.divider()

    # ── Look-Through Preview Card (no LLM needed) ──────────────────────────
    _render_lookthrough_card(selected_owner)

    st.divider()

    st.info(
        "The AI Advisor deploys a Crew of 4 specialised AI agents — Data Analyst, Risk Officer, "
        "Financial Planner, and CIO — to generate a Morningstar-style advisory report."
    )

    if st.button("Generate Comprehensive Report", type="primary"):
        status_box = st.status("Initialising AI agents...", expanded=True)

        with status_box:
            st.write("Agent 1 — Data Analyst: fetching holdings, allocation & look-through...")
            progress_placeholder = st.empty()

        try:
            from ai_advisor.advisor import generate_portfolio_review

            def _on_step(step_output):
                """Called by CrewAI after each agent step."""
                try:
                    agent_name = getattr(step_output, "agent", "Agent")
                    progress_placeholder.info(f"Completed step by: {agent_name}")
                except Exception:
                    pass

            with status_box:
                st.write("Agent 2 — Risk Officer: analysing concentration risks...")

            with status_box:
                st.write("Agent 3 — Financial Planner: mapping goals and tax optimisation...")

            report = generate_portfolio_review(selected_owner)

            with status_box:
                st.write("Agent 4 — CIO: writing final report...")
            status_box.update(label="Analysis complete!", state="complete", expanded=False)

            if report.startswith("Error") or report.startswith("Agent framework"):
                st.error(report)
            else:
                st.success(f"Report ready for {selected_owner}")

                st.download_button(
                    label="⬇️ Download Report as Markdown",
                    data=report,
                    file_name=f"{selected_owner}_AI_Portfolio_Review.md",
                    mime="text/markdown",
                )

                st.markdown("---")
                st.markdown(report)

        except Exception as e:
            status_box.update(label="Analysis failed", state="error", expanded=True)
            st.error(f"Failed to generate report: {e}")
            logger.error("AI Advisor error: %s", e, exc_info=True)
