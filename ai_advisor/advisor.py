import hashlib
import os
import logging
from pathlib import Path
from dotenv import load_dotenv

from crewai import Agent, Task, Crew, Process, LLM
from ai_advisor.ai_tools import (
    get_portfolio_holdings, get_asset_allocation, get_stock_fundamentals,
    get_equity_lookthrough,
)

load_dotenv()
logger = logging.getLogger(__name__)

def _portfolio_hash(owner_name: str) -> str:
    """Returns a short MD5 hash of the owner's current valuation data.
    Used as a cache key — changes when the portfolio changes."""
    try:
        import pandas as pd
        val = pd.read_csv('data/output/master_valuation.csv')
        if owner_name.lower() != 'all':
            val = val[val['Portfolio Owner'] == owner_name]
        return hashlib.md5(val.to_json().encode()).hexdigest()[:12]
    except Exception:
        return 'no_hash'


def _cache_path(owner_name: str) -> Path:
    """Returns the cache file path for a given owner + portfolio state."""
    safe_owner = owner_name.replace(' ', '_')
    return Path(f'data/output/advisor_cache_{safe_owner}_{_portfolio_hash(owner_name)}.md')

DEMO_VALUATION_CSV = """Portfolio Owner,Asset Class,Asset Name,Units,Live NAV,Current Value
DemoUser,Mutual Fund,Parag Parikh Flexi Cap Fund,500.0,85.23,42615.0
DemoUser,Mutual Fund,Mirae Asset Large Cap Fund,800.0,112.45,89960.0
DemoUser,Stocks,INFY.NS,150.0,1823.0,273450.0
DemoUser,Stocks,HDFCBANK.NS,200.0,1654.0,330800.0
DemoUser,NPS,HDFC NPS Scheme E,10000.0,45.12,451200.0
DemoUser,EPF,EPF - DemoUser,1.0,285000.0,285000.0
DemoUser,Fixed Deposit,SBI FD 7.1%,1.0,200000.0,200000.0
"""

DEMO_ALLOCATION_CSV = """Owner,Sub Class,Value
DemoUser,Large Cap Equity,432375.0
DemoUser,Flexi Cap Equity,42615.0
DemoUser,NPS Equity,315840.0
DemoUser,Debt - NPS,90240.0
DemoUser,Debt - EPF,285000.0
DemoUser,Debt - FD,200000.0
DemoUser,Cash,45360.0
"""


def _setup_demo_data() -> None:
    """Writes synthetic CSV files so the demo owner 'DemoUser' works with all tools."""
    import pandas as pd, io
    out = Path('data/output')
    out.mkdir(parents=True, exist_ok=True)

    val_path = out / 'master_valuation.csv'
    alloc_path = out / 'asset_allocation_drilldown.csv'

    # Only inject demo rows — do not overwrite real data if it exists
    demo_val = pd.read_csv(io.StringIO(DEMO_VALUATION_CSV))
    demo_alloc = pd.read_csv(io.StringIO(DEMO_ALLOCATION_CSV))

    for path, demo_df, owner_col in [
        (val_path, demo_val, 'Portfolio Owner'),
        (alloc_path, demo_alloc, 'Owner'),
    ]:
        if path.exists():
            existing = pd.read_csv(path)
            if 'DemoUser' in existing[owner_col].values:
                logger.info("Demo data already present in %s", path)
                continue
            combined = pd.concat([existing, demo_df], ignore_index=True)
        else:
            combined = demo_df
        combined.to_csv(path, index=False)
        logger.info("Demo data written to %s", path)


def generate_portfolio_review(owner_name: str) -> str:
    """
    Kicks off the multi-agent CrewAI process to analyze the portfolio.
    Returns a markdown string containing the final AI generated report.
    """
    cache = _cache_path(owner_name)
    # Cache disabled — uncomment to re-enable after testing
    # if cache.exists():
    #     logger.info("Cache hit for %s — returning cached report.", owner_name)
    #     return cache.read_text(encoding='utf-8')

    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        return "Error: GEMINI_API_KEY not found in .env file."
        
    model_name = os.getenv("GEMINI_MODEL", "gemini-3-flash-preview").strip('"\'')
    
    logger.info(f"Initializing AI Advisor for {owner_name} using model: {model_name}")
    
    # Initialize Gemini LLM natively via LiteLLM
    try:
        llm = LLM(
            model=f"gemini/{model_name}",
            api_key=api_key,
            temperature=0.2
        )
    except Exception as e:
        return f"Error initializing Google Gemini LLM: {e}"
    
    # --- Define Agents ---
    data_analyst = Agent(
        role='Senior Financial Data Analyst',
        goal='Extract and digest the precise portfolio holdings, metrics, and stock fundamentals for the client.',
        backstory='An expert quantitative data analyst who excels at pulling precise numbers from datasets and fetching live ticker fundamentals.',
        verbose=True,
        allow_delegation=False,
        cache=False,
        tools=[get_portfolio_holdings, get_asset_allocation, get_stock_fundamentals, get_equity_lookthrough],
        llm=llm
    )
    
    risk_manager = Agent(
        role='Chief Risk Officer',
        goal='Analyze the asset allocation look-through data to identify concentration risks, over-exposure, or severe under-diversification.',
        backstory='A seasoned risk manager who protects client wealth by ensuring they are not over-exposed to a single asset class or sector.',
        verbose=True,
        allow_delegation=False,
        cache=False,
        tools=[get_asset_allocation],
        llm=llm
    )
    
    financial_planner = Agent(
        role='Certified Financial Planner',
        goal=(
            'Map the current portfolio against the client\'s life stage. '
            'Identify whether the emergency fund (3-6 months expenses), '
            'retirement corpus, and major goal allocations look adequate based on asset mix.'
        ),
        backstory=(
            'A goal-based planner who translates risk assessments into concrete milestone targets. '
            'Specialises in Indian personal finance — PPF, NPS, ELSS, and term insurance coverage gaps.'
        ),
        verbose=True,
        allow_delegation=False,
        cache=False,
        tools=[get_portfolio_holdings, get_asset_allocation, get_equity_lookthrough],
        llm=llm
    )

    cio = Agent(
        role='Chief Investment Officer (CIO)',
        goal='Synthesize data and risk analysis into a highly professional, actionable portfolio recommendation report.',
        backstory='A top-tier wealth manager known for providing clear, actionable (Buy/Hold/Sell) advice based on cold hard data and risk parity.',
        verbose=True,
        allow_delegation=False,
        cache=False,
        llm=llm
    )
    
    # --- Define Tasks ---
    task_data_gathering = Task(
        description=(
            f'Use your tools to fetch the comprehensive portfolio holdings, asset allocation, and equity look-through '
            f'analysis for the owner: "{owner_name}". '
            f'Specifically: (1) call get_portfolio_holdings to get current holdings and values, '
            f'(2) call get_asset_allocation to get Equity/Debt/Gold/Cash breakdown, '
            f'(3) call get_equity_lookthrough to get the true Large/Mid/Small cap split and Domestic vs International breakdown. '
            f'If there are specific Indian stock tickers in the holdings, fetch their fundamentals. '
            f'Document the exact total invested amount, absolute returns, and current values.'
        ),
        expected_output=(
            'A structured summary of all assets the user owns, their performance, '
            'live fundamental data for key stocks, AND the equity look-through breakdown '
            '(large/mid/small cap % and domestic/international % of total equity).'
        ),
        agent=data_analyst
    )
    
    task_risk_assessment = Task(
        description=f'Analyze the asset allocation (Equity/Debt/Gold/Cash) breakdown for owner: "{owner_name}". '
                    f'Identify if the portfolio is too aggressive, too conservative, or too strictly concentrated in one area.',
        expected_output='A risk assessment report highlighting vulnerabilities and structural strengths in the portfolio allocation.',
        agent=risk_manager
    )
    
    task_goal_mapping = Task(
        description=(
            f'Using the holdings and allocation data for "{owner_name}", assess goal readiness:\n'
            f'1. Is there a liquid emergency fund (estimate based on Debt/Cash allocation)?\n'
            f'2. Is the long-term equity allocation sufficient for retirement horizon?\n'
            f'3. Are tax-saving instruments (ELSS, NPS) being utilised? Note: If the client is under the New Tax Regime, section 80C benefits (like ELSS) are no longer applicable.\n'
            f'State clearly what is present and what is missing. Do not invent numbers.'
        ),
        expected_output=(
            'A goal readiness summary covering emergency fund adequacy, '
            'retirement allocation status, and tax optimisation gaps.'
        ),
        agent=financial_planner
    )
    
    task_final_report = Task(
        description=(
            f'Review the data gathered, risk assessment, and goal mapping for "{owner_name}". '
            f'Write a highly professional, polished Markdown report (similar to Value Research or Morningstar style) addressed to the client. '
            f'Include 4 sections: '
            f'1) Portfolio Health Summary, '
            f'2) Equity Look-Through Analysis (cap-size & geography), '
            f'3) Key Risks & Allocation Gaps, '
            f'4) Actionable Recommendations (specific assets to Hold, Sell, or Add).\n\n'
            f'Apply Indian market context throughout:\n'
            f'- Equity funds: If the client uses the Old Tax Regime, recommend ELSS for 80C benefits. '
            f'If they use the New Tax Regime, 80C does not apply — recommend diversified/flexi-cap funds instead.\n'
            f'- If NPS holdings are present, comment on whether Tier I tax benefit under 80CCD(1B) is being maximised.\n'
            f'- Benchmark all equity mutual funds against Nifty 50 and their category average.\n'
            f'- Gold: If gold allocation exceeds 15%, suggest reviewing it. IMPORTANT: SGBs are NOT open for fresh '
            f'primary issuance by the RBI — advise secondary market purchase or Gold ETF/Fund instead.\n'
            f'- Flag any single stock position exceeding 10% of total equity as a concentration risk.\n'
            f'- Cap-size balance: Use the look-through data. Flag if Large Cap > 70% (too conservative for growth) '
            f'or Small Cap > 35% (high risk). Ideal balance for a growth-oriented portfolio: '
            f'Large 50-65%, Mid 20-30%, Small 10-20%.\n'
            f'- International exposure: Use the look-through data. Flag if international equity < 5% '
            f'(no global diversification) or > 30% (excessive currency risk). '
            f'Ideal range for an Indian investor: 10-20% of equity in international funds. '
            f'List the top contributing funds to international exposure.\n'
            f'- Do not use any fake data. If specific data is unavailable, say so clearly.'
        ),
        expected_output=(
            'A comprehensive, client-ready Markdown report covering portfolio health, '
            'equity look-through (cap-size + geography), risk analysis, and definitive recommendations.'
        ),
        agent=cio
    )
    
    # --- Assemble Crew ---
    crew = Crew(
        agents=[data_analyst, risk_manager, financial_planner, cio],
        tasks=[task_data_gathering, task_risk_assessment, task_goal_mapping, task_final_report],
        process=Process.sequential,
        verbose=True,
        cache=False
    )
    
    # Execute the workflow
    try:
        result = crew.kickoff()
        report = str(result.raw)
        if len(report) > 200:
            cache = _cache_path(owner_name)
            cache.write_text(report, encoding='utf-8')
            logger.info("Report cached at %s", cache)
        return report
    except Exception as e:
        logger.error(f"CrewAI execution failed: {e}")
        return f"Agent framework execution failed: {e}"

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)

    demo_mode = '--demo' in sys.argv
    owner = 'DemoUser' if demo_mode else 'ALL'

    if demo_mode:
        logger.info("DEMO MODE — injecting synthetic portfolio data for 'DemoUser'")
        _setup_demo_data()

    logger.info("Running AI Advisor for %s...", owner)
    report = generate_portfolio_review(owner)
    logger.info("\n--- FINAL REPORT ---\n%s", report)
