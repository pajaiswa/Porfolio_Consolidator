# Technical Documentation & Developer Guide

## 🏗️ Architecture & Module Map

The project is split into **six focused Python packages**, each with a single responsibility. `run_all.py` orchestrates them in dependency order.

```
ingestion/          → Parse raw files into master_ledger.csv
valuation/          → Fetch live prices into master_valuation.csv
analytics/          → Compute metrics (XIRR, allocation, benchmark)
dashboard/          → Streamlit web UI
ai_advisor/         → CrewAI multi-agent advisory engine
core/               → Shared utilities (logging)
```

### Pipeline Execution Order (`run_all.py`)

```
Step 1a  ingestion/ingest_mf.py        CAMS PDF + Groww XLSX → master_ledger.csv
Step 1b  ingestion/ingest_stocks.py    Groww stock XLSX      → master_ledger.csv
Step 1c  ingestion/ingest_nps.py       KFintech NPS PDF      → master_ledger.csv
Step 1d  ingestion/ingest_epf.py       Manual EPF config     → master_ledger.csv
Step 1e  ingestion/ingest_fd.py        Manual FD config      → master_ledger.csv
Step 1f  ingestion/ingest_global.py    IBKR manual CSV       → master_ledger.csv
Step 1g  valuation/fetch_nps_navs.py   npsnav.in API         → nps_latest_navs.json
Step 2a  valuation/valuate_mf_nps.py   AMFI API              → master_valuation.csv
Step 2b  valuation/valuate_stocks.py   yfinance (.NS)        → master_valuation.csv
Step 2c  valuation/valuate_epf.py      Accrual               → master_valuation.csv
Step 2d  valuation/valuate_fd.py       Accrual               → master_valuation.csv
Step 2e  valuation/valuate_global.py   yfinance + USDINR=X   → master_valuation.csv
Step 3   analytics/calculate_xirr.py   FIFO + XIRR           → performance_metrics.csv
Step 4a  analytics/calc_allocations.py Asset Class mapping   → asset_allocation.csv
Step 4b  analytics/compute_equity_lookthrough.py Cap/Geo Split → equity_lookthrough.csv
Step 5   analytics/benchmark.py        Nifty 50 yfinance     → nifty50_history.csv
Step 6   analytics/export_ai_summary.py Holding-level XIRR   → ai_portfolio_summary.csv
```

`run_all.py` stops on the first failure — partial data never reaches the dashboard.

---

## 🛠️ Customisations & Known Shortcomings

- **NPS NAV Fetching:** NAVs are fetched **live from the `npsnav.in` JSON API** via `valuation/fetch_nps_navs.py`. The PDF is still required for historical cash flows and unit counts, but the NAV used for valuation is always current. Falls back to cached value if the API is unreachable.

- **Broker Lock-in & UI Validation:** The pipeline strictly expects:
  - Mutual Funds: **Groww `.xlsx` exports** or **CAMS CAS secure PDFs**. The UI validates CAMS PDFs dynamically using a fast `casparser.read_cas_pdf` dry-run.
  - Stocks: **Groww stock order `.xlsx` exports** (skip-rows=5 format)
  - NPS: **KFintech / Protean NPS PDFs**. The UI validates these by intercepting `PdfminerException` for encryption and scanning the first page for keywords (e.g., "KFINTECH", "NATIONAL PENSION SYSTEM").
  - Other brokers may not parse correctly and will be rejected by the UI early.

- **Owner Name Discovery:** All ingestion scripts derive the owner name from the **filename stem** (e.g. `Champalal_MF.xlsx` → owner is `Champalal`). The first `_`-separated token is used.

- **Hardcoded Values & Heuristics:**
  - **EPF Interest Rate:** Provided via `epf_config.csv` — not fetched from EPFO.
  - **Look-through Heuristics:** If `fetch_allocations.py` fails to scrape cap-size or geographical split data from Moneycontrol, it falls back to a deterministic heuristic based on the AMFI fund category or fund name keywords. Direct stock holdings are statically classified using baked-in Nifty 50 and Midcap 150 lists.
  - User can provide manual look-through corrections using `data/input/allocation_overrides.csv`.
  - Fuzzy string matching (via `difflib`) is used to map Groww scheme names to AMFI codes.

---

## 📋 To-Do / Backlog

- [x] **[HIGH] Address NPS NAV:** ✅ `valuation/fetch_nps_navs.py` now fetches live NAVs from `npsnav.in` API (HDFC Scheme E/C/G/EquityAdv). Scheme A excluded — merged into Scheme C.
- [x] **[HIGH] AI Advisor Overhaul:** ✅ 4-agent CrewAI pipeline with Indian market context, Look-Through capability (Cap-size/Geography), tax regime-aware advice, `st.status` streaming UI, JSON tool outputs, and completely disabled internal AI caching for genuine fresh runs.
- [x] **[HIGH] IBKR Global Holdings Integration:** ✅ New `Global Holdings` asset class — persistent transaction ledger (`global_transactions.csv`) UI-managed from the Data Management screen. Each trade is one row. `ingestion/ingest_global.py` feeds trades into `master_ledger.csv`; `valuation/valuate_global.py` fetches live USD prices via yfinance and converts to INR using the live `USDINR=X` FX rate. Classified as `Equity_Foreign` in asset allocation. Enables accurate per-trade XIRR.
- [x] **[HIGH] AI Portfolio Summary Dataset:** ✅ Consolidated `ai_portfolio_summary.csv` holding-level dataset for deeper CrewAI analytics. Aggregates XIRR, FIFO invested amount, current value, and primary sub-class mapping per holding into a single rich flat-file context block.
- [x] **[MED] Refactor NPS Merger Hardcode:** ✅ Hardcoded `MERGER_TRANSFER_VALUE` was removed. The merger is now dynamically evaluated by computing Scheme A's exact pre-merger net invested capital so that FIFO seamlessly transfers the cost basis into Scheme C without artificially inflating overall Total Invested limits.
- [ ] **[MED] KFintech CAS Support:** Expand mutual fund parsing to handle standalone KFintech CAS PDFs.
- [ ] **[MED] Multi-Broker Support:** Abstract stock parsing to handle standard CDSL/NSDL CAS statements.
- [ ] **[LOW] PDF Export Styling:** Enhance the visual aesthetics and CSS-like layout of the FPDF2 generated reports to make them look more professional and polished.
- [ ] **[LOW] Config-based Owner Mapping:** Add a `config.yaml` for user-friendly owner aliases instead of filename-stem discovery.
- [x] **[LOW] Logging Framework:** ✅ `run_all.py` and all pipeline modules now use Python `logging` (configurable via `LOG_LEVEL` env var) instead of `print()`.
- [x] **[LOW] CI/CD Pipeline:** ✅ GitHub Actions workflow added (`.github/workflows/ci.yml`) — runs `pytest` on every push.
- [x] **[LOW] Automated Testing:** ✅ 19 test files across all modules; coverage gate at 75%.
- [x] **[LOW] Module Refactor:** ✅ Monolithic `pipeline/` package split into `ingestion/`, `valuation/`, `analytics/`, `dashboard/`, `ai_advisor/`, and `core/`.
