# Sample Data

This directory contains **anonymised, dummy sample data** to help you get started with the Portfolio Consolidator pipeline. All values are fictitious and do not represent real investments.

---

## Directory Structure

```
sample_data/
├── mf/
│   └── Rahul_MF.xlsx          ← Groww MF order export (owner name = "Rahul")
├── stock/
│   └── Rahul_stocks.xlsx       ← Groww stock order export
├── EPF/
│   └── epf_config.csv          ← Manual EPF config (closing balance + interest rate)
├── FD/
│   └── FD_details.xlsx         ← Fixed Deposit details
├── NPS/
│   └── (PDF required)          ← KFintech NPS PDF — cannot be provided as sample
│                                  See note below.
└── global/
    └── global_transactions.csv ← Manual IBKR / global holdings ledger
```

---

## How to Use

1. **Copy** any sample file to its corresponding `data/input/<subfolder>/` directory.
2. **Rename** the file so the filename starts with the **Portfolio Owner name** (e.g. `Rahul_MF.xlsx` → owner = "Rahul").
3. **Fill in your real data** following the same column structure as shown in the sample.
4. For PDF-based inputs (MF CAMS CAS, NPS KFintech), place the file in the correct folder and set the password in `.env`.

---

## File Format Reference

### EPF — `EPF/epf_config.csv`
| Column | Description |
|---|---|
| Owner | Portfolio owner name (must match across all files) |
| Asset_Name | Human label, e.g. "EPF - Rahul (Company Name)" |
| Closing_Balance | Latest EPF balance in ₹ |
| As_Of_Date | Date of the balance snapshot (YYYY-MM-DD) |
| Interest_Rate_Pct | Current EPF interest rate, e.g. 8.25 |

### FD — `FD/FD_details.xlsx`
| Column | Description |
|---|---|
| Owner | Portfolio owner name |
| FD Start Date | Date the FD was opened (DD-MM-YYYY) |
| Maturity Date | Date the FD matures (DD-MM-YYYY) |
| Invested Amount | Principal invested in ₹ |
| Interest Rate | Annual interest rate (%) |
| Bank | Name of the bank |

### Global Holdings — `global/global_transactions.csv`
| Column | Description |
|---|---|
| Owner | Portfolio owner name |
| Ticker | Exchange ticker (e.g. VTI, AAPL) |
| Asset_Name | Full name of the security |
| Transaction_Type | Buy or Sell |
| Shares | Number of shares |
| INR_Amount | Value in ₹ at the time of trade |
| Trade_Date | Date of trade (YYYY-MM-DD) |

### MF — `mf/Rahul_MF.xlsx`
This is a **Groww mutual fund order export**. Download it directly from your Groww account:
> Groww → Profile → Reports → Mutual Fund Orders → Export

### Stock — `stock/Rahul_stocks.xlsx`
This is a **Groww stock order export**. Download it directly from your Groww account:
> Groww → Profile → Reports → Stock Orders → Export

### NPS — PDF Statement
NPS statements are password-protected PDFs from **KFintech**. They cannot be provided as samples. Log in to your NPS account and download your transaction statement. Set `NPS_PASSWORD=<your_dob>` in your `.env` file.

---

> **Note:** The `sample_data/` directory is committed to git as a reference. Your personal data files must be placed in `data/input/` which is covered by `.gitignore`.
