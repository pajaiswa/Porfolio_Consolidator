"""
ui_data_management.py — Incremental Data Entry UI
===================================================
Provides the Streamlit interface for uploading files and editing
manual configurations (EPF, FD).
"""
import os
import subprocess
from pathlib import Path

import pandas as pd
import streamlit as st


def save_uploaded_file(uploaded_file, directory: str, filename: str) -> str:
    """Saves a Streamlit UploadedFile to the specified directory with the given filename."""
    path = Path(directory)
    path.mkdir(parents=True, exist_ok=True)
    full_path = path / filename
    with open(full_path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    return str(full_path)


def get_existing_files(directory: str, owner: str, extensions: list[str]) -> list[str]:
    """Finds existing files in a directory that belong to the specified owner."""
    path = Path(directory)
    if not path.exists():
        return []
        
    found = []
    for ext in extensions:
        # Match files like Owner_MF.xlsx or Owner_stocks.pdf
        found.extend([f.name for f in path.glob(f"{owner}_*.{ext}")])
        found.extend([f.name for f in path.glob(f"{owner}.{ext}")])
        
    # Also catch exact generic matches if people didn't use extensions in the glob above
    # Or cases where the owner name is exact prefix
    all_files = [f.name for f in path.glob("*") if f.is_file() and f.name.startswith(owner)]
    
    # Filter by requested extensions
    valid_files = [f for f in all_files if any(f.lower().endswith(f".{ext}") for ext in extensions)]
    return sorted(list(set(valid_files)))


def is_pdf_encrypted(uploaded_file) -> bool:
    """Checks if an uploaded PDF requires a password to open."""
    import pdfplumber
    from pdfminer.pdfdocument import PDFPasswordIncorrect
    try:
        with pdfplumber.open(uploaded_file):
            pass
        return False
    except PDFPasswordIncorrect:
        return True
    except Exception as e:
        if "PDFPasswordIncorrect" in repr(e) or "PdfminerException" in type(e).__name__:
            return True
        return False
    finally:
        uploaded_file.seek(0)

def render_data_management(existing_owners: list[str]) -> None:
    st.title("📁 Data Management")
    st.markdown("Upload statement files, edit manual mappings, and trigger the ingestion pipeline directly.")

    st.markdown("---")

    # 1. OWNER SELECTION
    col1, col2 = st.columns([1, 2])
    with col1:
        owner_options = existing_owners + ["+ Add New Owner..."]
        selected_owner = st.selectbox("Select Portfolio Owner", owner_options)
        if selected_owner == "+ Add New Owner...":
            new_owner = st.text_input("Enter New Owner Name (e.g. Rahul)").strip()
            owner = new_owner.title() if new_owner else ""
        else:
            owner = selected_owner.title()

    if not owner:
        st.warning("Please select or enter an owner to continue.")
        return

    # 2. ASSET SELECTION
    asset_tabs = st.tabs(["Mutual Funds", "Stocks", "NPS", "EPF (Manual)", "Fixed Deposits (Manual)", "🌍 Global Holdings (IBKR)"])

    # --- MUTUAL FUNDS ---
    with asset_tabs[0]:
        st.subheader(f"Upload Mutual Fund Statements for {owner}")
        st.markdown("Supported: **Groww Excel Export** (.xlsx) or **CAMS CAS** (.pdf)")
        
        # Show existing files
        existing_mf = get_existing_files("data/input/mf", owner, ["xlsx", "pdf"])
        if existing_mf:
            st.info(f"📁 **Currently on file:** {', '.join(existing_mf)}")
            st.caption("Upload a new file below only if you want to overwrite the existing one.")
        else:
            st.caption("No Mutual Fund statements found for this owner.")
            
        mf_file = st.file_uploader("Upload MF File", type=["xlsx", "pdf"], key="mf")
        if mf_file:
            ext = mf_file.name.split('.')[-1].lower()
            
            cams_pwd = ""
            if ext == "pdf" and is_pdf_encrypted(mf_file):
                cams_pwd = st.text_input("CAMS Statement Password", type="password", key="cams_pwd", help="Required to parse this encrypted PDF.")
                
            if st.button("Save MF Statement", type="primary", key="save_mf"):
                if ext == "pdf" and cams_pwd:
                    try:
                        from dotenv import set_key
                        set_key(".env", "CAMS_PASSWORD", cams_pwd)
                    except Exception:
                        pass
                
                dest = f"{owner}_MF.{ext}"
                dest_path = Path("data/input/mf") / dest
                try:
                    save_uploaded_file(mf_file, "data/input/mf", dest)
                    
                    if ext == "pdf":
                        import casparser
                        from casparser.exceptions import CASParseError
                        try:
                            # Validate that it is actually a CAMS CAS statement and password works
                            casparser.read_cas_pdf(str(dest_path), cams_pwd, force_pdfminer=True)
                        except CASParseError as e:
                            dest_path.unlink(missing_ok=True)
                            if "password" in str(e).lower():
                                st.error("Validation Failed: Incorrect CAMS password.")
                            else:
                                st.error(f"Validation Failed: Not a valid CAMS CAS statement. ({e})")
                            return
                        except Exception as e:
                            dest_path.unlink(missing_ok=True)
                            st.error(f"Validation Failed: Not a valid CAMS CAS statement. ({e})")
                            return

                    st.success(f"Saved: `data/input/mf/{dest}`")
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to save: {e}")

    # --- STOCKS ---
    with asset_tabs[1]:
        st.subheader(f"Upload Stock Orders for {owner}")
        st.markdown("Supported: **Groww Stock Order Export** (.xlsx)")
        
        existing_stocks = get_existing_files("data/input/stock", owner, ["xlsx"])
        if existing_stocks:
            st.info(f"📁 **Currently on file:** {', '.join(existing_stocks)}")
            st.caption("Upload a new file below only if you want to overwrite the existing one.")
        else:
            st.caption("No Stock order statements found for this owner.")
            
        stock_file = st.file_uploader("Upload Stock Excel", type=["xlsx"], key="stock")
        if stock_file:
            if st.button("Save Stock Statement", type="primary", key="save_stock"):
                dest = f"{owner}_stocks.xlsx"
                try:
                    save_uploaded_file(stock_file, "data/input/stock", dest)
                    st.success(f"Saved: `data/input/stock/{dest}`")
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to save: {e}")

    # --- NPS ---
    with asset_tabs[2]:
        st.subheader(f"Upload NPS Statement for {owner}")
        st.markdown("Supported: **KFintech/Protean NPS PDF** (.pdf)")
        
        existing_nps = get_existing_files("data/input/nps", owner, ["pdf"])
        if existing_nps:
            st.info(f"📁 **Currently on file:** {', '.join(existing_nps)}")
            st.caption("Upload a new file below only if you want to overwrite the existing one.")
        else:
            st.caption("No NPS statements found for this owner.")
            
        nps_file = st.file_uploader("Upload NPS PDF", type=["pdf"], key="nps")
        if nps_file:
            nps_pwd = ""
            if is_pdf_encrypted(nps_file):
                nps_pwd = st.text_input("NPS Statement Password", type="password", key="nps_pwd", help="Required to parse this encrypted PDF.")
                
            if st.button("Save NPS Statement", type="primary", key="save_nps"):
                if nps_pwd:
                    try:
                        from dotenv import set_key
                        set_key(".env", "NPS_PASSWORD", nps_pwd)
                    except Exception:
                        pass
                        
                dest = f"{owner}_NPS.pdf"
                dest_path = Path("data/input/nps") / dest
                try:
                    save_uploaded_file(nps_file, "data/input/nps", dest)
                    
                    import pdfplumber
                    is_valid_nps = False
                    try:
                        with pdfplumber.open(str(dest_path), password=nps_pwd) as pdf:
                            # Quick sanity check on the first page text
                            text = pdf.pages[0].extract_text() or ""
                            text_up = text.upper()
                            if any(k in text_up for k in ["NATIONAL PENSION SYSTEM", "NPS", "KFINTECH", "CRA", "PROTEAN", "NSDL"]):
                                is_valid_nps = True
                    except Exception as e:
                        dest_path.unlink(missing_ok=True)
                        st.error(f"Validation Failed: Incorrect NPS password or unreadable PDF. ({e})")
                        return
                        
                    if not is_valid_nps:
                        dest_path.unlink(missing_ok=True)
                        st.error("Validation Failed: Missing NPS keywords. Are you sure this is an NPS Statement?")
                        return

                    st.success(f"Saved: `data/input/nps/{dest}`")
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to save: {e}")

    # --- EPF ---
    with asset_tabs[3]:
        st.subheader("EPF Master Configuration")
        st.markdown("EPF records are maintained in a central CSV file since they aren't parsed from PDFs.")
        epf_path = Path("data/input/EPF/epf_config.csv")
        epf_df = pd.DataFrame()
        if epf_path.exists():
            epf_df = pd.read_csv(epf_path)
            
        edited_epf = st.data_editor(epf_df, num_rows="dynamic", width="stretch", key="epf_grid")
        if st.button("Save EPF Changes", type="primary", key="save_epf"):
            epf_path.parent.mkdir(parents=True, exist_ok=True)
            edited_epf.to_csv(epf_path, index=False)
            st.success(f"EPF config saved successfully with {len(edited_epf)} rows.")

    # --- FIXED DEPOSITS ---
    with asset_tabs[4]:
        st.subheader("Fixed Deposit Master Configuration")
        fd_path = Path("data/input/FD/FD_details.xlsx")
        fd_df = pd.DataFrame()
        if fd_path.exists():
            fd_df = pd.read_excel(fd_path)
            
        edited_fd = st.data_editor(fd_df, num_rows="dynamic", width="stretch", key="fd_grid")
        if st.button("Save FD Changes", type="primary", key="save_fd"):
            fd_path.parent.mkdir(parents=True, exist_ok=True)
            edited_fd.to_excel(fd_path, index=False)
            st.success(f"FD config saved successfully with {len(edited_fd)} rows.")

    # --- GLOBAL HOLDINGS (IBKR) ---
    with asset_tabs[5]:
        st.subheader("Global Holdings — IBKR Broker")
        st.markdown(
            "Track your international positions held in **Interactive Brokers (IBKR)**. "
            "Each row is **one transaction** (Buy or Sell). "
            "Live USD prices are fetched from yfinance and converted to INR via the live USD/INR rate."
        )
        st.info(
            "📌 **Ticker must be a valid yfinance symbol.** "
            "Examples: `VWRA.L` (Vanguard All-World, LSE/USD), `VTI` (NYSE), `QQQ` (NASDAQ). "
            "Add one row per trade — do not aggregate multiple purchases into one row."
        )

        global_path = Path("data/input/global/global_transactions.csv")
        global_df = pd.DataFrame(
            columns=["Owner", "Ticker", "Asset_Name", "Transaction_Type", "Shares", "INR_Amount", "Trade_Date"]
        )
        if global_path.exists():
            loaded = pd.read_csv(global_path).dropna(how="all")
            if not loaded.empty:
                # Convert Trade_Date strings → Python date objects (required by st.column_config.DateColumn)
                if "Trade_Date" in loaded.columns:
                    loaded["Trade_Date"] = pd.to_datetime(
                        loaded["Trade_Date"], format="mixed", dayfirst=True, errors="coerce"
                    ).dt.date
                global_df = loaded


        # Pre-fill the Owner column with the currently selected owner for convenience
        edited_global = st.data_editor(
            global_df,
            num_rows="dynamic",
            width="stretch",
            key="global_grid",
            column_config={
                "Owner": st.column_config.TextColumn(
                    "Owner", help="Portfolio owner name (e.g. Pankaj)", default=owner
                ),
                "Ticker": st.column_config.TextColumn(
                    "Ticker (yfinance)", help="e.g. VWRA.L for LSE, VTI for NYSE"
                ),
                "Asset_Name": st.column_config.TextColumn(
                    "Asset Name", help="Human-readable name (e.g. Vanguard FTSE All-World)"
                ),
                "Transaction_Type": st.column_config.SelectboxColumn(
                    "Type", options=["Buy", "Sell"], required=True
                ),
                "Shares": st.column_config.NumberColumn(
                    "Shares", min_value=0.0, format="%.4f", help="Number of shares/units in this trade"
                ),
                "INR_Amount": st.column_config.NumberColumn(
                    "INR Amount (₹)", min_value=0.0, format="₹ %.2f",
                    help="Total INR paid for this trade (including brokerage)"
                ),
                "Trade_Date": st.column_config.DateColumn(
                    "Trade Date", help="Date of this transaction"
                ),
            },
        )

        if st.button("Save Global Holdings", type="primary", key="save_global"):
            errors = []
            df_to_save = edited_global.dropna(how="all").copy()

            if df_to_save.empty:
                st.warning("No rows to save.")
            else:
                # Validation
                for idx, row in df_to_save.iterrows():
                    if not str(row.get("Ticker", "")).strip():
                        errors.append(f"Row {idx+1}: Ticker is required.")
                    if not str(row.get("Owner", "")).strip():
                        errors.append(f"Row {idx+1}: Owner is required.")
                    try:
                        shares = float(row.get("Shares", 0))
                        if shares <= 0:
                            errors.append(f"Row {idx+1}: Shares must be greater than 0.")
                    except (TypeError, ValueError):
                        errors.append(f"Row {idx+1}: Shares must be a valid number.")
                    try:
                        amt = float(row.get("INR_Amount", 0))
                        if amt <= 0:
                            errors.append(f"Row {idx+1}: INR Amount must be greater than 0.")
                    except (TypeError, ValueError):
                        errors.append(f"Row {idx+1}: INR Amount must be a valid number.")
                    if str(row.get("Transaction_Type", "")).strip() not in ("Buy", "Sell"):
                        errors.append(f"Row {idx+1}: Transaction Type must be 'Buy' or 'Sell'.")

                if errors:
                    for err in errors:
                        st.error(err)
                else:
                    global_path.parent.mkdir(parents=True, exist_ok=True)
                    df_to_save.to_csv(global_path, index=False)
                    st.success(f"Global Holdings saved — {len(df_to_save)} transaction(s) recorded.")
                    st.rerun()


    # 3. PIPELINE EXECUTION HUB
    st.markdown("---")
    st.subheader("🚀 Run Consolidation Pipeline")
    st.markdown("After updating inputs, run the pipeline to consolidate ledgers, fetch live NAVs, and recalculate XIRR. **Wait for the success message before switching to the Dashboard view.**")

    if st.button("Update Dashboard (Run Pipeline)", type="primary"):
        with st.status("Executing `run_all.py`...", expanded=True) as status:
            try:
                # Capture standard output directly to stream it on UI
                result = subprocess.run(
                    ["uv", "run", "python", "run_all.py"],
                    capture_output=True,
                    text=True,
                    check=False
                )
                
                # Render standard output and error using purely Streamlit components
                if result.stdout:
                    st.code(result.stdout, language="text")
                if result.stderr:
                    st.code(result.stderr, language="text")
                    
                if result.returncode == 0:
                    status.update(label="Pipeline Completed Successfully! 🎉", state="complete", expanded=False)
                    st.balloons()
                else:
                    status.update(label="Pipeline Failed. See logs above.", state="error", expanded=True)
                    st.error("There was an error in execution. Check the logs.")
                    
            except Exception as ex:
                status.update(label="System Error", state="error", expanded=True)
                st.error(f"Could not trigger process: {ex}")

