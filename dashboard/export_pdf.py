import os
import tempfile
import pandas as pd
from fpdf import FPDF

class PortfolioPDF(FPDF):
    def header(self):
        self.set_font('helvetica', 'B', 15)
        self.cell(0, 10, 'Family Portfolio Analysis Report', new_x="LMARGIN", new_y="NEXT", align='C')
        self.ln(5)

    def footer(self):
        self.set_y(-15)
        self.set_font('helvetica', 'I', 8)
        self.cell(0, 10, f'Page {self.page_no()}/{{nb}}', align='C')

def generate_pdf_report(owner_title: str, metrics: dict, figures: dict, df_raw: pd.DataFrame) -> str:
    """
    Generates a PDF report containing high-level metrics, Plotly chart images, 
    and appends the raw dataframe at the bottom.
    Returns the absolute path to the generated PDF.
    """
    temp_dir = tempfile.gettempdir()
    
    # Save figures as temporary PNG images
    img_paths = {}
    for name, fig in figures.items():
        if fig is not None:
            # We must use safe filenames
            img_path = os.path.join(temp_dir, f"{name.replace(' ', '_')}.png")
            try:
                # scale=2 for better resolution inside the PDF
                fig.write_image(img_path, format='png', width=1000, height=600, scale=2)
                img_paths[name] = img_path
            except Exception as e:
                import logging
                logging.getLogger(__name__).warning(f"Skipping image {name} due to error: {e}")

    pdf = PortfolioPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    
    # 1. Title and Filter Info
    pdf.set_font("helvetica", "B", 12)
    pdf.cell(0, 10, f"Dashboard View: {owner_title}", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(5)
    
    # 2. Key Metrics
    pdf.set_font("helvetica", "", 11)
    for key, value in metrics.items():
        # Replace Rupee symbol with Rs. as standard PDF fonts don't support the unicode character
        safe_val = str(value).replace('₹', 'Rs.')
        pdf.cell(0, 8, f"{key}: {safe_val}", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(5)
    
    # 3. Charts
    pdf.set_font("helvetica", "B", 14)
    pdf.cell(0, 10, "Visual Analysis", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(5)
    
    for name, path in img_paths.items():
        if os.path.exists(path):
            pdf.set_font("helvetica", "I", 10)
            pdf.cell(0, 8, name, new_x="LMARGIN", new_y="NEXT")
            # Width=180 fits standard A4 with 15mm margins
            pdf.image(path, w=180)
            pdf.ln(10)
            
    # 4. Raw Data Table
    pdf.add_page()
    pdf.set_font("helvetica", "B", 14)
    pdf.cell(0, 10, "Raw Data Reference", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(5)
    
    if not df_raw.empty:
        # FPDF2 provides table rendering
        pdf.set_font("helvetica", "", 8)
        
        # Pick relevant columns to fit on A4 securely
        desired_cols = ['Asset Name', 'Asset Class', 'Units', 'Current Value']
        cols = [c for c in desired_cols if c in df_raw.columns]
        
        # Calculate column widths
        col_widths = []
        for c in cols:
            if c == 'Asset Name':
                col_widths.append(85)
            elif c == 'Current Value':
                col_widths.append(30)
            else:
                col_widths.append(35)
                
        with pdf.table(col_widths=col_widths, text_align="LEFT") as table:
            # Header
            header_row = table.row()
            for col in cols:
                header_row.cell(col)
            
            # Limit rows if too many to prevent giant PDFs blocking memory, say top 150 
            top_raw = df_raw.head(150)
            
            # Rows
            for _, r in top_raw.iterrows():
                row = table.row()
                for c in cols:
                    val = str(r[c])
                    if c == 'Current Value':
                        try:
                            val = f"Rs {float(r[c]):,.2f}"
                        except:
                            pass
                    if c == 'Asset Name' and len(val) > 50:
                        val = val[:47] + "..."
                    row.cell(val)
                    
            if len(df_raw) > 150:
                footer_row = table.row()
                footer_row.cell(f"... and {len(df_raw)-150} more records (truncated for PDF)")
                for _ in range(len(cols)-1):
                    footer_row.cell("")
    else:
        pdf.set_font("helvetica", "", 10)
        pdf.cell(0, 10, "No raw data available for the current selection.", new_x="LMARGIN", new_y="NEXT")

    out_path = os.path.join(temp_dir, "Portfolio_Export.pdf")
    pdf.output(out_path)
    
    # Cleanup temp images
    for path in img_paths.values():
        if os.path.exists(path):
            try:
                os.remove(path)
            except Exception:
                pass
            
    return out_path
