import streamlit as st
import pandas as pd
import re
import json
import io
from sec_api import XbrlApi

# Initialize the XBRL API
xbrlApi = XbrlApi("a1293bb279cf316f31123670887b10c1fad2c098a90ff5bae1e3868ab327cf8f")

# Function to extract classid
def extract_classid(segment_value):
    match = re.findall(r'C\d{9}', str(segment_value))
    return match[0] if match else None

# 🌟 Streamlit UI
st.set_page_config(page_title="XBRL Expense Extractor", layout="centered")

# **Modern Header**
st.markdown(
    """
    <h1 style="text-align: center; color: #2E86C1;">📊 XBRL Expense Extractor</h1>
    <hr>
    """, unsafe_allow_html=True
)

# **Bullet Point Description**
st.markdown(
    """
    ### 🚀 What This Tool Does:
    - Extracts **Expense Ratios** and **Expenses Paid** from SEC XBRL filings.
    - Upload a **CSV file** containing up to **100 URLs** for processing.
    - Upload a **class mapping CSV** to enrich the extracted data.
    - Automatically organizes the extracted data in a downloadable CSV file.
    - Displays extracted results in real-time with progress tracking.
    
    ⚠️ **Note:** Please **limit the SEC filings upload to 100 URLs** to ensure smooth processing.
    """
)

# **File Uploader Section**
st.markdown("### 🔄 Upload Your CSV Files")
uploaded_filing_csv = st.file_uploader(
    "Upload the SEC Filing URLs CSV (Must contain 'filingURL' column, Max: 100 URLs)",
    type=["csv"],
    help="Ensure your CSV has a 'filingURL' column."
)

uploaded_mapping_csv = st.file_uploader(
    "Upload the Class Mapping CSV (Must contain 'classid', 'Ticker', 'Class Name', 'Series Name', 'Series ID')",
    type=["csv"],
    help="Ensure your CSV has columns: 'classid', 'Ticker', 'Class Name', 'Series Name', 'Series ID'."
)

# Process File if Uploaded
if uploaded_filing_csv is not None and uploaded_mapping_csv is not None:
    df_filing = pd.read_csv(uploaded_filing_csv)
    df_mapping = pd.read_csv(uploaded_mapping_csv)

    if "filingURL" not in df_filing.columns:
        st.error("❌ Error: The SEC Filings CSV must contain a 'filingURL' column.")
    elif len(df_filing) > 100:
        st.error(f"❌ Error: File contains {len(df_filing)} URLs. Please limit to **100 URLs max**.")
    elif not all(col in df_mapping.columns for col in ["classid", "Ticker", "Class Name", "Series Name", "Series ID"]):
        st.error("❌ Error: The Class Mapping CSV is missing required columns.")
    else:
        results = []
        progress_bar = st.progress(0)
        total_urls = len(df_filing)

        st.markdown("### ⏳ Processing Filings...")
        for idx, row in df_filing.iterrows():
            htm_url = row["filingURL"]
            st.write(f"🔍 Fetching data from: `{htm_url}`")

            try:
                # Fetch XBRL data
                xbrl_json = xbrlApi.xbrl_to_json(htm_url=htm_url)
                if not xbrl_json:
                    st.warning(f"⚠️ No data found for `{htm_url}`")
                    continue

                # Extract expenses
                expensepct = pd.json_normalize(xbrl_json.get("ExpenseRatioPct", []))
                expenseamt = pd.json_normalize(xbrl_json.get("ExpensesPaidAmt", []))

                if expensepct.empty or expenseamt.empty:
                    st.warning(f"⚠️ Skipping `{htm_url}`: No Expense Data Found")
                    continue

                # Extract class ID
                expenseamt['classid'] = expenseamt['segment.value'].apply(extract_classid)

                # Merge DataFrames
                combined_expenses = pd.merge(expensepct, expenseamt, left_index=True, right_index=True)
                combined_expenses.rename(columns={'value_x': 'expense_pct', 'value_y': 'expense_amt'}, inplace=True)
                combined_expenses["source_url"] = htm_url

                # Merge with class mapping data
                combined_expenses = combined_expenses.merge(df_mapping, on="classid", how="left")

                # **Reorder Columns**
                ordered_columns = [
                    "classid", "Ticker", "Class Name", "Series Name", "Series ID",
                    "expense_pct", "expense_amt", "period.startDate_y", "period.endDate_y", "source_url"
                ]
                combined_expenses = combined_expenses.reindex(columns=[col for col in ordered_columns if col in combined_expenses.columns])

                # Append to results
                results.append(combined_expenses)

                # Update progress bar
                progress_bar.progress((idx + 1) / total_urls)

            except Exception as e:
                st.warning(f"⚠️ Skipping `{htm_url}` due to error: {e}")

        # Display results
        if results:
            final_df = pd.concat(results, ignore_index=True)
            st.success("✅ Processing Complete! Here's the Extracted Data:")
            st.dataframe(final_df.style.set_properties(**{'text-align': 'center'}))

            # Convert to CSV for download
            csv_data = final_df.to_csv(index=False).encode("utf-8")
            st.download_button(
                "📥 Download Extracted Data",
                csv_data,
                "enriched_expenses.csv",
                "text/csv",
                key="download-btn"
            )
        else:
            st.error("❌ No valid data extracted from the provided URLs.")

# **Footer**
st.markdown(
    """
    <hr>
    <p style="text-align: center; font-size: 14px; color: grey;">
    </p>
    """, unsafe_allow_html=True
)
