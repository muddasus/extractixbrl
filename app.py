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

# Streamlit UI
st.title("📊 Expense Extraction App")

# Form Type Dropdown
form_type = st.selectbox("Select Form Type", ["N-CSR", "N-CSRS"])

# Date Pickers
from_date = st.date_input("From Date")
to_date = st.date_input("To Date")

# File Uploader
uploaded_file = st.file_uploader("Upload an Excel file with filing URLs", type=["csv"])

# Submit Button
if st.button("Submit"):
    if uploaded_file is not None:
        # Read the CSV file
        df = pd.read_csv(uploaded_file)
        if "filingURL" not in df.columns:
            st.error("CSV must contain a column named 'filingURL'.")
        else:
            results = []
            progress_bar = st.progress(0)
            total_urls = len(df)

            for idx, row in df.iterrows():
                htm_url = row["filingURL"]
                st.write(f"Processing: {htm_url}")

                try:
                    # Fetch XBRL data
                    xbrl_json = xbrlApi.xbrl_to_json(htm_url=htm_url)
                    if not xbrl_json:
                        continue

                    # Extract expenses
                    expensepct = pd.json_normalize(xbrl_json.get("ExpenseRatioPct", []))
                    expenseamt = pd.json_normalize(xbrl_json.get("ExpensesPaidAmt", []))

                    if expensepct.empty or expenseamt.empty:
                        continue

                    # Extract class ID
                    expenseamt['classid'] = expenseamt['segment.value'].apply(extract_classid)

                    # Merge DataFrames
                    combined_expenses = pd.merge(expensepct, expenseamt, left_index=True, right_index=True)
                    combined_expenses.rename(columns={'value_x': 'expense_pct', 'value_y': 'expense_amt'}, inplace=True)
                    combined_expenses["source_url"] = htm_url

                    # Append to results
                    results.append(combined_expenses)

                    # Update progress bar
                    progress_bar.progress((idx + 1) / total_urls)

                except Exception as e:
                    st.warning(f"Skipping {htm_url}: {e}")

            # Display results
            if results:
                final_df = pd.concat(results, ignore_index=True)
                st.success("Processing complete! Here's the extracted data:")
                st.dataframe(final_df)

                # Convert to CSV for download
                csv_data = final_df.to_csv(index=False).encode("utf-8")
                st.download_button("Download Extracted Data", csv_data, "extracted_expenses.csv", "text/csv")
            else:
                st.error("No valid data extracted from the provided URLs.")
