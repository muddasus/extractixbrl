import streamlit as st
import pandas as pd
import re
import os
from sec_api import QueryApi, XbrlApi
sec_filing_urls = []  
# SEC API Keys
QUERY_API_KEY = "a1293bb279cf316f31123670887b10c1fad2c098a90ff5bae1e3868ab327cf8f"
xbrlApi = XbrlApi(QUERY_API_KEY)
queryApi = QueryApi(QUERY_API_KEY)

CLASS_MAPPING_FILE = "class_series_mapping.csv"

# Function to extract classid
def extract_classid(segment_value):
    match = re.findall(r'C\d{9}', str(segment_value))
    return match[0] if match else None

def fetch_filings(form_types, from_date, to_date):
    """Fetches filings from SEC based on form type and date range."""
    st.write("🔍 Fetching filings from SEC...")
    
    form_query = " OR ".join([f'formType:"{ft}"' for ft in form_types])
    search_query = f'({form_query}) AND filedAt:[{from_date} TO {to_date}]'
    
    search_params = {
        "query": search_query,
        "from": "0",
        "size": "50",
        "sort": [{"filedAt": {"order": "desc"}}],
    }
    
    filing_urls = []
    
    try:
        while True:
            search_results = queryApi.get_filings(search_params)
            if not search_results or "filings" not in search_results:
                break

            filings = search_results["filings"]
            if not filings:
                break

            metadata = [
                {
                    "ticker": f.get("ticker", "N/A"),
                    "cik": f.get("cik", "Unknown"),
                    "filedAt": f.get("filedAt", "Unknown"),
                    "accessionNo": f.get("accessionNo", "Unknown"),
                    "filingURL": f.get("linkToFilingDetails", "No URL"),
                }
                for f in filings
            ]
            filing_urls.extend(metadata)
            search_params["from"] = str(int(search_params["from"]) + int(search_params["size"]))

        df = pd.DataFrame(filing_urls)
        if not df.empty:
            st.success(f"✅ Successfully retrieved {len(df)} filings from SEC.")
        return df

    except Exception as e:
        st.error(f"❌ Error fetching filings: {str(e)}")
        return pd.DataFrame()

def extract_ixbrl_data(filing_url):
    """Extracts IXBRL data from a given filing URL."""
    try:
        xbrl_json = xbrlApi.xbrl_to_json(htm_url=filing_url)
       
        if "ExpenseRatioPct" not in xbrl_json or "ExpensesPaidAmt" not in xbrl_json:
            return pd.DataFrame()

        expensepct = pd.json_normalize(xbrl_json.get("ExpenseRatioPct", {}))
        expenseamt = pd.json_normalize(xbrl_json.get("ExpensesPaidAmt", {}))

        if "segment.value" in expenseamt.columns:
            expenseamt["classid"] = expenseamt["segment.value"].apply(extract_classid)

        combined_expenses = pd.merge(expensepct, expenseamt, left_index=True, right_index=True)
        combined_expenses = combined_expenses.rename(columns={"value_x": "expense_pct", "value_y": "expense_amt"})
        combined_expenses["filingURL"] = filing_url

        return combined_expenses

    except Exception as e:
        st.warning(f"⚠️ Error extracting IXBRL data from {filing_url}: {str(e)}")
        return pd.DataFrame()

def process_filings(sec_filing_urls):
    """Processes a list of SEC filing URLs by extracting IXBRL data."""
    results = []
    progress_bar = st.progress(0)
    extracted_data_placeholder = st.empty()

    for idx, htm_url in enumerate(sec_filing_urls):
        extracted_data = extract_ixbrl_data(htm_url)
        if not extracted_data.empty:
            results.append(extracted_data)
            extracted_data_placeholder.dataframe(pd.concat(results, ignore_index=True))

        progress_bar.progress((idx + 1) / len(sec_filing_urls))

    return pd.concat(results, ignore_index=True) if results else pd.DataFrame()

# 🌟 Streamlit UI
st.set_page_config(page_title="XBRL Expense Extractor", layout="centered")

st.markdown(
    """
    <h1 style="text-align: center; color: #2E86C1;">📊 XBRL Expense Extractor</h1>
    <hr>
    """, unsafe_allow_html=True
)

st.markdown(
    """
    ### 🚀 What This Tool Does:
    - **Option 1**: Upload a CSV file with filing URLs  
    - **Option 2**: Fetch filings dynamically from SEC using **Form Type & Dates**
    - Automatically enriches extracted data using an internal class mapping file.
    - Displays live extracted data while processing.
    """
)

data_input_method = st.radio("Choose Processing Method:", ("Upload CSV File", "Fetch from SEC by Form Type & Date"))

uploaded_filing_csv = None
form_types, from_date, to_date = None, None, None

if data_input_method == "Upload CSV File":
    uploaded_filing_csv = st.file_uploader("Upload SEC Filings CSV", type=["csv"])
elif data_input_method == "Fetch from SEC by Form Type & Date":
    form_types = st.multiselect("📄 Select Form Type(s)", ["N-CSR", "N-CSRS"])
    col1, col2 = st.columns(2)
    with col1:
        from_date = st.date_input("📅 From Date")
    with col2:
        to_date = st.date_input("📅 To Date")

if st.button("🚀 Submit & Process Data"):
    if not os.path.exists(CLASS_MAPPING_FILE):
        st.error(f"❌ Required file '{CLASS_MAPPING_FILE}' is missing!")
    else:
        df_mapping = pd.read_csv(CLASS_MAPPING_FILE)
        required_columns = ["classid", "Ticker", "Class Name", "Series Name", "Series ID"]

        if not all(col in df_mapping.columns for col in required_columns):
            st.error(f"❌ '{CLASS_MAPPING_FILE}' is missing required columns.")
        else:
            if uploaded_filing_csv:
                df_filing = pd.read_csv(uploaded_filing_csv)
                if "filingURL" not in df_filing.columns:
                    st.error("❌ CSV must contain a 'filingURL' column.")
                elif len(df_filing) > 100:
                    st.error("❌ File exceeds 100 URLs limit.")
                else:
                    sec_filing_urls = df_filing["filingURL"].tolist()
            elif form_types and from_date and to_date:
                df_filings = fetch_filings(form_types, from_date, to_date)
                if df_filings.empty:
                    st.error("❌ No filings retrieved from SEC.")
                else:
                    sec_filing_urls = df_filings["filingURL"].tolist()
            else:
                st.error("❌ Select a valid input method.")

            extracted_df = process_filings(sec_filing_urls)

            if not extracted_df.empty:
                extracted_df = extracted_df.merge(df_mapping, on="classid", how="left")
                extracted_df = extracted_df.reindex(columns=["classid", "Ticker", "Class Name", "Series Name", "Series ID", "expense_pct", "expense_amt", "period.startDate_y", "period.endDate_y", "filingURL"])

                st.success(f"✅ Successfully processed {len(extracted_df)} records!")
                st.dataframe(extracted_df)
                csv_data = extracted_df.to_csv(index=False).encode("utf-8")
                st.download_button("📥 Download Extracted Data", csv_data, "extracted_expenses.csv", "text/csv")
            else:
                st.error("❌ No valid data extracted.")

st.markdown("<hr><p style='text-align:center;'>Built with ❤️ using Streamlit | SEC API Integration</p>", unsafe_allow_html=True)
