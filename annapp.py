import streamlit as st
import pandas as pd
from sec_api import QueryApi, XbrlApi

# Your sec-api.io API Key (Replace with your actual key)
API_KEY = "a1293bb279cf316f31123670887b10c1fad2c098a90ff5bae1e3868ab327cf8f"

# Initialize API clients
queryApi = QueryApi(api_key=API_KEY)
xbrlApi = XbrlApi(api_key=API_KEY)

# Function to get the latest filing with Avg Annual Return (oef:AvgAnnlRtrPct)
def get_filing_cik(cik):
    query = {
        "query": f"formType:(N-CSR OR N-CSRS) AND cik:{cik}",
        "from": "0",
        "size": "1",
        "sort": [{"filedAt": {"order": "desc"}}]
    }
    
    response = queryApi.get_filings(query)
    
    if response.get("filings"):
        return response["filings"][0]["id"]
    return None

# Function to extract Avg Annual Return from XBRL
def fetch_avg_annual_return(cik):
    filing_id = get_filing_cik(cik)
    if not filing_id:
        return None

    xbrl_data = xbrlApi.xbrl(filing_id)
    
    results = []
    for key, item in xbrl_data["facts"].items():
        if "oef:AvgAnnlRtrPct" in key:
            value = item.get("value", "N/A")
            period = item.get("period", "N/A")
            months = int(period.split(" ")[0]) if period != "N/A" else None
            
            results.append({"CIK": cik, "Period (Months)": months, "Avg Annual Return": value})
    
    return results if results else None

# Streamlit App
st.title("📈 SEC Mutual Fund Annual Return Tracker (sec-api.io)")

# User input for CIKs
cik_input = st.text_input("Enter Fund CIKs (comma-separated):", "0001166559, 0000936754, 0000315066")
cik_list = [cik.strip() for cik in cik_input.split(",")]

if st.button("Fetch Data"):
    all_fund_data = []

    for cik in cik_list:
        st.write(f"🔍 Fetching data for CIK: {cik}")
        fund_data = fetch_avg_annual_return(cik)

        if fund_data:
            all_fund_data.extend(fund_data)
            st.success(f"✅ Extracted {len(fund_data)} records for CIK {cik}")
        else:
            st.warning(f"⚠️ No data found for CIK {cik}")

    # Convert to DataFrame
    if all_fund_data:
        df = pd.DataFrame(all_fund_data)
        st.dataframe(df)

        # Download as CSV
        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button(label="📥 Download CSV", data=csv, file_name="avg_annual_returns.csv", mime="text/csv")
    else:
        st.error("❌ No data available. Please check the CIK numbers and try again.")
