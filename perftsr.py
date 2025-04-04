import streamlit as st
import pandas as pd
import re
import asyncio
import aiohttp
import os
import matplotlib.pyplot as plt
import seaborn as sns
from sec_api import QueryApi, XbrlApi

# SEC API Key
API_KEY = "a1293bb279cf316f31123670887b10c1fad2c098a90ff5bae1e3868ab327cf8f"
queryApi = QueryApi(API_KEY)
xbrlApi = XbrlApi(API_KEY)

# File path for class mapping
CLASS_MAPPING_FILE = "class_series_mapping.csv"

# Helper to extract classid from segment value
def extract_classid(segment_value):
    match = re.findall(r'C\d{9}', str(segment_value))
    return match[0] if match else None

# SEC Filing Search Function
def fetch_filing_metadata(form_types, from_date, to_date, limit):
    st.write("🔍 Searching SEC filings...")
    form_query = " OR ".join([f'formType:"{ft}"' for ft in form_types])
    search_query = f'({form_query}) AND filedAt:[{from_date} TO {to_date}]'

    search_params = {
        "query": search_query,
        "from": "0",
        "size": "50",
        "sort": [{"filedAt": {"order": "desc"}}],
    }

    filing_metadata = []
    try:
        while len(filing_metadata) < limit:
            result = queryApi.get_filings(search_params)
            filings = result.get("filings", [])
            if not filings:
                break

            for f in filings:
                filing_metadata.append({
                    "Filing URL": f.get("linkToFilingDetails"),
                    "Filed At": f.get("filedAt"),
                    "Ticker": f.get("ticker", "N/A")
                })

            if len(filing_metadata) >= limit:
                break
            search_params["from"] = str(int(search_params["from"]) + int(search_params["size"]))

        return filing_metadata[:limit]

    except Exception as e:
        st.error(f"❌ Error during SEC query: {e}")
        return []

# Async performance check with classid, expenses, and performance extraction
def process_filing_with_details(filing_url):
    try:
        xbrl_data = xbrlApi.xbrl_to_json(htm_url=filing_url)
        perf_present = "AvgAnnlRtrTableTextBlock" in xbrl_data

        expense_amt_data = xbrl_data.get("ExpensesPaidAmt", [])
        performance_data = xbrl_data.get("AvgAnnlRtrPct", [])

        df_exp = pd.json_normalize(expense_amt_data)
        df_perf = pd.json_normalize(performance_data)

        if "segment.value" in df_exp.columns:
            df_exp["classid"] = df_exp["segment.value"].apply(extract_classid)
            df_exp = df_exp.dropna(subset=["classid"])
            df_exp = df_exp.rename(columns={"value": "expense_amt"})

        if "segment.value" in df_perf.columns:
            df_perf["classid"] = df_perf["segment.value"].apply(extract_classid)
            df_perf = df_perf.dropna(subset=["classid"])
            df_perf = df_perf.rename(columns={"value": "performance_pct"})

        if df_exp.empty and not df_perf.empty:
            df_exp = pd.DataFrame({"classid": df_perf["classid"], "expense_amt": [None]*len(df_perf)})
        elif df_perf.empty and not df_exp.empty:
            df_perf = pd.DataFrame({"classid": df_exp["classid"], "performance_pct": [None]*len(df_exp)})
        elif df_exp.empty and df_perf.empty:
            return pd.DataFrame()

        df_combined = pd.merge(df_exp, df_perf, on="classid", how="outer")
        skipped_before = len(df_combined)
        df_combined = df_combined.dropna(subset=["classid"])
        skipped_after = len(df_combined)
        skipped_count = skipped_before - skipped_after
        if skipped_count > 0:
            st.info(f"ℹ️ Skipped {skipped_count} rows with missing classid in filing: {filing_url}")

        df_combined["Filing URL"] = filing_url
        df_combined["Has Performance Data"] = perf_present

        return df_combined[["classid", "Filing URL", "Has Performance Data", "expense_amt", "performance_pct"]]

    except Exception as e:
        st.warning(f"⚠️ Failed to process filing: {filing_url} — {e}")
        return pd.DataFrame()

async def check_all_filings(metadata):
    loop = asyncio.get_event_loop()
    results = []
    for idx, entry in enumerate(metadata):
        df = await loop.run_in_executor(None, process_filing_with_details, entry["Filing URL"])
        if isinstance(df, pd.DataFrame) and not df.empty:
            df["Filed At"] = entry["Filed At"]
            df["Ticker"] = entry["Ticker"]
            results.append(df)
    return pd.concat(results, ignore_index=True) if results else pd.DataFrame()

# Streamlit UI
st.set_page_config(page_title="Performance Disclosure Checker", layout="wide")

st.markdown("""
    <div style='background-color:#eaf2f8;padding:2vw;border-radius:10px;width:100%;box-sizing:border-box;max-width:100%;overflow-x:auto;'>
    <h1 style='text-align:center;color:#2E86C1;'>📊 Mutual Fund Performance & Expense Disclosure Analyzer</h1>
    <p style='text-align:center;font-size:16px;'>Analyze iXBRL-tagged SEC filings to understand how funds report performance and expenses.</p>
    </div>
    <br>
""", unsafe_allow_html=True)

with st.sidebar:
    st.header("🔧 Search Filters")
    form_types = st.multiselect("📄 Select Form Type(s)", ["N-CSR", "N-CSRS"])
    from_date = st.date_input("🗓️ From Date")
    to_date = st.date_input("🗓️ To Date")
    limit = st.selectbox("🔢 Number of filings to check", options=[5, 20, 50, 100, 200, 300, 500], index=1)

submit_clicked = st.sidebar.button("🚀 Submit")

if submit_clicked:
    if form_types and from_date and to_date:
        if not os.path.exists(CLASS_MAPPING_FILE):
            st.error(f"❌ Required file '{CLASS_MAPPING_FILE}' is missing!")
        else:
            df_mapping = pd.read_csv(CLASS_MAPPING_FILE)
            if "classid" not in df_mapping.columns:
                st.error("❌ Mapping file must include 'classid' column")
            else:
                metadata = fetch_filing_metadata(form_types, from_date, to_date, limit)
                if not metadata:
                    st.error("❌ No filings found.")
                else:
                    st.info(f"🔄 Checking {len(metadata)} filings for performance information...")
                    progress_placeholder = st.empty()
                    table_placeholder = st.empty()
                    df_results_list = []
                    combined_df = pd.DataFrame()

                    for idx, entry in enumerate(metadata):
                        df = asyncio.run(check_all_filings([entry]))
                        if not df.empty:
                            df_results_list.append(df)
                            combined_df = pd.concat([combined_df, df], ignore_index=True)
                            table_placeholder.dataframe(combined_df)
                        progress_placeholder.progress((idx + 1) / len(metadata))

                    df_results = combined_df
                    st.session_state.df_results = df_results

                    if df_results.empty:
                        st.error("❌ No valid data extracted.")
                    else:
                        df_results = df_results.merge(df_mapping, on="classid", how="left")

                        perf_count = df_results["Has Performance Data"].sum()
                        total_classes = len(df_results)
                        class_pct = round((perf_count / total_classes) * 100, 2)
                        st.success(f"✅ {perf_count} out of {total_classes} share classes ({class_pct}%) disclose performance information.")

                        if "Entity Name" in df_results.columns:
                            entity_perf = df_results.groupby("Entity Name")["Has Performance Data"].any().reset_index()
                            entity_count = len(entity_perf)
                            entity_disclosing = entity_perf["Has Performance Data"].sum()
                            entity_pct = round((entity_disclosing / entity_count) * 100, 2)
                            st.info(f"🏢 {entity_disclosing} out of {entity_count} entities ({entity_pct}%) disclose performance information.")

                        with st.expander("📄 Detailed Results", expanded=True):
                            df_display = df_results.copy()
                            for col in ["Entity Name", "Series Name"]:
                                if col not in df_display.columns:
                                    df_display[col] = "Data not available"
                                else:
                                    df_display[col] = df_display[col].fillna("Data not available")
                            st.container().markdown("""
                                <div style='overflow-x:auto;'>
                            """, unsafe_allow_html=True)
                            st.dataframe(df_display.style.apply(lambda x: ["color: green" if v is True else "" for v in x], subset=['Has Performance Data']))

st.markdown("""</div>""", unsafe_allow_html=True)

if 'df_results' in st.session_state and isinstance(st.session_state.df_results, pd.DataFrame) and not st.session_state.df_results.empty:
    df_results = st.session_state.df_results
    csv_data = df_results.to_csv(index=False).encode("utf-8")
    st.download_button("⬇️ Download Results as CSV", csv_data, "performance_disclosure_results.csv", "text/csv")
else:
    st.error("❌ No valid data available to download.")

with st.expander("📊 Performance Disclosure by Entity", expanded=True):
    if 'df_results' in st.session_state and isinstance(st.session_state.df_results, pd.DataFrame) and not st.session_state.df_results.empty:
        df_results = st.session_state.df_results
        if not df_results.empty and "Entity Name" in df_results.columns:
            perf_by_entity = df_results[df_results["Has Performance Data"]].groupby("Entity Name")["classid"].count().sort_values(ascending=False)
            fig, ax = plt.subplots(figsize=(10, 6))
            sns.barplot(x=perf_by_entity.values, y=perf_by_entity.index, ax=ax, palette="Blues_d")
            ax.set_xlabel("# of Classes Disclosing Performance")
            ax.set_ylabel("Entity Name")
            ax.set_title("Performance Disclosure by Entity")
            st.pyplot(fig)
        else:
            st.info("ℹ️ 'Entity Name' column not found in the results or no valid data available.")
    else:
        st.error("❌ No valid data available to download.")

if 'df_results' in st.session_state and isinstance(st.session_state.df_results, pd.DataFrame) and not st.session_state.df_results.empty:
    df_results = st.session_state.df_results
    with st.expander("🌟 Funds with Lowest Expenses and Highest Performance", expanded=True):
        col_exp, col_perf, col_topn = st.columns(3)
        with col_exp:
            max_expense_filter = st.number_input("💰 Max Expense Amount ($)", min_value=0.0, value=100.0, key="expense_filter")
        with col_perf:
            min_perf_filter = st.number_input("📈 Min Performance (%)", min_value=0.0, value=0.0, key="performance_filter")
        with col_topn:
            top_n = st.slider("🔢 Number of Top Results", min_value=5, max_value=50, value=10, key="topn_slider")

        top_performers = df_results.copy()
        top_performers["expense_amt"] = pd.to_numeric(top_performers["expense_amt"], errors="coerce")
        top_performers["performance_pct"] = pd.to_numeric(top_performers["performance_pct"], errors="coerce")
        top_performers = top_performers.dropna(subset=["expense_amt", "performance_pct"])

        top_performers_filtered = top_performers[
            (top_performers["expense_amt"] <= max_expense_filter) &
            (top_performers["performance_pct"] >= min_perf_filter)
        ]

        lowest_expense = top_performers_filtered.sort_values("expense_amt").head(top_n)
        highest_perf = top_performers_filtered.sort_values("performance_pct", ascending=False).head(top_n)

        st.write(f"#### 💸 Lowest Expense Funds (Filtered to ≤ ${max_expense_filter})")
        df_le_display = lowest_expense.copy()
        for col in ["Entity Name", "Series Name"]:
            if col not in df_le_display.columns:
                df_le_display[col] = "Data not available"
            else:
                df_le_display[col] = df_le_display[col].fillna("Data not available")
        st.container().markdown("""
<div style='overflow-x:auto;'>
""", unsafe_allow_html=True)
        st.dataframe(df_le_display[["Entity Name", "Series Name", "classid", "expense_amt", "performance_pct"]])
        st.markdown("""</div>""", unsafe_allow_html=True)

        st.write(f"#### 🚀 Highest Performance Funds (Filtered to ≥ {min_perf_filter}%)")
        df_hp_display = highest_perf.copy()
        for col in ["Entity Name", "Series Name"]:
            if col not in df_hp_display.columns:
                df_hp_display[col] = "Data not available"
            else:
                df_hp_display[col] = df_hp_display[col].fillna("Data not available")
        st.container().markdown("""
<div style='overflow-x:auto;'>
""", unsafe_allow_html=True)
        st.dataframe(df_hp_display[["Entity Name", "Series Name", "classid", "expense_amt", "performance_pct"]])
        st.markdown("""</div>""", unsafe_allow_html=True)

st.markdown("""
    <br><hr>
""", unsafe_allow_html=True)
