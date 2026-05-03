import streamlit as st
import pandas as pd
from databricks import sql

# ================= CONFIG =================
import streamlit as st

DATABRICKS_SERVER_HOSTNAME = st.secrets["DATABRICKS_SERVER_HOSTNAME"]
DATABRICKS_HTTP_PATH = st.secrets["DATABRICKS_HTTP_PATH"]
DATABRICKS_TOKEN = st.secrets["DATABRICKS_TOKEN"]

GOLD_SUMMARY_TABLE = "cleanstream.gold.data_quality_summary"
GOLD_ERROR_TABLE = "cleanstream.gold.error_breakdown"
SILVER_VALID_TABLE = "cleanstream.silver.valid_data"
QUARANTINE_TABLE = "cleanstream.silver.quarantine_table"

st.set_page_config(
    page_title="CleanStream Dashboard",
    page_icon="🧹",
    layout="wide"
)

# ================= CSS =================
st.markdown("""
<style>
.main-title {
    font-size: 38px;
    font-weight: 800;
    margin-bottom: 5px;
}
.sub-title {
    font-size: 17px;
    color: #666;
    margin-bottom: 25px;
}
.section-title {
    font-size: 24px;
    font-weight: 700;
    margin-top: 25px;
    margin-bottom: 10px;
}
.metric-box {
    background-color: #f8f9fa;
    padding: 22px;
    border-radius: 14px;
    border: 1px solid #e6e6e6;
}
.badge-good {
    color: green;
    font-weight: 700;
}
.badge-bad {
    color: red;
    font-weight: 700;
}
</style>
""", unsafe_allow_html=True)

st.markdown('<div class="main-title">CleanStream Data Quality Dashboard</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="sub-title">Kafka → Databricks → Delta Lake data quality monitoring system</div>',
    unsafe_allow_html=True
)

# ================= QUERY FUNCTION =================
@st.cache_data(ttl=10)
def run_query(query):
    with sql.connect(
        server_hostname=DATABRICKS_SERVER_HOSTNAME,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN
    ) as connection:
        return pd.read_sql(query, connection)

def safe_query(query):
    try:
        return run_query(query)
    except Exception as e:
        st.warning(f"Query failed: {e}")
        return pd.DataFrame()

# ================= LOAD GOLD DATA =================
summary_df = safe_query(f"SELECT * FROM {GOLD_SUMMARY_TABLE}")
error_df = safe_query(f"SELECT * FROM {GOLD_ERROR_TABLE}")

if summary_df.empty:
    st.error("Gold summary table is empty. Run your Gold SQL notebook first.")
    st.stop()

summary = summary_df.iloc[0]

# ================= KPI SECTION =================
total_events = int(summary["total_events"])
valid_events = int(summary["valid_events"])
invalid_events = int(summary["invalid_events"])
valid_rate = float(summary["valid_rate"])
invalid_rate = float(summary["invalid_rate"])
schema_drift_rate = float(summary["schema_drift_rate"])

st.markdown('<div class="section-title">Pipeline Health</div>', unsafe_allow_html=True)

col1, col2, col3, col4, col5 = st.columns(5)

col1.metric("Total Events", f"{total_events:,}")
col2.metric("Valid Events", f"{valid_events:,}", f"{valid_rate}%")
col3.metric("Invalid Events", f"{invalid_events:,}", f"{invalid_rate}%")
col4.metric("Schema Drift", f"{schema_drift_rate}%")
col5.metric("Kafka Partitions", int(summary["kafka_partition_count"]))

st.divider()

# ================= QUALITY STATUS =================
if valid_rate >= 80:
    status_text = "Healthy"
    status_color = "badge-good"
elif valid_rate >= 60:
    status_text = "Warning"
    status_color = "badge-bad"
else:
    status_text = "Critical"
    status_color = "badge-bad"

st.markdown(
    f"""
    <div class="metric-box">
        <h3>Pipeline Status: <span class="{status_color}">{status_text}</span></h3>
        <p>
        This dashboard is monitoring how many Kafka events are clean, how many are broken,
        and what kind of data quality issues are entering the Delta Lake pipeline.
        </p>
    </div>
    """,
    unsafe_allow_html=True
)

st.divider()

# ================= ISSUE SUMMARY =================
st.markdown('<div class="section-title">Data Quality Issue Summary</div>', unsafe_allow_html=True)

issue_data = pd.DataFrame({
    "Issue Type": [
        "Missing Event ID",
        "Missing Customer ID",
        "Missing Amount",
        "Missing Payment Status",
        "Invalid Amount",
        "Invalid Payment Status",
        "Negative Amount",
        "Future Event Time",
        "Schema Drift"
    ],
    "Count": [
        int(summary["missing_event_id_count"]),
        int(summary["missing_customer_id_count"]),
        int(summary["missing_amount_count"]),
        int(summary["missing_payment_status_count"]),
        int(summary["invalid_amount_count"]),
        int(summary["invalid_payment_status_count"]),
        int(summary["negative_amount_count"]),
        int(summary["future_event_time_count"]),
        int(summary["schema_drift_count"])
    ]
})

left, right = st.columns([1.3, 1])

with left:
    st.bar_chart(issue_data.set_index("Issue Type"))

with right:
    st.dataframe(issue_data, use_container_width=True, hide_index=True)

st.divider()

# ================= ERROR BREAKDOWN =================
st.markdown('<div class="section-title">Root Cause Error Breakdown</div>', unsafe_allow_html=True)

if not error_df.empty:
    chart_df = error_df[["error_type", "error_count"]].copy()
    chart_df = chart_df.set_index("error_type")

    col_a, col_b = st.columns([1.3, 1])

    with col_a:
        st.bar_chart(chart_df)

    with col_b:
        st.dataframe(
            error_df[
                [
                    "error_type",
                    "error_count",
                    "error_percentage",
                    "affected_partitions",
                    "first_seen_offset",
                    "last_seen_offset"
                ]
            ],
            use_container_width=True,
            hide_index=True
        )
else:
    st.success("No error data found. Your quarantine/error breakdown table is empty.")

st.divider()

# ================= SAMPLE BAD JSON =================
st.markdown('<div class="section-title">Sample Broken Records</div>', unsafe_allow_html=True)

if not error_df.empty:
    sample_df = error_df[["error_type", "sample_raw_json"]].copy()
    st.dataframe(sample_df, use_container_width=True, hide_index=True)
else:
    st.info("No broken samples available.")

st.divider()

# ================= LATEST VALID DATA =================
st.markdown('<div class="section-title">Latest Valid Events</div>', unsafe_allow_html=True)

valid_latest = safe_query(f"""
SELECT 
    event_id, 
    customer_id, 
    amount, 
    payment_status, 
    event_time, 
    silver_processed_time
FROM {SILVER_VALID_TABLE}
ORDER BY silver_processed_time DESC
LIMIT 20
""")

st.dataframe(valid_latest, use_container_width=True, hide_index=True)

# ================= LATEST QUARANTINE DATA =================
st.markdown('<div class="section-title">Latest Quarantine Events</div>', unsafe_allow_html=True)

invalid_latest = safe_query(f"""
SELECT 
    parsed_event_id, 
    parsed_customer_id, 
    parsed_amount, 
    parsed_payment_status,
    parsed_event_time, 
    final_error_summary, 
    silver_processed_time
FROM {QUARANTINE_TABLE}
ORDER BY silver_processed_time DESC
LIMIT 20
""")

st.dataframe(invalid_latest, use_container_width=True, hide_index=True)

# ================= FOOTER =================
st.caption("Gold tables used: cleanstream.gold.data_quality_summary and cleanstream.gold.error_breakdown")
st.caption("Refresh: reload page or wait around 10 seconds because cache TTL is set to 10 seconds.")
