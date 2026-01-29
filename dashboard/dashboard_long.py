import streamlit as st
from queries_s3 import get_available_dates, get_data_for_date, get_last_n_days

st.set_page_config(page_title="Daily Dashboard", layout="wide")
st.title("Daily Dashboard")

# ---- CACHING ----


@st.cache_data(ttl=600)
def cached_dates():
    return get_available_dates()


@st.cache_data(ttl=600)
def cached_day(date_str: str):
    return get_data_for_date(date_str)


@st.cache_data(ttl=600)
def cached_last_14():
    return get_last_n_days(14)


# ---- UI ----
dates_df = cached_dates()
date_options = dates_df["date"].tolist()
selected = st.selectbox("Select a day", date_options)

with st.spinner("Loading..."):
    df_day = cached_day(selected)
    df_14 = cached_last_14()

if df_day.empty:
    st.info("No rows for this day.")
    st.stop()

# ---- KPI CARDS (single day) ----
row = df_day.iloc[0]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Readings", int(
    row["readings"]) if "readings" in df_day.columns and row["readings"] is not None else "—")
c2.metric("Plants", int(
    row["plants"]) if "plants" in df_day.columns and row["plants"] is not None else "—")
c3.metric("Avg Soil Moisture", round(float(
    row["soil_mean"]), 2) if "soil_mean" in df_day.columns and row["soil_mean"] is not None else "—")
c4.metric("Avg Temperature", round(float(
    row["temp_mean"]), 2) if "temp_mean" in df_day.columns and row["temp_mean"] is not None else "—")

st.divider()

# ---- LAST 14 DAYS TREND ----
if df_14.empty:
    st.info("No trend data available.")
    st.stop()

# Ensure proper ordering for charts (oldest -> newest)
df_14 = df_14.sort_values("dt")

st.subheader("Last 14 days trend")

# Line chart: soil + temperature
trend_cols = [c for c in ["soil_mean", "temp_mean"] if c in df_14.columns]
if trend_cols:
    chart_df = df_14.set_index("dt")[trend_cols]
    st.line_chart(chart_df)

# Optional: readings/plants as separate chart (often different scale)
rp_cols = [c for c in ["readings", "plants"] if c in df_14.columns]
if rp_cols:
    st.subheader("Last 14 days volume")
    st.line_chart(df_14.set_index("dt")[rp_cols])
