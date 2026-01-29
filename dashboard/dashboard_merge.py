import streamlit as st
import pandas as pd
import pyodbc
from os import environ as ENV
from dotenv import load_dotenv

from components import sidebar, soil_moisture_over_time, temperature_over_time
from queries_s3 import get_available_dates, get_data_for_date, get_last_n_days

st.set_page_config(page_title="LMNH Plant Health Dashboard", layout="wide")

load_dotenv()

def get_db_connection():
    conn_str = (
        f"DRIVER={{{ENV['DB_DRIVER']}}};"
        f"SERVER={ENV['DB_HOST']};"
        f"PORT={ENV['DB_PORT']};"
        f"DATABASE={ENV['DB_NAME']};"
        f"UID={ENV['DB_USERNAME']};"
        f"PWD={ENV['DB_PASSWORD']};"
        f"Encrypt=no;"
    )
    return pyodbc.connect(conn_str)


@st.cache_data(ttl=300)  
def load_rds_data() -> pd.DataFrame:
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM alpha.recordings;")
        rows = cur.fetchall()
        columns = [column[0] for column in cur.description]
        return pd.DataFrame.from_records(rows, columns=columns)
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()


@st.cache_data(ttl=300)
def get_filtered_data(data: pd.DataFrame, plant_selection):
    if plant_selection and plant_selection != "All":
        return data[data["plant_id"] == plant_selection].copy()
    return data


def display_short_term(short_term_data: pd.DataFrame):
    st.header("Short term (Live)")
    st.caption("Live Data (updates every minute).")

    plant_selection = sidebar(short_term_data)
    filtered_data = get_filtered_data(short_term_data, plant_selection)

    col1, col2 = st.columns(2)
    with col1:
        soil_moisture_over_time(filtered_data)
    with col2:
        temperature_over_time(filtered_data)


@st.cache_data(ttl=600)
def cached_dates():
    return get_available_dates()


@st.cache_data(ttl=600)
def cached_day(date_str: str):
    return get_data_for_date(date_str)


@st.cache_data(ttl=600)
def cached_last_n(n: int = 14):
    return get_last_n_days(n)


def display_long_term():
    st.header("Long term (Daily aggregates)")
    st.caption("Daily summaries (partitioned by date).")

    dates_df = cached_dates()
    if dates_df.empty or "date" not in dates_df.columns:
        st.info("No available dates found in the long term dataset.")
        return

    date_options = dates_df["date"].tolist()
    selected = st.selectbox("Select a day", date_options, key="long_term_day")

    n_days = st.slider("Trend window (days)", min_value=7,
                       max_value=60, value=14, step=1)

    with st.spinner("Loading long term data..."):
        df_day = cached_day(selected)
        df_n = cached_last_n(n_days)

    if df_day.empty:
        st.info("No rows for this day.")
        return

    row = df_day.iloc[0]

    soil_col = "Soil Mean" if "Soil Mean" in df_day.columns else "soil_mean"
    temp_col = "Temp Mean" if "Temp Mean" in df_day.columns else "temp_mean"

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Readings", int(
        row["readings"]) if "readings" in df_day.columns and row["readings"] is not None else "—")
    c2.metric("Plants", int(
        row["plants"]) if "plants" in df_day.columns and row["plants"] is not None else "—")
    c3.metric("Avg Soil Moisture", round(float(
        row[soil_col]), 2) if soil_col in df_day.columns and row[soil_col] is not None else "—")
    c4.metric("Avg Temperature", round(float(
        row[temp_col]), 2) if temp_col in df_day.columns and row[temp_col] is not None else "—")

    st.divider()

    if df_n.empty:
        st.info("No trend data available.")
        return

    if "dt" not in df_n.columns:
        st.warning("Trend data is missing 'dt' column.")
        return

    df_n = df_n.sort_values("dt")

    st.subheader(f"Last {n_days} days trend")

    trend_cols = [c for c in [soil_col, temp_col] if c in df_n.columns]
    if trend_cols:
        chart_df = df_n.set_index("dt")[trend_cols]
        st.line_chart(chart_df)

    rp_cols = [c for c in ["readings", "plants"] if c in df_n.columns]
    if rp_cols:
        st.subheader(f"Last {n_days} days volume")
        st.line_chart(df_n.set_index("dt")[rp_cols])


st.title("LMNH Botanical Wing Plant Health")


with st.spinner("Loading live data..."):
    short_term_data = load_rds_data()

display_short_term(short_term_data)

st.divider()

display_long_term()
