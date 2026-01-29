"""Script to load data from AWS RDS and S3 and display on a Streamlit dashboard"""

import pyodbc
import awswrangler as wr
import streamlit as st
from os import environ as ENV
from dotenv import load_dotenv
from components import sidebar, soil_moisture_over_time, temperature_over_time
import pandas as pd

S3_OUTPUT = "s3://c21-alpha-s3/recordings/"


def get_db_connection(event=None, context=None):
    """Setup connection to the RDS"""
    conn_str = (f"DRIVER={{{ENV['DB_DRIVER']}}};SERVER={ENV['DB_HOST']};"
                f"PORT={ENV['DB_PORT']};DATABASE={ENV['DB_NAME']};"
                f"UID={ENV['DB_USERNAME']};PWD={ENV['DB_PASSWORD']};Encrypt=no;")

    conn = pyodbc.connect(conn_str)
    return conn


@st.cache_data(ttl=86400)
def load_rds_data():
    """Connect to RDS and return all data from the recordings table"""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM alpha.recordings;")
        rows = cur.fetchall()
        columns = [column[0] for column in cur.description]
        return pd.DataFrame.from_records(rows, columns=columns)
    finally:
        cur.close()
        conn.close()


@st.cache_data
def get_filtered_data(data, plant_selection):
    """Cache the filtered data"""
    if plant_selection and plant_selection != 'All':
        return data[data['plant_id'] == plant_selection].copy()
    return data


def display_dashboard(short_term_data):
    """Display all components on the dashboard"""
    st.title('LMNH Botanical Wing Plant Health', text_alignment='center')

    plant_selection = sidebar(short_term_data)

    filtered_data = get_filtered_data(short_term_data, plant_selection)

    col1, col2 = st.columns(2)

    with col1:
        soil_moisture_over_time(filtered_data)

    with col2:
        temperature_over_time(filtered_data)



if __name__ == "__main__":
    load_dotenv()
    short_term_data = load_rds_data()
    st.set_page_config(layout="wide")
    display_dashboard(short_term_data)
