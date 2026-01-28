"""Script to load data from AWS RDS and S3 and display on a Streamlit dashboard"""

import pyodbc
import awswrangler as wr
import streamlit as st
from os import environ as ENV
from dotenv import load_dotenv

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
    cur = conn.cursor()
    query = "SELECT * FROM alpha.recordings;"
    cur.execute(query)
    return cur.fetchall()


def display_dashboard():
    """Display all components on the dashboard"""
    st.title('LMNH Botanical Wing Plant Health', text_alignment='center')


if __name__ == "__main__":
    load_dotenv()
    short_term_data = load_rds_data()
    display_dashboard()
