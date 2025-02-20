import duckdb
import streamlit as st
from duckdb.duckdb import DuckDBPyConnection
import pandas as pd

from aws_iceberg_demo.catalog import nessie_catalog

st.markdown("""# Big Ecommerce Store

A dashboard for understanding Big Ecommerce customers 
""")

# noinspection LongLine
st.image("images/shopping.jpg",
         caption="Photo by "
                 "[rupixen](https://unsplash.com/@rupixen) on "
                 "[Unsplash](https://unsplash.com/photos/person-using-laptop-computer-holding-card-Q59HmzK38eQ)",
         use_container_width=True
)

@st.cache_resource
def duckdb_conn() -> DuckDBPyConnection:
    conn = duckdb.connect("local.ddb")
    conn.install_extension("iceberg")
    conn.load_extension("iceberg")
    conn.sql("""
    CREATE OR REPLACE secret minio (
             TYPE S3, 
             KEY_ID 'minio', 
             SECRET 'minio1234', 
             ENDPOINT 'localhost:9000', 
             URL_STYLE path,
             USE_SSL false
             )""")
    return conn

conn = duckdb_conn()
table = nessie_catalog.load_table("store.events")


@st.cache_data
def run_query(query: str) -> pd.DataFrame:
    return conn.sql(query).df()

starting_brands = run_query("SELECT '*'"
                            "UNION ALL " 
                            f"SELECT distinct brand from iceberg_scan('{table.metadata_location}') "
                            )
start_end_date = run_query(f"SELECT min(event_time) as start_date, "
                           f"max(event_time) as end_date "
                           f"from iceberg_scan('{table.metadata_location}')")



col1, col2 = st.columns(2)

with col1:
    selected_brand = st.selectbox("Brands", starting_brands)
with col2:
    selected_daterange = st.date_input("Dates",
                                   (start_end_date.iat[0, 0], start_end_date.iat[0, 1]),
                                   min_value=start_end_date.iat[0, 0],
                                   max_value=start_end_date.iat[0, 1])


