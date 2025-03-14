import datetime as dt

import polars as pl
import streamlit as st
from millify import millify
from pyiceberg.catalog import Catalog
from pyiceberg.table import Table

from aws_iceberg_demo.catalog import get_catalog

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
def catalog() -> Catalog:
    return get_catalog()


@st.cache_data
def get_brands(_table: Table) -> list[str]:
    brands = (pl.scan_iceberg(_table)
              .select(pl.col("brand"))
              .unique()
              .collect()
              .to_series(0)
              .to_list()

              )
    brands.insert(0, "*")
    return brands


@st.cache_data
def get_event_time_interval(_table: Table) -> tuple[dt.datetime, dt.datetime]:
    return (pl.scan_iceberg(_table)
            .select(
        pl.col("event_time").min().alias("min_event_time"),
        pl.col("event_time").max().alias("max_event_time")
    )
            .collect()
            .row(0))


@st.cache_data
def get_total_rows(_table: Table) -> int:
    return pl.scan_iceberg(_table).select(pl.count('event_time')).collect().item()


@st.cache_data
def get_hourly_distribution(_table: Table, start_date: dt.datetime.date, end_date: dt.datetime.date,
                            brand: str) -> pl.DataFrame:
    filters = [pl.col("event_time").is_between(start_date, end_date)]
    if brand != "*":
        filters.append(pl.col("brand").eq(brand))
    return pl.scan_iceberg(_table).filter(*filters).group_by(pl.col("event_time").dt.hour(),
                                                             pl.col("event_type").eq("purchase").alias(
                                                                 "purchase_event")).agg(
        total_events=pl.len()).sort(by="event_time").collect()


@st.cache_data
def get_top_10_brands(_table: Table, start_date: dt.datetime.date, end_date: dt.datetime.date):
    return pl.scan_iceberg(_table).filter(pl.col("event_time").is_between(start_date, end_date)).group_by("brand").agg(
        num_brands=pl.len()).top_k(10, by="num_brands").sort(by="num_brands", descending=True).collect()


table = catalog().load_table("store.events_v2")

starting_brands = get_brands(table)
start_end_date = get_event_time_interval(table)

total_rows = get_total_rows(table)

first_row_cols = st.columns(3)

with first_row_cols[0]:
    st.metric("Total Rows", millify(total_rows))

with first_row_cols[1]:
    st.metric("Total Brands", len(starting_brands) - 1)

col1, col2 = st.columns(2)

with col1:
    selected_brand = st.selectbox("Brands", starting_brands)
with col2:
    selected_daterange = st.date_input("Dates",
                                       (start_end_date[0], start_end_date[1]),
                                       min_value=start_end_date[0],
                                       max_value=start_end_date[1])

if len(selected_daterange) != 2:
    st.warning("Please select a date range")
    st.stop()

selected_startdate = dt.datetime.combine(selected_daterange[0], dt.time(), tzinfo=dt.UTC)
selected_enddate = dt.datetime.combine(selected_daterange[1], dt.time(), tzinfo=dt.UTC)

hourly_purchase_distribution = get_hourly_distribution(table, selected_startdate, selected_enddate,
                                                       selected_brand)
st.markdown(f"## Hourly Purchase Distribution {f'for {selected_brand}' if selected_brand != '*' else ''}")
st.bar_chart(hourly_purchase_distribution, x="event_time", y="total_events", x_label="Event Hour",
             y_label="Total Events", color="purchase_event")

top_10_brands = get_top_10_brands(table, selected_startdate, selected_enddate)
st.markdown("Top 10 Brands")
st.bar_chart(top_10_brands, x="brand", y="num_brands", x_label="Brand Name", y_label="Count of events")
