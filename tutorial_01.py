"""
# Loading data to Iceberg

Pyiceberg is the official python library for managing Iceberg tables. It's using Apache Arrow
as the data interchange format, so we read the data from a parquet file
"""
import pyarrow.parquet as pq

from aws_iceberg_demo.catalog import get_catalog
from aws_iceberg_demo.tables import event_table_schema


# %%
# Create a namespace
catalog = get_catalog()
catalog.create_namespace_if_not_exists("store")

# %%
# Create a table, using the predefined Iceberg schema. There is now a snapshot in the bucket
# defining this table
t = catalog.create_table_if_not_exists("store.events",
                                       schema=event_table_schema,
                                       location="s3://data-engineering-events/warehouse/events")

# %%
# Load the data from a Parquet file. Note that we're using the Iceberg schema translated into arrow
# to load the data. The interoperability between Iceberg types and Arrow types make sure the data is
# loaded with the correct schema and doesn't have to be reconverted
df = pq.read_table("./data/parquet/sampled/2019-Oct.parquet",
                   schema=t.schema().as_arrow())
t.append(df)
