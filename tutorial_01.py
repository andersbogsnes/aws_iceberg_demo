"""
# Loading data to Iceberg

Pyiceberg is the official python library for managing Iceberg tables. It's using Apache Arrow
as the data interchange format, so we read the data from a parquet file
"""
import pyarrow.parquet as pq

from aws_iceberg_demo.catalog import nessie_catalog
from aws_iceberg_demo.tables import event_table_schema

# Create a namespace
nessie_catalog.create_namespace_if_not_exists("store")

# Create a table, using the predefined Iceberg schema
t = nessie_catalog.create_table_if_not_exists("store.events",
                                schema=event_table_schema,
                                              location="s3://warehouse/nessie/events")


# Load the data from a Parquet file. Note that we're using the Iceberg schema to load the data,
# to make sure the data is loaded with the correct schema
df = pq.read_table("/home/anders/projects/tutorials/aws_iceberg_demo/data/parquet/2019-Nov.parquet",
                   schema=t.schema().as_arrow())
t.append(df)