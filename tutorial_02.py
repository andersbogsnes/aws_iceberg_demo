"""
# Loading more data

When we load more data, Iceberg will now update the snapshot to contain more
"""

from aws_iceberg_demo.catalog import nessie_catalog
import pyarrow.parquet as pq

# %%
# Fetch the existing table
t = nessie_catalog.load_table("store.events")

# %%
# Load the data from a Parquet file. Note that we're using the Iceberg schema to load the data,
# to make sure the data is loaded with the correct schema
df = pq.read_table("/home/anders/projects/tutorials/aws_iceberg_demo/data/parquet/2019-Nov.parquet",
                   schema=t.schema().as_arrow())
# %%
# Appending new data will create a new snapshot.
# Inspecting the manifest list, we should see multiple manifests
t.append(df)