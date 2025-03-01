"""
# Loading more data

When we load more data, Iceberg will now update the snapshot to contain more
"""
import pyarrow.parquet as pq

from aws_iceberg_demo.catalog import get_catalog

# %%
# Fetch the existing table
catalog = get_catalog()
t = catalog.load_table("store.events")

# %%
# Load the data from a Parquet file. Note that we're using the Iceberg schema to load the data,
# to make sure the data is loaded with the correct schema
df = pq.read_table("data/parquet/sampled/2019-Nov.parquet",
                   schema=t.schema().as_arrow())
# %%
# Appending new data will create a new snapshot.
# Inspecting the manifest list, we should see multiple manifests
t.append(df)
