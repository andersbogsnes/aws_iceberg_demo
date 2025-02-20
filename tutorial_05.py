"""
As we add more data, query performance becomes an issue. If this was in a Postgres database, we'd start adding
indexes. In the data lakehouse we have partitions
"""

#%%
from aws_iceberg_demo.connections import duckdb_conn
from aws_iceberg_demo.catalog import nessie_catalog
import time
t = nessie_catalog.load_table("store.events")

#%%

start_time = time.perf_counter()

