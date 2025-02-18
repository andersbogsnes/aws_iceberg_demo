"""
We've received a new request to add a `_loaded_at` column to help with late arriving data.

In a normal data lake, we would be sad - we would have to rewrite the physical files.
Iceberg gives us Schema Evolution through its metadata layer
"""
# %%
from aws_iceberg_demo.catalog import nessie_catalog
import pyarrow.parquet as pq
import pyarrow as pa
import datetime as dt

t = nessie_catalog.load_table("store.events")
df = pq.read_table("data/parquet/2019-Oct.parquet", schema=t.schema().as_arrow())
_loaded_at = pa.array([dt.datetime.now(tz=dt.UTC) for _ in range(len(df))])
df = df.append_column(pa.field("_loaded_at", pa.timestamp('us', tz='UTC')), _loaded_at)
t.append(df)

# %%
"""
We can update the schema for the table in a transaction
"""
from pyiceberg.types import TimestamptzType

with t.update_schema() as update:
    update.add_column("_loaded_at", TimestamptzType(), doc="Timestamp when the data was loaded")

# %%
"""
We can now add the `_loaded_at` column to the next file we want to load
"""
t.append(df)

