"""
We've received a new request to add a `_loaded_at` column to help with late arriving data.

In a normal data lake, we would be sad - we would have to rewrite the physical files.
Iceberg gives us Schema Evolution through its metadata layer
"""
# %%
from aws_iceberg_demo.catalog import nessie_catalog
import pyarrow.parquet as pq
import pyarrow as pa
import numpy as np

t = nessie_catalog.load_table("store.events")
df = pq.read_table("data/parquet/2019-Dec.parquet", schema=t.schema().as_arrow())
_loaded_at = pa.array(np.array(['2019-12-01T00:00:00'] * len(df), dtype=np.datetime64), type=pa.timestamp('us', tz='UTC'))
df = df.append_column(pa.field("_loaded_at", pa.timestamp('us', tz='UTC')), _loaded_at)

#%%
"""
The data has a breaking schema change - we've added a new column that wasn't there before.
This should not work since it will break our schema!
"""
t.append(df)

# %%
"""
We can update the schema for the table in a transaction. Let's check the metadata to see what has changed
"""
from pyiceberg.types import TimestamptzType

with t.update_schema() as update:
    update.add_column("_loaded_at", TimestamptzType(), doc="Timestamp when the data was loaded")


# %%
"""
We can now add the `_loaded_at` column to the next file we want to load
"""
t.append(df)

#%%
"""
What happens when load data stored in the old schema?
"""
from aws_iceberg_demo.connections import duckdb_conn
# Make sure the table object is updated
t.refresh()

duckdb_conn.sql(f"""
SELECT count('event_time'), _loaded_at 
FROM iceberg_scan('{t.metadata_location}')
GROUP BY ALL
""").show()

#%%
"""
Duckdb doesn't support writing Iceberg tables (yet), so let's use Trino to update our data back in time. 
"""
from aws_iceberg_demo.connections import trino_engine
import sqlalchemy as sa
import datetime as dt
with trino_engine.connect() as conn:
    conn.execute(sa.text("UPDATE store.events SET _loaded_at = :_loaded_at where _loaded_at IS NULL "), dict(_loaded_at=dt.datetime(year=2019, month=12, day=1, tzinfo=dt.UTC)))
    conn.execute(sa.text("ALTER TABLE store.events execute optimize"))
    conn.commit()