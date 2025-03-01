# %%
"""
The SQL we just ran changed the data in the database, and we might not have been happy about that.
Maybe we decide we made a mistake in our update logic, and now we want to see what rows didn't have a
_loaded_at column.

In Iceberg, time-travel is built-in, as all we have to do is keep track of the metadata for the files
"""
import duckdb

from aws_iceberg_demo.connections import get_trino_engine
import sqlalchemy as sa
import polars as pl

pl.config.Config.set_tbl_width_chars(-1)

engine = get_trino_engine()

with engine.connect() as conn:
    result = conn.execute(sa.text('SELECT * FROM store."events$snapshots"'))
    print(pl.from_arrow(result.cursor.as_arrow()))

#%%
"""
Iceberg versions the schemas, so if we timetravel back to before the schema update, we get an error
"""
with engine.connect() as conn:
    sql = """
    SELECT count(event_time) as num_events, _loaded_at 
    FROM store.events FOR VERSION AS OF 8433192130702135638 
    group by _loaded_at
    """
    result = conn.execute(sa.text(sql))
    print(pl.from_arrow(result.cursor.as_arrow()))

#%%
"""We can also travel in time based on timestamps"""

with engine.connect() as conn:
    sql = """
    SELECT count(event_time) as num_events, _loaded_at
    FROM store.events FOR TIMESTAMP AS OF TIMESTAMP '2025-02-26 20:33:50 UTC'
    GROUP BY _loaded_at
    """
    result = conn.execute(sa.text(sql))
    print(pl.from_arrow(result.cursor.as_arrow()))

#%%
"""We can tag a given snapshot to give it a name, such as Q4 report"""

from aws_iceberg_demo.catalog import get_catalog

t = get_catalog().load_table("store.events")
t.manage_snapshots().create_tag(5463529947108187791, "q4_report").commit()

#%%
"""Now that we have a named tag, we can reference it in our FROM statement to be able to see data as it was back then
Tags can be configured with an expiration date e.g keep it for 7 days or in this case, keep it forever
"""

with engine.connect() as conn:
    sql = f"""
    SELECT count(event_time) as num_events, _loaded_at
    FROM store.events FOR VERSION AS OF 'q4_report'
    GROUP BY _loaded_at
    """
    result = conn.execute(sa.text(sql))
    print(pl.from_arrow(result.cursor.as_arrow()))

#%%
"""Let's add in some more data to demonstrate"""
import pathlib
import pyarrow.parquet as pq

data_files = [
              "2020-Jan.parquet",
              "2020-Feb.parquet",
              "2020-Mar.parquet",
              "2020-Apr.parquet"]
for data_file in data_files:
    print(data_file)
    df = pq.read_table(pathlib.Path("data/parquet/sampled") / data_file, schema=t.schema().as_arrow())
    t.append(df)

#%%
with engine.connect() as conn:
    r = conn.execute(sa.text("SELECT count(event_time) as num_events, date_trunc('month', event_time) FROM store.events group by 2"))
    print(pl.from_arrow(r.cursor.as_arrow()))

#%%
with engine.connect() as conn:
    r = conn.execute(
        sa.text("SELECT count(event_time) as num_events, date_trunc('month', event_time) "
                "FROM store.events FOR VERSION AS OF 'q4_report' "
                "GROUP BY 2"))
    print(pl.from_arrow(r.cursor.as_arrow()))