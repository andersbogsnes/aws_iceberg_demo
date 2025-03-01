"""
# Querying the data

Now that we have some data, let's try querying it. We have a number of options, which is one of
the strengths of using a format like Iceberg. Open storage standards means any query engine can
choose to support it
"""
import polars as pl

from aws_iceberg_demo.catalog import get_catalog
import time

catalog = get_catalog()
t = catalog.load_table("store.events")

# %%
"""
With pyiceberg, we can do some filtering before fetching the data. Pyiceberg uses Apache Arrow
# behind the scenes, so converting to your favourite DataFrame library is straight forward
"""
from pyiceberg.expressions import GreaterThanOrEqual, LessThanOrEqual, And

start_time = time.perf_counter()
# Apply predicates to only get the data we're interested in
df = t.scan(row_filter=And(GreaterThanOrEqual("event_time", "2019-11-01T00:00:00+00:00"),
                           LessThanOrEqual("event_time", "2019-11-07T00:00:00+00:00")),
            selected_fields=("event_time", "category_code", "price")
            ).to_arrow()
print(f"Iceberg Scan {time.perf_counter() - start_time:.5f} seconds")

print(pl.from_arrow(df).group_by(pl.col("category_code")).agg(avg_price=pl.col("price").mean()))
print(f"With polars aggregation: {time.perf_counter() - start_time:.5f} seconds")
# %%
"""
We can use something like duckdb to run SQL directly on the iceberg table.

Duckdb doesn't yet talk to the catalogue directly, it reads the metadata.json file directly,
which we can get from the catalogue
"""
from aws_iceberg_demo.connections import get_duckdb_conn
conn = get_duckdb_conn()

metadata_file = t.metadata_location

sql = f"""
SELECT 
    category_code, avg(price) as avg_price 
from iceberg_scan('{metadata_file}') 
where event_time between '2019-11-01' and '2019-11-07'
GROUP BY all
ORDER BY avg_price DESC
"""

start_time = time.perf_counter()
dd_df = conn.sql(sql)
dd_df.show()
print(f"Duckdb: {time.perf_counter() - start_time:.5f} seconds")
# %%
"""
We can use polars directly on the iceberg table as well
"""
import polars as pl

start_time = time.perf_counter()
df = pl.scan_iceberg(t).group_by(pl.col("category_code")).agg(avg_price=pl.col("price").mean()).sort("avg_price",
                                                                                      descending=True).collect()
print(df)
print(f"Polars: {time.perf_counter() - start_time:.5f} seconds")
# %%
"""
We can easily switch to use a distributed compute system like Trino, which has very good support for Iceberg. Note that
AWS Athena is a managed version of Trino, giving us a serverless option as well

We can even run on top of SQLAlchemy, since Trino has a SQLAlchemy driver!
"""
from aws_iceberg_demo.connections import get_trino_engine
import sqlalchemy as sa

engine = get_trino_engine()
start_time = time.perf_counter()
with engine.connect() as conn:
    results = conn.execute(sa.text("""
select 
    category_code, 
    avg(price) as avg_price
from store.events
where event_time between
      CAST('2019-11-01' as timestamp) and CAST('2019-11-07' as timestamp)
group by category_code
ORDER BY avg_price DESC
""")).fetchall()

df = pl.from_records(results, schema=["category_code", "avg_price"])
print(df)
print(f"Trino: {time.perf_counter() - start_time:.5f} seconds")