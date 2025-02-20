"""
# Querying the data

Now that we have some data, let's try querying it. We have a number of options, which is one of
the strengths of using a format like Iceberg. Open storage standards means any query engine can
choose to support it
"""
import datetime as dt
from aws_iceberg_demo.catalog import nessie_catalog

t = nessie_catalog.load_table("store.events")

# %%
"""
With pyiceberg, we can do some filtering before fetching the data. Pyiceberg uses Apache Arrow
# behind the scenes, so converting to your favourite DataFrame library is straight forward
"""
from pyiceberg.expressions import GreaterThanOrEqual, LessThanOrEqual, And
import polars as pl

df = t.scan(row_filter=And(GreaterThanOrEqual("event_time", "2019-11-01T00:00:00+00:00"),
                           LessThanOrEqual("event_time", "2019-11-07T00:00:00+00:00")),
            selected_fields=("event_time", "category_code", "price")
            ).to_arrow()
# %%
print(pl.from_arrow(df).group_by(pl.col("category_code")).agg(avg_price=pl.col("price").mean()))

# %%
"""
We can use something like duckdb to run SQL directly on the iceberg table.

Duckdb doesn't yet talk to the catalogue directly, it reads the metadata.json file directly,
which we can get from the catalogue
"""
from aws_iceberg_demo.connections import duckdb_conn

metadata_file = t.metadata_location

sql = f"""
SELECT 
    category_code, avg(price) as avg_price 
from iceberg_scan('{metadata_file}') 
where event_time between '2019-11-01' and '2019-11-07'
GROUP BY all
ORDER BY avg_price DESC
"""

dd_df = duckdb_conn.execute(sql).arrow()
print(pl.from_arrow(dd_df))

#%%
"""
We can use polars directly on the iceberg table as well
"""
import polars as pl

df = pl.scan_iceberg(t).group_by(pl.col("category_code")).agg(avg_price=pl.col("price").mean()).sort("avg_price", descending=True).collect()
print(df)

# %%
"""
We can easily switch to use a distributed compute system like Trino, which has very good support for Iceberg. Note that
AWS Athena is a managed version of Trino, giving us a serverless option as well

We can even run on top of SQLAlchemy, since Trino has a SQLAlchemy driver!
"""

import sqlalchemy as sa

engine = sa.create_engine("trino://myuser:@localhost:8080/store")

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

#%%
"""
A new Rust-based player is Daft, built on top of Apache Datafusion. When the storage format is standardized on Iceberg,
switching query engines becomes a matter of a few lines of code.
"""
import daft
from daft.io import IOConfig, S3Config

io_config = IOConfig(s3=S3Config(endpoint_url="http://localhost:9000",
                                 region_name="us-east-1",
                                 key_id="minio",
                                 access_key="minio1234",
                                 use_ssl=False))

df = daft.read_iceberg(t, io_config=io_config)
result = (
    df.where(daft.col("event_time").between(dt.datetime(2019, 11, 1, tzinfo=dt.UTC),
                                        dt.datetime(2019, 11, 7, tzinfo=dt.UTC)))
    .groupby("category_code")
    .agg(daft.col("price").mean().alias("avg_price"))
    .sort("avg_price", desc=True)
    .collect()
          )
result.show()