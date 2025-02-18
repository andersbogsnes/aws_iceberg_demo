"""
# Querying the data

Now that we have some data, let's try querying it. We have a number of options, which is one of
the strengths of using a format like Iceberg. Open storage standards means any query engine can
choose to support it
"""






# %%
"""# Grab the table reference"""
from aws_iceberg_demo.catalog import nessie_catalog
t = nessie_catalog.load_table("store.events")

# %%
"""
With pyiceberg, we can do some filtering before fetching the data. Pyiceberg uses Apache Arrow
# behind the scenes, so converting to your favourite DataFrame library is straight forward
"""
from pyiceberg.expressions import GreaterThanOrEqual, LessThanOrEqual, And
import polars as pl

df = t.scan(row_filter=And(GreaterThanOrEqual("event_time", "2019-12-01T00:00:00+00:00"),
                      LessThanOrEqual("event_time", "2019-12-07T00:00:00+00:00")),
              selected_fields=("event_time", "category_code", "price")
       ).to_arrow()

print(pl.from_arrow(df).group_by(pl.col("category_code")).agg(avg_price=pl.col("price").mean()))

# %%
"""
We can use something like duckdb to run SQL directly on the iceberg table.

Duckdb doesn't yet talk to the catalogue directly, it reads the metadata.json file directly,
which we can get from the catalogue
"""
import duckdb

conn = duckdb.connect("local.ddb")
# Iceberg support comes from the official duckdb extension
conn.install_extension("iceberg")
conn.load_extension("iceberg")

conn.execute("CREATE OR REPLACE SECRET minio ("
             "type S3,"
             "KEY_ID 'minio',"
             "SECRET 'minio1234',"
             "ENDPOINT 'localhost:9000',"
             "USE_SSL false,"
             "url_style path"
             ")")

metadata_file = t.metadata_location

sql = ("SELECT category_code, avg(price) as avg_price "
       f"from iceberg_scan('{metadata_file}') "
       "where event_time between '2019-12-01' and '2019-12-07'"
       "GROUP BY all")

dd_df = conn.execute(sql).arrow()

# %%
"""
We can use a distributed compute system like Trino, which has very good support for Iceberg
"""

import sqlalchemy as sa

engine = sa.create_engine("trino://myuser:@localhost:8080/store")

with engine.connect() as conn:

    results = conn.execute(sa.text("select "
              "category_code, "
              "avg(price) as avg_price "
              "from store.events "
              "where event_time between "
              "         CAST('2019-12-01' as timestamp) and CAST('2019-12-07' as timestamp) "
              "group by category_code")).fetchall()

df = pl.from_records(results, schema=["category_code", "avg_price"])
