"""
# Partitioning
In datalakes, there's no index on the table, instead we've historically stored the files in a way
that let the query engine skip the appropriate files.
It often looked something like this, where the files are physically written to a partitioned folder structure.
.
├── year=2024
│    └── month=1
│       └── data.parquet
├── year=2024
│    └── month=1
│       └── data.parquet

Iceberg lets us do partitioning on a metadata level instead using Hidden Partitioning - a transformation we define at
the metadata level, instead of the physical file layout.

In this example, we want to optimize a common analytical query - what's the top 10 categories per month?
"""
import pathlib

#%%
from aws_iceberg_demo.catalog import get_catalog
from aws_iceberg_demo.connections import get_duckdb_conn
import time

from aws_iceberg_demo.tables import event_table_schema

t = get_catalog().load_table("store.events")

conn = get_duckdb_conn()

start_time = time.perf_counter()
conn.sql(f"""
with monthly_categories as (
SELECT 
date_trunc('month', event_time) as event_month, 
category_code,
count(*) as num_events
FROM iceberg_scan('{t.metadata_location}')
where event_time between '2020-01-01' and '2020-01-31'
GROUP BY ALL
)
SELECT * from monthly_categories
QUALIFY row_number() OVER (partition by event_month ORDER BY num_events DESC) <= 10
ORDER BY event_month, num_events DESC
""").show()

print(f"Non-partitioned runtime: {time.perf_counter() - start_time} seconds")

#%%
from pyiceberg.partitioning import MonthTransform, PartitionSpec, PartitionField

with t.update_spec() as spec:
    spec.add_field("event_time", MonthTransform(), "monthly_event_time")

"""
Note that this will only take effect on new data added to the table, and not the old, so we'd have to rewrite the table
to be able to use this new partitioning. To demonstrate, let's try creating a new table with partitioning
"""
#%%
import pyarrow.parquet as pq
catalog = get_catalog()
partition_spec = PartitionSpec(PartitionField(source_id=1, field_id=1, transform=MonthTransform(), name="event_month_transform"))
t = catalog.create_table_if_not_exists("store.events_v2",
                                   schema=event_table_schema,
                                   partition_spec=partition_spec,
                                   location="s3://data-engineering-events/warehouse/events_v2")

data_files = ["2019-Oct.parquet",
              "2019-Nov.parquet",
              "2019-Dec.parquet",
              "2020-Jan.parquet",
              "2020-Feb.parquet",
              "2020-Mar.parquet",
              "2020-Apr.parquet"]
for data_file in data_files:
    print(data_file)
    df = pq.read_table(pathlib.Path("data/parquet/sampled") / data_file, schema=t.schema().as_arrow())
    t.append(df)

#%%
t = catalog.load_table("store.events_v2")

conn = get_duckdb_conn()

start_time = time.perf_counter()

conn.sql(f"""
with monthly_categories as (
SELECT 
date_trunc('month', event_time) as event_month, 
category_code,
count(*) as num_events
FROM iceberg_scan('{t.metadata_location}')
where event_time between '2020-01-01' and '2020-01-31'
GROUP BY ALL
)
SELECT * from monthly_categories
QUALIFY rank() OVER (partition by event_month ORDER BY num_events DESC) <= 10
ORDER BY event_month, num_events DESC

""").show()

print(f"Partitioned runtime: {time.perf_counter() - start_time} seconds")


#%%
from aws_iceberg_demo.connections import get_trino_engine
import sqlalchemy as sa
import polars as pl

engine = get_trino_engine()
start_time = time.perf_counter()
with engine.connect() as conn:
   r = conn.execute(sa.text(f"""
    with monthly_categories as (
    SELECT 
    date_trunc('month', event_time) as event_month, 
    category_code,
    count(*) as num_events
    FROM store.events_v2
    GROUP BY 1, 2
    ), ranked as (
    SELECT 
        event_month, 
        category_code,
        num_events, 
        rank() OVER (partition by event_month order by num_events desc) as rank_number
    from monthly_categories 
    ORDER BY event_month, num_events DESC
)

select event_month, num_events from ranked where rank_number <= 10
    """))
print(pl.from_arrow(r.cursor.as_arrow()))

print(f"Partitioned runtime: {time.perf_counter() - start_time} seconds")

