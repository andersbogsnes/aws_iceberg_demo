from aws_iceberg_demo.catalog import nessie_catalog
import pyarrow.parquet as pq

# Fetch the existing table
t = nessie_catalog.load_table("store.events")


# Load the data from a Parquet file. Note that we're using the Iceberg schema to load the data,
# to make sure the data is loaded with the correct schema
df = pq.read_table("/home/anders/projects/tutorials/aws_iceberg_demo/data/parquet/2019-Dec.parquet",
                   schema=t.schema().as_arrow())
t.append(df)