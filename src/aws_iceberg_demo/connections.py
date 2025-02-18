import sqlalchemy as sa
import duckdb

trino_engine = sa.create_engine("trino://myuser:@localhost:8080/store")

duckdb_conn = duckdb.connect()
duckdb_conn.install_extension("iceberg")
duckdb_conn.load_extension("iceberg")

