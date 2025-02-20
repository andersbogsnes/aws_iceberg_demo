import sqlalchemy as sa
import duckdb
import s3fs

trino_engine = sa.create_engine("trino://myuser:@localhost:8080/store")

duckdb_conn = duckdb.connect()

duckdb_conn.sql("""
CREATE OR REPLACE SECRET minio (
    TYPE S3,
    ENDPOINT 'localhost:9000',
    KEY_ID 'minio',
    SECRET 'minio1234',
    USE_SSL false,
    URL_STYLE path
)""")
duckdb_conn.install_extension("iceberg")
duckdb_conn.load_extension("iceberg")

fs = s3fs.S3FileSystem(endpoint_url="http://localhost:9000", key="minio", secret="minio1234", use_ssl=False)