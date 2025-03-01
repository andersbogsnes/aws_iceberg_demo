import duckdb
import s3fs
import sqlalchemy as sa
from aiobotocore.session import AioSession
from s3fs import S3FileSystem

from aws_iceberg_demo import TUTORIAL_TYPE


def _get_docker_trino_engine() -> sa.Engine:
    return sa.create_engine("trino://myuser:@localhost:8080/store")


def _get_athena_engine() -> sa.Engine:
    return sa.create_engine(
        "awsathena+arrow://:@athena.eu-north-1.amazonaws.com:443/"
        "stores?s3_staging_dir=s3://data-engineering-events/athena-staging")


def get_trino_engine() -> sa.Engine:
    if TUTORIAL_TYPE == "aws":
        return _get_athena_engine()
    if TUTORIAL_TYPE == "local":
        return _get_docker_trino_engine()
    raise ValueError(f"Unknown TUTORIAL_TYPE: {TUTORIAL_TYPE}")


def get_duckdb_conn() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect("local.ddb")
    conn.install_extension("iceberg")
    conn.load_extension("iceberg")
    if TUTORIAL_TYPE == "local":
        sql = """
            CREATE OR REPLACE SECRET aws (
        TYPE S3,
        ENDPOINT 'localhost:9000',
        KEY_ID 'minio',
        SECRET 'minio1234',
        USE_SSL false,
        URL_STYLE path
    )"""
    elif TUTORIAL_TYPE == "aws":
        sql = """
        CREATE OR REPLACE SECRET aws (
        TYPE S3,
        PROVIDER CREDENTIAL_CHAIN
        )"""
    else:
        raise ValueError(f"Unknown TUTORIAL_TYPE: {TUTORIAL_TYPE}")

    conn.sql(sql)
    return conn


def _get_minio_fs() -> S3FileSystem:
    return s3fs.S3FileSystem(endpoint_url="http://localhost:9000",
                             key="minio",
                             secret="minio1234",
                             use_ssl=False)


def _get_s3_fs() -> S3FileSystem:
    session = AioSession(profile="default")
    return s3fs.S3FileSystem(session=session)


def get_fs() -> s3fs.S3FileSystem:
    if TUTORIAL_TYPE == "local":
        return _get_minio_fs()
    if TUTORIAL_TYPE == "aws":
        return _get_s3_fs()
    raise ValueError(f"Unknown TUTORIAL_TYPE: {TUTORIAL_TYPE}")
