from pyiceberg.catalog import load_rest, Catalog
from pyiceberg.catalog.rest import RestCatalog

from aws_iceberg_demo import TUTORIAL_TYPE


def _get_glue_catalog() -> Catalog:
    return load_rest("awsglue", conf={
        "uri": "https://glue.eu-north-1.amazonaws.com/iceberg",
        "rest.sigv4-enabled": "true",
        "rest.signing-name": "glue",
        "rest.signing-region": "eu-north-1"
    })


def _get_nessie_catalog() -> Catalog:
    return load_rest("nessie", conf={
        "uri": "http://localhost:19120/iceberg",
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "minio",
        "s3.secret-access-key": "minio1234"
    })


def get_catalog() -> Catalog:
    if TUTORIAL_TYPE == "aws":
        return _get_glue_catalog()
    if TUTORIAL_TYPE == "local":
        return _get_nessie_catalog()
    raise ValueError(f"Unknown catalog type: {TUTORIAL_TYPE}")
