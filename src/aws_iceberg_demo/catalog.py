from pyiceberg.catalog import load_rest

aws_catalog = load_rest("awsglue", conf={
    "uri": "https://glue.eu-north-1.amazonaws.com/iceberg",
    "rest.sigv4-enabled": "true",
    "rest.signing-name": "glue",
    "rest.signing-region": "eu-north-1"
})

nessie_catalog = load_rest("nessie", conf={
    "uri": "http://localhost:19120/iceberg",
    "s3.endpoint": "http://localhost:9000",
    "s3.access-key-id": "minio",
    "s3.secret-access-key": "minio1234"
})