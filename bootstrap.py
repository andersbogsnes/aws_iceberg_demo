import httpx

config = {
  "warehouse-name": "warehouse",
  "project-id": "00000000-0000-0000-0000-000000000000",
  "storage-profile": {
    "type": "s3",
    "bucket": "warehouse",
    "key-prefix": "lakekeeper",
    "assume-role-arn": None,
    "endpoint": "http://minio:9000",
    "region": "local-01",
    "path-style-access": True,
    "flavor": "minio",
    "sts-enabled": True
  },
  "storage-credential": {
    "type": "s3",
    "credential-type": "access-key",
    "aws-access-key-id": "minio",
    "aws-secret-access-key": "minio1234"
  }
}

r = httpx.post("http://localhost:8181/management/v1/warehouse", json=config)

print(r)



