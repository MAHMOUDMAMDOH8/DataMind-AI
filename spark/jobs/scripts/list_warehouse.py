"""List folder names under warehouse/silver/ in MinIO."""
import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin123",
    region_name="us-east-1",
)

print("=== Folders under warehouse/silver/ ===")
paginator = s3.get_paginator("list_objects_v2")
for page in paginator.paginate(Bucket="warehouse", Prefix="silver/", Delimiter="/"):
    for p in page.get("CommonPrefixes") or []:
        print(f"  {p['Prefix']}")

print("\n=== ALL top-level folders in warehouse ===")
for page in paginator.paginate(Bucket="warehouse", Prefix="", Delimiter="/"):
    for p in page.get("CommonPrefixes") or []:
        print(f"  {p['Prefix']}")
