"""Clean up old dim UUID folders from warehouse root."""
import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id="admin",
    aws_secret_access_key="password",
    region_name="us-east-1",
)

paginator = s3.get_paginator("list_objects_v2")

print("=== Cleaning old top-level dim UUID folders ===")
for page in paginator.paginate(Bucket="warehouse", Prefix="", Delimiter="/"):
    for p in page.get("CommonPrefixes") or []:
        folder = p["Prefix"]
        if folder.startswith("dim_"):
            del_count = 0
            for del_page in paginator.paginate(Bucket="warehouse", Prefix=folder):
                objects = [{"Key": obj["Key"]} for obj in del_page.get("Contents") or []]
                if objects:
                    s3.delete_objects(Bucket="warehouse", Delete={"Objects": objects})
                    del_count += len(objects)
            print(f"  DELETED: {folder} ({del_count} objects)")

print("\n=== Final warehouse contents ===")
for page in paginator.paginate(Bucket="warehouse", Prefix="", Delimiter="/"):
    for p in page.get("CommonPrefixes") or []:
        print(f"  {p['Prefix']}")

print("\n=== Final silver/ contents ===")
for page in paginator.paginate(Bucket="warehouse", Prefix="silver/", Delimiter="/"):
    for p in page.get("CommonPrefixes") or []:
        print(f"  {p['Prefix']}")

print("\n=== Done ===")
