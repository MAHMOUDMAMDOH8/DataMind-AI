"""
Recreate all silver tables with clean physical storage paths.
Drops existing tables and recreates them with explicit location
so the S3 paths are clean like: warehouse/silver/calls/
"""
import sys
sys.path.insert(0, "/home/iceberg/jobs")
from scripts.spark_init import get_spark_session
import boto3

spark = get_spark_session("FixSilverPaths")

# Ensure namespace exists
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.silver")

tables = [
    "silver.calls",
    "silver.sms",
    "silver.crm_registration",
    "silver.crm_profile_update",
    "silver.network_metrics",
    "silver.network_qos_reports",
    "silver.payments",
    "silver.recharges",
    "silver.roaming",
    "silver.data_usage",
    "silver.support_tickets",
]

WAREHOUSE = "s3://warehouse"

for t in tables:
    full = f"local.{t}"
    clean_name = t.replace("silver.", "")  # e.g., "calls"
    clean_location = f"{WAREHOUSE}/silver/{clean_name}"

    print(f"\n=== {t} ===")

    # Read existing data if table exists
    df = None
    if spark.catalog.tableExists(full):
        df = spark.table(full)
        count = df.count()
        print(f"  Read {count} records from existing table")

        # Cache the data in memory so we can re-write after drop
        df = df.localCheckpoint()

        # Drop the old table (without PURGE since GC is disabled)
        spark.sql(f"DROP TABLE IF EXISTS {full}")
        print(f"  Dropped old table")

    if df is not None and df.count() > 0:
        # Recreate with explicit clean location
        df.writeTo(full).tableProperty("location", clean_location).create()
        print(f"  Created at {clean_location} with {df.count()} records")
    else:
        print(f"  No data to recreate")

# Now clean up old UUID folders from S3
print("\n\n=== Cleaning old UUID folders from S3 ===")
s3 = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin123",
    region_name="us-east-1",
)

# List all folders under silver/
paginator = s3.get_paginator("list_objects_v2")

# Find and delete old UUID-based folders under silver/
clean_names = {t.replace("silver.", "") for t in tables}  # e.g., {"calls", "sms", ...}

for page in paginator.paginate(Bucket="warehouse", Prefix="silver/", Delimiter="/"):
    for p in page.get("CommonPrefixes") or []:
        folder = p["Prefix"]  # e.g., "silver/calls_3698c82d-.../
        folder_name = folder.replace("silver/", "").rstrip("/")
        
        # Check if this is an old UUID folder (contains underscore followed by UUID pattern)
        if folder_name in clean_names:
            print(f"  KEEP: {folder}")
        else:
            # Delete all objects in this old folder
            del_count = 0
            for del_page in paginator.paginate(Bucket="warehouse", Prefix=folder):
                objects = [{"Key": obj["Key"]} for obj in del_page.get("Contents") or []]
                if objects:
                    s3.delete_objects(Bucket="warehouse", Delete={"Objects": objects})
                    del_count += len(objects)
            print(f"  DELETED: {folder} ({del_count} objects)")

# Also clean up old top-level UUID folders
print("\n=== Cleaning old top-level UUID folders ===")
for page in paginator.paginate(Bucket="warehouse", Prefix="", Delimiter="/"):
    for p in page.get("CommonPrefixes") or []:
        folder = p["Prefix"]
        # Skip silver/ and pipeline_metadata/ and dim_ folders
        if folder in ("silver/", "pipeline_metadata/") or folder.startswith("dim_"):
            print(f"  KEEP: {folder}")
            continue
        # Delete old UUID top-level folders (calls_xxx, sms_xxx, etc.)
        del_count = 0
        for del_page in paginator.paginate(Bucket="warehouse", Prefix=folder):
            objects = [{"Key": obj["Key"]} for obj in del_page.get("Contents") or []]
            if objects:
                s3.delete_objects(Bucket="warehouse", Delete={"Objects": objects})
                del_count += len(objects)
        print(f"  DELETED: {folder} ({del_count} objects)")

# Final verification
print("\n\n=== Final warehouse/silver/ contents ===")
for page in paginator.paginate(Bucket="warehouse", Prefix="silver/", Delimiter="/"):
    for p in page.get("CommonPrefixes") or []:
        print(f"  {p['Prefix']}")

print("\n=== Final top-level warehouse contents ===")
for page in paginator.paginate(Bucket="warehouse", Prefix="", Delimiter="/"):
    for p in page.get("CommonPrefixes") or []:
        print(f"  {p['Prefix']}")

spark.stop()
print("\n=== Done ===")
