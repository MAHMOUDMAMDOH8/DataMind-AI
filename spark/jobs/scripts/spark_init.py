import json
import os
import uuid
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import input_file_name, regexp_extract

warehouse_name_bucket = "warehouse"
pipeline_metadata_prefix = "pipeline_metadata"


def _normalize_endpoint_url(endpoint_url: str) -> str:
    url = endpoint_url.rstrip("/")
    if not url.startswith("http"):
        url = f"http://{url}"
    return url


def get_spark_session(app_name: str = "DataMindAI"):
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "nessie")
        .config("spark.sql.catalog.local.uri", "http://nessie:19120/api/v1")
        .config("spark.sql.catalog.local.ref", "main")
        .config("spark.sql.catalog.local.warehouse", "s3://warehouse/")
        .config("spark.sql.catalog.local.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.local.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.local.s3.path-style-access", "true")
        .config("spark.sql.catalog.local.s3.access-key-id", "minioadmin")
        .config("spark.sql.catalog.local.s3.secret-access-key", "minioadmin123")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "1")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )


def has_new_files(endpoint_url: str, bucket: str, table_name: str, base_layer: str = "") -> bool:
    url = _normalize_endpoint_url(endpoint_url)
    s3 = boto3.client(
        "s3",
        endpoint_url=url,
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin123",
        region_name="us-east-1",
    )
    prefix = f"{base_layer}/{table_name}/" if base_layer else f"{table_name}/"
    try:
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=10)
        for obj in resp.get("Contents") or []:
            key = obj["Key"]
            if key.endswith(".json") and obj["Size"] > 0:
                return True
        return False
    except ClientError:
        return False


def read_from_iceberg(table_name, spark):

    if spark is None:
        # Try to get active SparkSession
        try:
            spark = SparkSession.getActiveSession()
            if spark is None:
                spark = get_spark_session()
        except:
            spark = get_spark_session()

    full_table = f"local.{table_name}"

    try:
        if not spark.catalog.tableExists(full_table):
            print(f"Table {full_table} does not exist.")
            return None

        print(f"Reading data from Iceberg table: {full_table}")
        df = spark.table(full_table)
        count = df.count()
        print(f"Successfully read {count} records from {full_table}")

        return df

    except Exception as e:
        print(f"Error reading data from Iceberg table: {e}")
        raise


def write_to_iceberg(df, table_name, mode="append"):
    spark = df.sparkSession
    full_table = f"local.{table_name}"

    if not spark.catalog.tableExists(full_table):
        df.writeTo(full_table).create()
    else:
        if mode == "overwrite":
            df.writeTo(full_table).overwritePartitions()
        else:
            df.writeTo(full_table).append()

def read_bronze_table(
    spark: SparkSession,
    table_name: str,
    bucket: str = "telecom-bronze",
    base_layer: str = "",
):
    s3_path = f"s3a://{bucket}/{base_layer}/{table_name}/"

    return (
        spark.read.option("multiLine", "true")
        .option("recursiveFileLookup", "true")
        .json(s3_path)
        .withColumn("source_file", input_file_name())
        .withColumn(
            "event_date",
            regexp_extract("source_file", r"/(\d{4}-\d{2}-\d{2})/", 1),
        )
        .withColumn(
            "event_hour",
            regexp_extract("source_file", r"/\d{4}-\d{2}-\d{2}/(\d{2})/", 1),
        )
    )


def read_from_iceberg(table_name: str, spark: SparkSession = None):
    if spark is None:
        try:
            spark = SparkSession.getActiveSession()
            if spark is None:
                spark = get_spark_session()
        except Exception:
            spark = get_spark_session()

    full_table = f"local.{table_name}"

    try:
        if not spark.catalog.tableExists(full_table):
            print(f"Table {full_table} does not exist.")
            return None

        print(f"Reading data from Iceberg table: {full_table}")
        df = spark.table(full_table)
        count = df.count()
        print(f"Successfully read {count} records from {full_table}")
        return df

    except Exception as e:
        print(f"Error reading data from Iceberg table: {e}")
        raise


def write_to_iceberg(df: DataFrame, table_name: str, mode: str = "append"):
    spark = df.sparkSession
    full_table = f"local.{table_name}"

    if not spark.catalog.tableExists(full_table):
        df.writeTo(full_table).create()
    else:
        if mode == "overwrite":
            df.writeTo(full_table).overwritePartitions()
        else:
            df.writeTo(full_table).append()


def move_to_archive(
    df: DataFrame,
    table_name: str,
    archive_bucket: str = "s3a://telecom-bronze",
    archive_layer: str = "archive",
    bronze_base_layer: str = "",
):
    spark = df.sparkSession

    if "source_file" not in df.columns:
        print(f"No source_file column, skipping archive for {table_name}")
        return df

    jvm = spark.sparkContext._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

    if "event_date" in df.columns:
        rows = df.select("source_file", "event_date").distinct().collect()
    else:
        rows = df.select("source_file").distinct().collect()

    fs = None
    archived_count = 0

    for row in rows:
        src = row.source_file
        if not src:
            continue

        date_str = None
        if "event_date" in df.columns:
            try:
                date_str = row.event_date
            except Exception:
                date_str = None

        if not date_str:
            date_str = datetime.now().strftime("%Y%m%d")
        else:
            date_str = str(date_str).replace("-", "")

        archive_base = f"{archive_bucket}/{archive_layer}/{table_name}/{date_str}"

        if fs is None:
            fs = jvm.org.apache.hadoop.fs.FileSystem.get(
                jvm.java.net.URI(archive_base), hadoop_conf
            )

        dest_base_path = jvm.org.apache.hadoop.fs.Path(archive_base)
        if not fs.exists(dest_base_path):
            fs.mkdirs(dest_base_path)

        src_path = jvm.org.apache.hadoop.fs.Path(src)
        dest_path = jvm.org.apache.hadoop.fs.Path(
            f"{archive_base}/{src_path.getName()}"
        )
        fs.rename(src_path, dest_path)
        archived_count += 1

    print(
        f"Archived {archived_count} raw files under {archive_bucket}/{archive_layer}/{table_name}/<event_date>"
    )

    return df


def delete_raws_in_bronze(
    table_name: str,
    date: str = None,
    hour: str = None,
    bronze_bucket: str = "s3a://telecom-bronze",
    bronze_base_layer: str = "",
    spark: SparkSession = None,
):
    if spark is None:
        spark = SparkSession.getActiveSession() or get_spark_session()

    base_path = f"{bronze_bucket}/{bronze_base_layer}/{table_name}" if bronze_base_layer else f"{bronze_bucket}/{table_name}"

    if date:
        base_path += f"/{date}"
        if hour:
            base_path += f"/{hour}"

    jvm = spark.sparkContext._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(
        jvm.java.net.URI(base_path), hadoop_conf
    )

    path = jvm.org.apache.hadoop.fs.Path(base_path)

    if not fs.exists(path):
        print(f"No files found at {base_path}")
        return 0

    deleted = fs.delete(path, True)
    print(f"Deleted bronze data at {base_path}")
    return deleted


def write_pipeline_metadata_event(
    endpoint_url: str,
    *,
    pipeline_stage: str,
    entity: str,
    action: str,
    source_files: list[str] | None = None,
    row_count: int | None = None,
    target: str | None = None,
    status: str = "success",
    error_message: str | None = None,
    extra: dict | None = None,
) -> str | None:
    try:
        base = (pipeline_metadata_prefix or "pipeline_metadata").rstrip("/")
        events_pre = f"{base}/events/"
        dt = datetime.now(timezone.utc)
        day = dt.strftime("%Y/%m/%d")
        eid = str(uuid.uuid4())
        key = f"{events_pre}{day}/{dt.strftime('%H%M%S')}_{eid}.json"
        payload = {
            "schema_version": 1,
            "event_id": eid,
            "event_time_utc": dt.isoformat(),
            "pipeline_stage": pipeline_stage,
            "entity": entity,
            "action": action,
            "source_files": list(source_files) if source_files else [],
            "source_file_count": len(source_files) if source_files else 0,
            "row_count": row_count,
            "target": target,
            "status": status,
            "error_message": error_message,
            "warehouse_bucket": warehouse_name_bucket,
            "extra": extra or {},
        }
        url = _normalize_endpoint_url(endpoint_url)
        s3 = boto3.client(
            "s3",
            endpoint_url=url,
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin123",
            region_name="us-east-1",
        )
        s3.put_object(
            Bucket=warehouse_name_bucket,
            Key=key,
            Body=json.dumps(payload, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        return key
    except Exception as e:
        print(f"[pipeline metadata] S3 write skipped: {e}")
        return None


def load_pipeline_metadata_events(
    endpoint_url: str,
    bucket: str | None = None,
    prefix: str | None = None,
    max_events: int = 500,
) -> list[dict]:
    bucket = bucket or warehouse_name_bucket
    root = (prefix or pipeline_metadata_prefix or "pipeline_metadata").rstrip("/") + "/events/"
    url = _normalize_endpoint_url(endpoint_url)
    s3 = boto3.client(
        "s3",
        endpoint_url=url,
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin123",
        region_name="us-east-1",
    )
    keys: list[tuple] = []
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=root):
            for obj in page.get("Contents") or []:
                k = obj["Key"]
                if k.endswith(".json"):
                    keys.append((obj.get("LastModified"), k))
    except ClientError:
        return []
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    keys.sort(key=lambda x: x[0] or epoch, reverse=True)
    out: list[dict] = []
    for _, k in keys[:max_events]:
        try:
            body = s3.get_object(Bucket=bucket, Key=k)["Body"].read()
            doc = json.loads(body.decode("utf-8"))
            doc["_s3_key"] = k
            out.append(doc)
        except Exception:
            continue
    return out
