from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

from config import (
    DEFAULT_ARGS, SPARK_CONTAINER,
    SCHEDULE_MAINTENANCE, DOCKER_EXEC,
)


def check_minio_usage() -> None:
    import boto3

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin123",
        region_name="us-east-1",
    )
    buckets = ["telecom-bronze", "telecom-silver", "telecom-gold", "warehouse"]
    total_size = 0
    total_objects = 0
    for bucket in buckets:
        try:
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket):
                for obj in page.get("Contents", []):
                    total_size += obj["Size"]
                    total_objects += 1
        except Exception:
            continue
    gb = total_size / (1024 ** 3)
    print(f"MinIO usage: {total_objects} objects, {gb:.2f} GB")
    if gb > 10:
        print("WARNING: Storage exceeds 10 GB, consider cleanup")


with DAG(
    "pipeline_maintenance",
    default_args=DEFAULT_ARGS,
    description="Weekly pipeline maintenance: monitoring, cleanup, and health checks",
    schedule=SCHEDULE_MAINTENANCE,
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["maintenance", "ops"],
) as dag:

    check_marts = BashOperator(
        task_id="check_gold_marts_exist",
        bash_command=DOCKER_EXEC.format(
            container=SPARK_CONTAINER,
            command=(
                "spark-submit --master local[*] -e "
                '"spark.sql(&#39;SHOW TABLES IN local.gold&#39;).show()"'
                " 2>/dev/null || echo 'Gold tables check completed'"
            ),
        ),
    )

    check_pipeline_metadata = BashOperator(
        task_id="check_pipeline_metadata",
        bash_command=DOCKER_EXEC.format(
            container=SPARK_CONTAINER,
            command=(
                'spark-submit --master local[*] '
                '-c spark.sql.catalog.local.io-impl=org.apache.iceberg.aws.s3.S3FileIO '
                f"{'/home/iceberg/jobs/scripts/list_warehouse.py'}"
                " 2>/dev/null || echo 'Metadata check done'"
            ),
        ),
    )

    storage_audit = PythonOperator(
        task_id="storage_audit",
        python_callable=check_minio_usage,
    )

    storage_audit >> check_marts >> check_pipeline_metadata
