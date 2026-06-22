from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.dates import days_ago
import json
import subprocess
import sys

default_args = {
    "owner": "datamind",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

SPARK = "docker exec spark-iceberg spark-submit --master local[*]"
JOBS = "/home/iceberg/jobs"
BRONZE_BUCKET = "telecom-bronze"
ENTITIES = [
    "calls", "sms", "CRM", "Network", "Payments",
    "Recharge", "Roaming", "data_usage", "Support",
]


def check_connection():
    result = subprocess.run(
        ["docker", "exec", "spark-iceberg", "spark-submit", "--master", "local[*]",
         "--conf", "spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog",
         "--conf", "spark.sql.catalog.local.type=rest",
         "--conf", "spark.sql.catalog.local.uri=http://rest:8181",
         "--conf", "spark.sql.catalog.local.warehouse=s3://warehouse",
         "--conf", "spark.sql.catalog.local.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
         "-e", "SHOW TABLES IN local.silver"],
        capture_output=True, text=True, timeout=60,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Spark connection failed: {result.stderr}")
    print(f"Spark connected. Silver tables:\n{result.stdout}")


def has_new_files(**context):
    result = subprocess.run(
        ["docker", "exec", "spark-iceberg", "python3", "-c", f"""
import boto3
c = boto3.client('s3', endpoint_url='http://minio:9000',
    aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin123')
entities = {json.dumps(ENTITIES)}
found = []
for e in entities:
    resp = c.list_objects_v2(Bucket='{BRONZE_BUCKET}', Prefix=f'{{e}}/',
        MaxKeys=5)
    if resp.get('KeyCount', 0) > 0:
        found.append(e)
import json; print(json.dumps(found))
        """],
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode != 0:
        print(f"Check failed: {result.stderr}")
        return False
    found = json.loads(result.stdout.strip().split("\n")[-1])
    print(f"Entities with new files: {found}")
    context["ti"].xcom_push(key="entities_with_data", value=found)
    return len(found) > 0


with DAG(
    "bronze_to_silver",
    default_args=default_args,
    description="Bronze to Silver: check connection, scan for new files, then transform",
    schedule="*/15 * * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["bronze", "silver", "spark", "iceberg"],
) as dag:

    check_conn = PythonOperator(
        task_id="check_spark_connection",
        python_callable=check_connection,
    )

    scan_bronze = ShortCircuitOperator(
        task_id="scan_bronze_for_new_files",
        python_callable=has_new_files,
        provide_context=True,
    )

    run_pipeline = BashOperator(
        task_id="transform_bronze_to_silver",
        bash_command=f"{SPARK} {JOBS}/bronze_to_silver/main.py",
    )

    check_conn >> scan_bronze >> run_pipeline
