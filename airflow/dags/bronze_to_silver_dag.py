from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import json
import subprocess

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
    ("calls",      "silver.calls",              False),
    ("sms",        "silver.sms",                False),
    ("CRM",        "silver.crm_registration",   True),
    ("Network",    "silver.network_metrics",    True),
    ("Payments",   "silver.payments",           False),
    ("Recharge",   "silver.recharges",          False),
    ("Roaming",    "silver.roaming",            False),
    ("data_usage", "silver.data_usage",         False),
    ("Support",    "silver.support_tickets",    False),
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
    print(f"Spark connected.\n{result.stdout}")


def scan_bronze(**context):
    result = subprocess.run(
        ["docker", "exec", "spark-iceberg", "python3", "-c", f"""
import boto3, json
c = boto3.client('s3', endpoint_url='http://minio:9000',
    aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin123')
entities = {json.dumps([e[0] for e in ENTITIES])}
found = []
for e in entities:
    resp = c.list_objects_v2(Bucket='{BRONZE_BUCKET}', Prefix=f'{{e}}/', MaxKeys=5)
    if resp.get('KeyCount', 0) > 0:
        found.append(e)
print(json.dumps(found))
        """],
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode != 0:
        print(f"Scan failed: {result.stderr}")
        context["ti"].xcom_push(key="entities_with_data", value=[])
        return
    found = json.loads(result.stdout.strip().split("\n")[-1])
    print(f"Entities with new files: {found}")
    context["ti"].xcom_push(key="entities_with_data", value=found)


with DAG(
    "bronze_to_silver",
    default_args=default_args,
    description="Bronze to Silver: connection check, scan, per-entity Spark transforms",
    schedule="*/15 * * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["bronze", "silver", "spark", "iceberg"],
) as dag:

    check_conn = PythonOperator(
        task_id="check_spark_connection",
        python_callable=check_connection,
    )

    scan = PythonOperator(
        task_id="scan_bronze_for_new_files",
        python_callable=scan_bronze,
        provide_context=True,
    )

    check_conn >> scan

    for entity, silver_name, _ in ENTITIES:
        task_id = f"transform_{entity}"
        entity_arg = entity
        t = BashOperator(
            task_id=task_id,
            bash_command=f"""
            ENTITIES=$(printf '%s' '{{{{ ti.xcom_pull(key="entities_with_data", task_ids="scan_bronze_for_new_files") }}}}')
            if echo "$ENTITIES" | python3 -c "import sys,json; d=json.load(sys.stdin); exit(0 if '{entity_arg}' in d else 1)" 2>/dev/null; then
                {SPARK} {JOBS}/bronze_to_silver/main.py --entity {entity_arg}
            else
                echo "No new files for {entity_arg}, skipping"
            fi
            """,
        )
        scan >> t
