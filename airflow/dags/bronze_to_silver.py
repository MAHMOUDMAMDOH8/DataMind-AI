from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.python import PythonSensor
from datetime import datetime

from config import (
    DEFAULT_ARGS, SPARK_CONTAINER, SPARK_MASTER, SPARK_PACKAGES,
    JOBS_DIR, SCHEDULE_BRONZE_TO_SILVER,
    BRONZE_TO_SILVER_MAIN, DOCKER_EXEC,
)

DOMAIN_ENTITIES = [
    "calls", "sms", "CRM", "Network", "Payments",
    "Recharge", "Roaming", "data_usage", "Support",
]

SILVER_NAMES = [
    "silver.calls", "silver.sms", "silver.crm_registration",
    "silver.network_metrics", "silver.payments",
    "silver.recharges", "silver.roaming",
    "silver.data_usage", "silver.support_tickets",
]

BRONZE_TABLES = [
    "calls", "sms", "CRM", "Network", "Payments",
    "Recharge", "Roaming", "data_usage", "Support",
]


def check_has_new_files(entity: str) -> bool:
    import boto3
    from botocore.exceptions import ClientError

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="admin",
        aws_secret_access_key="password",
        region_name="us-east-1",
    )
    prefix = f"{entity}/"
    try:
        resp = s3.list_objects_v2(Bucket="telecom-bronze", Prefix=prefix, MaxKeys=10)
        for obj in resp.get("Contents") or []:
            if obj["Key"].endswith(".json") and obj["Size"] > 0:
                return True
        return False
    except ClientError:
        return False


def make_sensor(entity: str, task_id: str) -> PythonSensor:
    return PythonSensor(
        task_id=task_id,
        python_callable=check_has_new_files,
        op_kwargs={"entity": entity},
        timeout=600,
        poke_interval=60,
        mode="reschedule",
        soft_fail=True,
    )


with DAG(
    "bronze_to_silver",
    default_args=DEFAULT_ARGS,
    description="Bronze to Silver ETL: clean, validate, and transform raw events",
    schedule=SCHEDULE_BRONZE_TO_SILVER,
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["etl", "bronze", "silver"],
) as dag:

    run_all = BashOperator(
        task_id="run_all_domains",
        bash_command=DOCKER_EXEC.format(
            container=SPARK_CONTAINER,
            command=(
                f"spark-submit --master {SPARK_MASTER} "
                f"--packages {SPARK_PACKAGES} "
                f"{BRONZE_TO_SILVER_MAIN}"
            ),
        ),
        retries=2,
        retry_delay=60,
    )

    run_all
