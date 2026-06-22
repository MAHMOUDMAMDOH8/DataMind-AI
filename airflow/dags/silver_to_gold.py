from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime

from config import (
    DEFAULT_ARGS, SPARK_CONTAINER, SPARK_MASTER,
    JOBS_DIR, SCHEDULE_SILVER_TO_GOLD,
    SILVER_TO_GOLD_DIMS_MAIN, SILVER_TO_GOLD_MARTS_SCRIPT,
    DOCKER_EXEC,
)

GOLD_MARTS = [
    "customer_360",
    "customer_usage_daily",
    "daily_revenue",
    "fraud_monitoring",
    "network_performance",
    "payment_analytics",
    "recharge_analytics",
    "roaming_analytics",
    "support_analytics",
]


def spark_submit_cmd(job_path: str) -> str:
    return DOCKER_EXEC.format(
        container=SPARK_CONTAINER,
        command=f"spark-submit --master {SPARK_MASTER} {job_path}",
    )


with DAG(
    "silver_to_gold",
    default_args=DEFAULT_ARGS,
    description="Silver to Gold ETL: build dimension tables and business marts",
    schedule=SCHEDULE_SILVER_TO_GOLD,
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["etl", "silver", "gold"],
) as dag:

    start = DummyOperator(task_id="start")

    load_dims = BashOperator(
        task_id="load_gold_dimensions",
        bash_command=spark_submit_cmd(SILVER_TO_GOLD_DIMS_MAIN),
        retries=2,
        retry_delay=60,
    )

    marts_tasks = []
    for mart in GOLD_MARTS:
        task = BashOperator(
            task_id=f"build_{mart}",
            bash_command=spark_submit_cmd(
                f"{JOBS_DIR}/silver_to_gold/marts/{mart}.py"
            ),
            retries=2,
            retry_delay=60,
        )
        marts_tasks.append(task)

    done = DummyOperator(task_id="done")

    start >> load_dims >> marts_tasks >> done
