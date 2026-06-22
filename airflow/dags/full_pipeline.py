from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

from config import (
    DEFAULT_ARGS, SPARK_CONTAINER, SPARK_MASTER, SPARK_PACKAGES,
    JOBS_DIR, BRONZE_TO_SILVER_MAIN,
    SILVER_TO_GOLD_DIMS_MAIN, LOAD_DIMS_SCRIPT,
    DOCKER_EXEC, DOCKER_SPARK_SUBMIT,
)

GOLD_MARTS = [
    ("customer_360", f"{JOBS_DIR}/silver_to_gold/marts/customer_360.py"),
    ("customer_usage_daily", f"{JOBS_DIR}/silver_to_gold/marts/customer_usage_daily.py"),
    ("daily_revenue", f"{JOBS_DIR}/silver_to_gold/marts/daily_revenue.py"),
    ("fraud_monitoring", f"{JOBS_DIR}/silver_to_gold/marts/fraud_monitoring.py"),
    ("network_performance", f"{JOBS_DIR}/silver_to_gold/marts/network_performance.py"),
    ("payment_analytics", f"{JOBS_DIR}/silver_to_gold/marts/payment_analytics.py"),
    ("recharge_analytics", f"{JOBS_DIR}/silver_to_gold/marts/recharge_analytics.py"),
    ("roaming_analytics", f"{JOBS_DIR}/silver_to_gold/marts/roaming_analytics.py"),
    ("support_analytics", f"{JOBS_DIR}/silver_to_gold/marts/support_analytics.py"),
]


def spark_submit(job_path: str, extra_conf: str = "") -> str:
    base = DOCKER_SPARK_SUBMIT.format(
        container=SPARK_CONTAINER,
        master=SPARK_MASTER,
        packages=SPARK_PACKAGES,
        job_path=job_path,
    )
    return base


with DAG(
    "full_pipeline",
    default_args={
        **DEFAULT_ARGS,
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    description="End-to-end pipeline: Bronze → Silver → Gold",
    schedule=timedelta(hours=1),
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["etl", "pipeline", "full"],
) as dag:

    start = DummyOperator(task_id="start")

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=spark_submit(BRONZE_TO_SILVER_MAIN),
        execution_timeout=timedelta(minutes=30),
    )

    load_silver_dims = BashOperator(
        task_id="load_silver_dimensions",
        bash_command=spark_submit(LOAD_DIMS_SCRIPT),
        execution_timeout=timedelta(minutes=15),
    )

    load_gold_dims = BashOperator(
        task_id="load_gold_dimensions",
        bash_command=spark_submit(SILVER_TO_GOLD_DIMS_MAIN),
        execution_timeout=timedelta(minutes=15),
    )

    marts_tasks = []
    for name, path in GOLD_MARTS:
        task = BashOperator(
            task_id=f"build_{name}",
            bash_command=spark_submit(path),
            execution_timeout=timedelta(minutes=15),
        )
        marts_tasks.append(task)

    done = DummyOperator(task_id="done")

    start >> bronze_to_silver >> load_silver_dims >> load_gold_dims >> marts_tasks >> done
