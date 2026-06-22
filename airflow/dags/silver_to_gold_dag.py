from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "datamind",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

SPARK = "docker exec spark-iceberg spark-submit --master local[*]"
JOBS = "/home/iceberg/jobs"

with DAG(
    "silver_to_gold",
    default_args=default_args,
    description="Silver to Gold: load dims then marts",
    schedule=None,
    catchup=False,
    tags=["silver", "gold", "spark", "iceberg", "dims", "marts"],
) as dag:

    load_dims = BashOperator(
        task_id="load_dimensions",
        bash_command=f"{SPARK} {JOBS}/silver_to_gold/Dims/main.py",
    )

    MARTS = [
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

    mart_tasks = []
    for mart in MARTS:
        t = BashOperator(
            task_id=f"load_{mart}",
            bash_command=f"{SPARK} {JOBS}/silver_to_gold/marts/{mart}.py",
        )
        mart_tasks.append(t)

    load_dims >> mart_tasks
