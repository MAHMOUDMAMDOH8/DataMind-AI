from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from datetime import datetime, timedelta

from config import DEFAULT_ARGS

SOURCE_DIR = "/opt/airflow/source"
PYTHON = "/usr/local/bin/python"
BOOTSTRAP_SERVERS = "kafka:29092"


with DAG(
    "data_ingestion",
    default_args=DEFAULT_ARGS,
    description="Publish sample telecom events to Kafka from all 7 source systems",
    schedule=None,
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["ingestion", "source", "kafka"],
    params={
        "rate": 20,
        "duration_seconds": 60,
        "clean": False,
    },
) as dag:

    start = DummyOperator(task_id="start")

    run_all_producers = BashOperator(
        task_id="run_all_producers",
        bash_command=(
            f"cd {SOURCE_DIR} && "
            f"{PYTHON} -m runners.run_all "
            f"--rate {{{{ params.get('rate', 20) }}}} "
            f"--duration-seconds {{{{ params.get('duration_seconds', 60) }}}} "
            f"--bootstrap-servers {BOOTSTRAP_SERVERS} "
            f"{{% if params.get('clean', False) %}}--clean{{% endif %}}"
        ),
        execution_timeout=timedelta(minutes=5),
    )

    done = DummyOperator(task_id="done")

    start >> run_all_producers >> done
