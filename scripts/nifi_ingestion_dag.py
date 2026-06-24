from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from metadata.workflow.metadata import MetadataWorkflow

default_args = {
    "owner": "datamind",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

CONFIG = {
    "source": {
        "type": "custompipeline",
        "serviceName": "NiFi-Pipeline",
        "serviceConnection": {
            "config": {
                "type": "CustomPipeline",
                "sourcePythonClass": "nifi_http.metadata.NifiHttpSource",
                "connectionOptions": {"hostPort": "http://nifi:8080"},
            }
        },
        "sourceConfig": {"config": {"type": "PipelineMetadata"}},
    },
    "sink": {"type": "metadata-rest", "config": {}},
    "workflowConfig": {
        "openMetadataServerConfig": {
            "hostPort": "http://openmetadata-server:8585/api",
            "authProvider": "openmetadata",
            "securityConfig": {
                "jwtToken": "eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImluZ2VzdGlvbi1ib3QiLCJyb2xlcyI6WyJJbmdlc3Rpb25Cb3RSb2xlIl0sImVtYWlsIjoiaW5nZXN0aW9uLWJvdEBvcGVuLW1ldGFkYXRhLm9yZyIsImlzQm90Ijp0cnVlLCJ0b2tlblR5cGUiOiJCT1QiLCJpYXQiOjE3ODIzMDA3NjYsImV4cCI6bnVsbH0.N1xfx_7omX8lprcx-d3XVV1V7XcPkOCnzHt-pmSRGaRbYhyKXjNCDhKSw7cdn44BIotACGFIwzAkURnyMob-KblYcU5J562s1LgCr95bsCmJxAC6FBvM_8DhNPzNplKmuXUhFdHdCoNOhlTxdWGp9muVcD2y7NKlz8XWNmoq1m3QNsz6B7h3VAfVSxz5c9ZPPdv4UhQr-q7Xxfhzw0Uqs3EVTfORVFgEE-8Jo-EKb_tLt1tKrqjl3iMjdDdsE1P35x8tmzNYl3deSpnZEIHE1VQg-NRdfP4kENfTV2q0mPRf3Iktj4yzXmR04TNV7SETCoUpBeM5wBHF5Xs7XjWqpg"
            },
        }
    },
}


def ingest():
    workflow = MetadataWorkflow.create(CONFIG)
    workflow.execute()
    workflow.print_status()
    workflow.stop()


with DAG(
    "NiFi_Pipeline_Ingestion",
    default_args=default_args,
    description="NiFi Pipeline Ingestion",
    schedule=None,
    catchup=False,
    tags=["nifi", "pipeline", "custom"],
) as dag:
    ingest_task = PythonOperator(
        task_id="ingest_pipelines",
        python_callable=ingest,
    )
