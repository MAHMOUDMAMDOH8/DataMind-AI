import requests, base64, json

OM_URL = "http://localhost:8585/api/v1"

# Login
password_b64 = base64.b64encode(b"admin").decode("utf-8")
login = requests.post(
    f"{OM_URL}/users/login",
    json={"email": "admin@open-metadata.org", "password": password_b64},
    headers={"Content-Type": "application/json"},
)
token = login.json().get("accessToken", "")
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# Get the ingestion pipeline
ingest = requests.get(
    f"{OM_URL}/services/ingestionPipelines/name/NiFi-Pipeline-Ingestion",
    headers=headers,
)
data = ingest.json()
print(f"Ingestion: {data.get('name')} / {data.get('id')}")

# Generate the DAG Python code and write it to Airflow
dag_code = '''from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from metadata.workflow.metadata import MetadataWorkflow
from metadata.workflow.workflow_output_handler import print_status
from metadata.utils.logger import ingestion_logger
import yaml

logger = ingestion_logger()

default_args = {
    "owner": "datamind",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

config = '''
config_str = yaml.dump({
    "source": {
        "type": "custompipeine",
        "serviceName": "NiFi-Pipeline",
        "serviceConnection": {
            "config": {
                "type": "CustomPipeline",
                "sourcePythonClass": "nifi_http.metadata.NifiHttpSource",
                "connectionOptions": {
                    "hostPort": "http://nifi:8080"
                }
            }
        },
        "sourceConfig": {
            "config": {
                "type": "PipelineMetadata"
            }
        }
    },
    "sink": {"type": "metadata-rest", "config": {}},
    "workflowConfig": {
        "openMetadataServerConfig": {
            "hostPort": "http://openmetadata-server:8585/api",
            "authProvider": "openmetadata",
            "securityConfig": {
                "jwtToken": "eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsImF1ZCI6Ik9wZW5NZXRhZGF0YSIsImV4cCI6MTc5MDA4NTYzOCwiaWF0IjoxNzgyMzE4MDU3fQ.KwPMqk4j35uQmSVoD5JsRNwIVy0ldk4zsnHtfnFRasj9LRG7_wKFqWIl3vELXjrN8kWMv4r9pzNno8wY0T9bVjKCE3IVdNqBgpAYFq6k3Xc4ds0JjyZq7DqrNCvFSG6UYCQ7T3dHL4XfKT2TSfJ35cPFdfcz-dML7SuPGOOGbPo-_zWh8_goXbnf7ImUjUxkZYYWQ14GR2nTbRfpzNyOu3Td22k0FeKn7MTFtFNMi0G0AynwDSxlPH5pqOZyX8djY3f8CpGUTQQqX5v7-M0cQ5PKrSGWrgBWFj6F7eBTGZkEkJxYcLZtsEwSxkWh9Q-9IRQdsV4O09fPP7m9GdBmqg"
            }
        }
    }
}, default_flow_style=False)
dag_code += f"\\n{config_str}\\n'''\n\n"
dag_code += '''
with DAG(
    "NiFi-Pipeline-Ingestion",
    default_args=default_args,
    description="NiFi Pipeline Ingestion",
    schedule=None,
    catchup=False,
    tags=["nifi", "pipeline", "custom"],
) as dag:
    def ingest():
        workflow = MetadataWorkflow.create(config)
        workflow.execute()
        workflow.print_status()
        workflow.stop()

    ingest_task = PythonOperator(
        task_id="ingest_pipelines",
        python_callable=ingest,
    )
'''

# Write the DAG to OM Airflow
resp = requests.post(
    f"{OM_URL}/services/ingestionPipelines/{data['id']}/deploy",
    headers=headers,
)
print(f"Deploy via API: {resp.status_code}")

# If API fails, write DAG directly
if resp.status_code >= 400:
    print("API deploy failed, writing DAG directly...")
    airflow_url = "http://localhost:8080/api/v1"
    dag_id = "NiFi-Pipeline-Ingestion"

    # Upload DAG via Airflow REST API
    # Actually, let's write it to the Airflow DAGs dir via Docker exec
    print("The DAG needs to be deployed via Airflow UI or by writing directly to the DAGs folder.")
    print("Please run the following command:")
    print()
    print(f'  docker exec openmetadata-ingestion bash -c "cat > /opt/airflow/dags/{dag_id}.py << \\'DAGEOF\\'")
    print(dag_code[:200])
    print("...")
    print("DAGEOF")
