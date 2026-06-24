import requests, json, base64

OM_URL = "http://localhost:8585/api/v1"

# Get auth token
password_b64 = base64.b64encode(b"admin").decode("utf-8")
login = requests.post(
    f"{OM_URL}/users/login",
    json={"email": "admin@open-metadata.org", "password": password_b64},
    headers={"Content-Type": "application/json"},
)
if login.status_code != 200:
    print(f"Login failed: {login.status_code} - {login.text}")
    exit(1)

token = login.json().get("accessToken", "")
print(f"Logged in, token: {token[:20]}...")

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json",
}

# Check if service already exists
existing = requests.get(
    f"{OM_URL}/services/pipelineServices/name/NiFi-Pipeline",
    headers=headers,
)
if existing.status_code == 200:
    print(f"Service already exists: {existing.json()['id']}")
    exit(0)

# Create the pipeline service
pipeline_service = {
    "name": "NiFi-Pipeline",
    "displayName": "NiFi Pipeline Service",
    "description": "Custom NiFi HTTP pipeline connector for bronze layer ingestion",
    "serviceType": "CustomPipeline",
    "connection": {
        "config": {
            "type": "CustomPipeline",
            "sourcePythonClass": "nifi_http.metadata.NifiHttpSource",
            "connectionOptions": {
                "hostPort": "http://nifi:8080"
            }
        }
    }
}

resp = requests.post(
    f"{OM_URL}/services/pipelineServices",
    json=pipeline_service,
    headers=headers,
)
print(f"Create service: {resp.status_code}")
if resp.status_code in (200, 201):
    data = resp.json()
    print(f"Pipeline Service created: {data['id']}")
    print(f"Name: {data['name']}")

    # Trigger ingestion
    ingest = {
        "name": "NiFi-Pipeline-Ingestion",
        "displayName": "NiFi Pipeline Ingestion",
        "service": "NiFi-Pipeline",
        "pipelineType": "metadata",
        "sourceConfig": {
            "config": {
                "type": "PipelineMetadata",
                "includeLineage": True,
                "markDeletedPipelines": True,
            }
        }
    }
    resp2 = requests.post(
        f"{OM_URL}/services/ingestionPipelines",
        json=ingest,
        headers=headers,
    )
    print(f"Create ingestion: {resp2.status_code}")
    if resp2.status_code in (200, 201):
        ingest_data = resp2.json()
        ingest_id = ingest_data.get("id") or ingest_data.get("fullyQualifiedName")
        print(f"Ingestion created: {ingest_id}")

        # Deploy and trigger
        deploy = requests.post(
            f"{OM_URL}/services/ingestionPipelines/{ingest_id}/deploy",
            headers=headers,
        )
        print(f"Deploy: {deploy.status_code}")

        trigger = requests.post(
            f"{OM_URL}/services/ingestionPipelines/{ingest_id}/trigger",
            headers=headers,
        )
        print(f"Trigger: {trigger.status_code}")
    else:
        print(f"Error: {resp2.text[:500]}")
else:
    print(f"Error: {resp.text[:1000]}")
