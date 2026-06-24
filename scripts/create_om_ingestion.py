import requests, json, base64

OM_URL = "http://localhost:8585/api/v1"

password_b64 = base64.b64encode(b"admin").decode("utf-8")
login = requests.post(
    f"{OM_URL}/users/login",
    json={"email": "admin@open-metadata.org", "password": password_b64},
    headers={"Content-Type": "application/json"},
)
token = login.json().get("accessToken", "")
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# Get the pipeline service ID
svc = requests.get(f"{OM_URL}/services/pipelineServices/name/NiFi-Pipeline", headers=headers)
svc_id = svc.json()["id"]
svc_name = svc.json()["name"]
print(f"Service: {svc_name} ({svc_id})")

# Create ingestion pipeline
ingest = {
    "name": "NiFi-Pipeline-Ingestion",
    "displayName": "NiFi Pipeline Ingestion",
    "service": {"id": svc_id, "type": "pipelineService"},
    "pipelineType": "metadata",
    "sourceConfig": {
        "config": {
            "type": "PipelineMetadata",
            "includeLineage": True,
            "markDeletedPipelines": True,
        }
    },
    "airflowConfig": {},
}
resp = requests.post(f"{OM_URL}/services/ingestionPipelines", json=ingest, headers=headers)
print(f"Create ingestion: {resp.status_code}")
if resp.status_code in (200, 201):
    data = resp.json()
    print(f"ID: {data.get('id', '?')}")
    print(f"Name: {data.get('name', '?')}")
    
    # Deploy and trigger
    ingest_id = data.get("id")
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
    print(resp.text[:1000])
