import os, requests
token = os.environ.get("OM_JWT_TOKEN", "")
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
url = "http://openmetadata-server:8585/api/v1"

print("=== Pipeline Services ===")
r = requests.get(f"{url}/services/pipelineServices?limit=50", headers=headers, timeout=10)
if r.status_code == 200:
    for s in r.json().get("data", []):
        print(f'  {s["name"]} ({s["serviceType"]})')

print("\n=== Pipelines ===")
r = requests.get(f"{url}/pipelines?limit=50", headers=headers, timeout=10)
if r.status_code == 200:
    for p in r.json().get("data", []):
        print(f'  {p["fullyQualifiedName"]}')
