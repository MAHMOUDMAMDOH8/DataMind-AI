import os, requests, json
token = os.environ.get("OM_JWT_TOKEN", "")
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
url = os.environ.get("OM_HOST", "http://openmetadata-server:8585/api/v1")

r = requests.get(f"{url}/pipelines?limit=50", headers=headers, timeout=10)
if r.status_code == 200:
    for p in r.json().get("data", []):
        if "nifi" in p["service"]["name"].lower():
            print(f'{p["fullyQualifiedName"]}  displayName="{p.get("displayName","")}" name="{p["name"]}"')
