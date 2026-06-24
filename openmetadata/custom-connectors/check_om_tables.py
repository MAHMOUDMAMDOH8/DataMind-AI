import os, requests
token = os.environ.get("OM_JWT_TOKEN", "")
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
url = "http://openmetadata-server:8585/api/v1"

print("=== Database Services ===")
r = requests.get(f"{url}/services/databaseServices?limit=50", headers=headers, timeout=10)
if r.status_code == 200:
    for s in r.json().get("data", []):
        print(f"  {s['name']} ({s['serviceType']})")

print("\n=== Tables ===")
r = requests.get(f"{url}/tables?limit=200", headers=headers, timeout=10)
if r.status_code == 200:
    for t in r.json().get("data", []):
        print(f"  {t['fullyQualifiedName']}")
else:
    print(f"Error: {r.status_code} {r.text[:300]}")
