import requests, base64

OM_URL = "http://localhost:8585/api/v1"
password_b64 = base64.b64encode(b"admin").decode("utf-8")
login = requests.post(
    f"{OM_URL}/users/login",
    json={"email": "admin@open-metadata.org", "password": password_b64},
    headers={"Content-Type": "application/json"},
)
token = login.json().get("accessToken", "")
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

r = requests.get(f"{OM_URL}/pipelines?service=NiFi-Pipeline&limit=50", headers=headers)
data = r.json()
pipes = data.get("data", [])
print(f"Total pipelines ingested: {len(pipes)}")
for p in pipes:
    name = p.get("displayName", "?")
    pid = p.get("id", "?")[:8]
    print(f"  - {name} ({pid}...)")
