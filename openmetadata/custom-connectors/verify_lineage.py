import os, requests, json
token = os.environ.get("OM_JWT_TOKEN", "")
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
url = os.environ.get("OM_HOST", "http://openmetadata-server:8585/api/v1")

r = requests.get(f"{url}/lineage/table/name/trino.iceberg.gold.customer_360", headers=headers, timeout=10)
if r.status_code == 200:
    data = r.json()
    up = data.get("upstreamEdges", [])
    down = data.get("downstreamEdges", [])
    print(f"customer_360 lineage: {len(up)} upstream, {len(down)} downstream")
    for e in up:
        fe = e.get("fromEntity")
        print(f"  <- {fe}")
    for e in down:
        te = e.get("toEntity")
        print(f"  -> {te}")
else:
    print(f"Error: {r.status_code} {r.text[:300]}")
