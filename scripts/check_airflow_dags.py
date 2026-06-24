import requests, base64

# Check Airflow DAGs
r = requests.get("http://localhost:8080/api/v1/dags?limit=100", auth=("admin", "admin"))
print(f"Airflow DAGs: {r.status_code}")
dags = r.json().get("dags", [])
nifi_dags = [d for d in dags if "nifi" in d.get("dag_id", "").lower()]
print(f"NiFi-related DAGs: {len(nifi_dags)}")
for d in nifi_dags:
    print(f'  {d["dag_id"]}: paused={d["is_paused"]}')

if not nifi_dags:
    print("No NiFi DAGs found")
    print(f"Total DAGs: {len(dags)}")
    for d in dags[:5]:
        print(f'  {d["dag_id"]}')
