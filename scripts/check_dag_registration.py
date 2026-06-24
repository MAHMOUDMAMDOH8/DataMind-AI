import requests, time

# Wait for Airflow to parse the new DAG
time.sleep(5)

r = requests.get(
    "http://localhost:8080/api/v1/dags?limit=100",
    auth=("admin", "DhhSndUYhkzKWkCp"),
)
dags = r.json().get("dags", [])
nifi = [d for d in dags if "nifi" in d["dag_id"].lower()]
print(f"Found {len(nifi)} NiFi DAGs")
for d in nifi:
    print(f'  {d["dag_id"]}: paused={d["is_paused"]}')

if not nifi:
    print(f"Total DAGs: {len(dags)}")
    for d in dags:
        print(f'  {d["dag_id"]}')
