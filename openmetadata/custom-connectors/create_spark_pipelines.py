import os, requests
token = os.environ.get("OM_JWT_TOKEN", "")
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
url = "http://openmetadata-server:8585/api/v1"

SPARK_SERVICE = "datamind-spark"

SPARK_PIPELINES = [
    ("bronze_to_silver", "Bronze → Silver ETL pipeline"),
    ("customer_360", "Silver → Gold customer_360 pipeline"),
    ("customer_usage_daily", "Silver → Gold customer_usage_daily pipeline"),
    ("daily_revenue", "Silver → Gold daily_revenue pipeline"),
    ("network_performance", "Silver → Gold network_performance pipeline"),
    ("payment_analytics", "Silver → Gold payment_analytics pipeline"),
    ("recharge_analytics", "Silver → Gold recharge_analytics pipeline"),
    ("roaming_analytics", "Silver → Gold roaming_analytics pipeline"),
    ("support_analytics", "Silver → Gold support_analytics pipeline"),
    ("fraud_monitoring", "Silver → Gold fraud_monitoring pipeline"),
    ("load_dims", "DIM table load pipeline"),
]

# Find spark service id
r = requests.get(f"{url}/services/pipelineServices/name/{SPARK_SERVICE}", headers=headers, timeout=10)
if r.status_code != 200:
    print(f"Service '{SPARK_SERVICE}' not found!")
    exit(1)
service = r.json()
service_id = service["id"]
print(f"Using pipeline service: {SPARK_SERVICE} (id={service_id})")

for name, desc in SPARK_PIPELINES:
    payload = {
        "name": name,
        "displayName": name.replace("_", " ").title(),
        "description": desc,
        "service": SPARK_SERVICE,
    }
    r2 = requests.post(f"{url}/pipelines", headers=headers, json=payload, timeout=10)
    if r2.status_code in (200, 201):
        print(f"  + Created pipeline {SPARK_SERVICE}.{name}")
    elif r2.status_code == 409:
        print(f"  ~ Already exists: {SPARK_SERVICE}.{name}")
    else:
        print(f"  ! Failed ({r2.status_code}): {name} - {r2.text[:200]}")
