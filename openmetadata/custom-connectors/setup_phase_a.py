import os, requests, json
token = os.environ.get("OM_JWT_TOKEN", "")
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
url = os.environ.get("OM_HOST", "http://openmetadata-server:8585/api/v1")

# ── 1. Create Kafka Service ──────────────────────────────────────────────────
KAFKA_SERVICE = "Kafka"
print("=== Kafka Service ===")
r = requests.get(f"{url}/services/messagingServices/name/{KAFKA_SERVICE}", headers=headers, timeout=10)
if r.status_code == 200:
    print(f"  ~ Already exists: {KAFKA_SERVICE}")
else:
    payload = {
        "name": KAFKA_SERVICE,
        "serviceType": "Kafka",
        "connection": {"config": {"type": "Kafka", "bootstrapServers": "kafka:9092"}},
    }
    r = requests.post(f"{url}/services/messagingServices", headers=headers, json=payload, timeout=10)
    if r.status_code in (200, 201):
        print(f"  + Created Kafka service")
    else:
        print(f"  ! Failed ({r.status_code}): {r.text[:200]}")
        exit(1)

# ── 2. Register Kafka Topics ─────────────────────────────────────────────────
TOPICS = [
    "customer_topic", "calls_topic", "sms_topic", "data_usage_topic",
    "network_metrics_topic", "payments_topic", "recharge_topic",
    "roaming_topic", "tickets_topic",
]
print("\n=== Kafka Topics ===")
for topic in TOPICS:
    fqn = f"{KAFKA_SERVICE}.{topic}"
    r = requests.get(f"{url}/topics/name/{fqn}", headers=headers, timeout=10)
    if r.status_code == 200:
        print(f"  ~ Already exists: {fqn}")
    else:
        payload = {"name": topic, "service": KAFKA_SERVICE, "partitions": 1}
        r = requests.post(f"{url}/topics", headers=headers, json=payload, timeout=10)
        if r.status_code in (200, 201):
            print(f"  + Created topic {fqn}")
        else:
            print(f"  ! Failed ({r.status_code}): {fqn} - {r.text[:200]}")

# ── 3. Update NiFi Pipeline names ───────────────────────────────────────────
NIFI_SERVICE = "NiFi-Pipeline"
# Map lineage names -> display names
NIFI_PIPELINE_NAMES = {
    "nifi_crm_ingestion": "CRM Ingestion",
    "nifi_calls_ingestion": "Calls Ingestion",
    "nifi_sms_ingestion": "SMS Ingestion",
    "nifi_data_usage_ingestion": "Data Usage Ingestion",
    "nifi_network_ingestion": "Network Ingestion",
    "nifi_payment_ingestion": "Payment Ingestion",
    "nifi_recharge_ingestion": "Recharge Ingestion",
    "nifi_roaming_ingestion": "Roaming Ingestion",
    "nifi_support_ingestion": "Support Ingestion",
}

print("\n=== NiFi Pipelines ===")
for pipeline_name, display_name in NIFI_PIPELINE_NAMES.items():
    fqn = f"{NIFI_SERVICE}.{pipeline_name}"
    r = requests.get(f"{url}/pipelines/name/{fqn}", headers=headers, timeout=10)
    if r.status_code == 200:
        print(f"  ~ Already exists: {fqn}")
    else:
        payload = {
            "name": pipeline_name,
            "displayName": display_name,
            "description": display_name,
            "service": NIFI_SERVICE,
        }
        r = requests.post(f"{url}/pipelines", headers=headers, json=payload, timeout=10)
        if r.status_code in (200, 201):
            print(f"  + Created pipeline {fqn}")
        else:
            print(f"  ! Failed ({r.status_code}): {fqn} - {r.text[:200]}")

print("\nDone.")
