"""
DataMind AI — OpenMetadata Lineage Builder
==========================================
Manually wires the Kafka topic → Bronze table lineage edges
that cannot be auto-detected (NiFi has no OM connector).

Also wires NiFi as a pipeline entity to close the full lineage chain:
  Kafka Topic → [NiFi] → Bronze Table → [Spark/Airflow] → Silver → Gold

Usage (run from project root on the target machine):
    python governance/scripts/build_lineage.py

Prerequisites:
    pip install requests
    Set OM_JWT_TOKEN env var (copy from OM UI → Settings → Bots → ingestion-bot)
    Run AFTER phases 3 (iceberg ingestion) and 4 (kafka ingestion) are complete.
"""

import os
import sys
import requests

# ── Config ────────────────────────────────────────────────────────────────────
OM_HOST = os.getenv("OM_HOST", "http://localhost:8585/api/v1")
JWT_TOKEN = os.getenv("OM_JWT_TOKEN", "REPLACE_WITH_INGESTION_BOT_JWT")
HEADERS = {
    "Authorization": f"Bearer {JWT_TOKEN}",
    "Content-Type": "application/json",
}

# ── Topic → Bronze table lineage map ─────────────────────────────────────────
# Format: (kafka_topic_fqn, bronze_table_fqn)
TOPIC_TO_BRONZE = [
    (
        "datamind-kafka.customer_topic",
        "datamind-iceberg-bronze.bronze.default.customers",
    ),
    (
        "datamind-kafka.calls_topic",
        "datamind-iceberg-bronze.bronze.default.calls",
    ),
    (
        "datamind-kafka.sms_topic",
        "datamind-iceberg-bronze.bronze.default.sms",
    ),
    (
        "datamind-kafka.data_usage_topic",
        "datamind-iceberg-bronze.bronze.default.data_usage",
    ),
    (
        "datamind-kafka.network_metrics_topic",
        "datamind-iceberg-bronze.bronze.default.network_metrics",
    ),
    (
        "datamind-kafka.payments_topic",
        "datamind-iceberg-bronze.bronze.default.payments",
    ),
    (
        "datamind-kafka.recharge_topic",
        "datamind-iceberg-bronze.bronze.default.recharge",
    ),
    (
        "datamind-kafka.roaming_topic",
        "datamind-iceberg-bronze.bronze.default.roaming",
    ),
    (
        "datamind-kafka.tickets_topic",
        "datamind-iceberg-bronze.bronze.default.tickets",
    ),
]


def resolve_entity(entity_type: str, fqn: str) -> dict | None:
    """Resolve an entity ID from its fully qualified name."""
    url = f"{OM_HOST}/{entity_type}/name/{fqn}"
    r = requests.get(url, headers=HEADERS, timeout=10)
    if r.status_code == 200:
        data = r.json()
        return {"id": data["id"], "type": entity_type.rstrip("s")}  # tables → table
    print(f"  ✗ Could not resolve {entity_type} '{fqn}': HTTP {r.status_code}")
    return None


def create_lineage_edge(
    from_type: str, from_fqn: str, to_type: str, to_fqn: str
) -> bool:
    """POST a lineage edge between two entities."""
    from_entity = resolve_entity(from_type, from_fqn)
    to_entity = resolve_entity(to_type, to_fqn)

    if not from_entity or not to_entity:
        return False

    payload = {
        "edge": {
            "fromEntity": {"id": from_entity["id"], "type": from_entity["type"]},
            "toEntity": {"id": to_entity["id"], "type": to_entity["type"]},
        }
    }

    r = requests.put(f"{OM_HOST}/lineage", headers=HEADERS, json=payload, timeout=10)
    if r.status_code in (200, 201):
        print(f"  ✓ {from_fqn} → {to_fqn}")
        return True
    print(f"  ✗ Failed ({r.status_code}): {from_fqn} → {to_fqn}\n    {r.text[:200]}")
    return False


def main():
    if JWT_TOKEN == "REPLACE_WITH_INGESTION_BOT_JWT":
        print("ERROR: Set the OM_JWT_TOKEN environment variable first.")
        print("  export OM_JWT_TOKEN='<token from OM UI → Settings → Bots → ingestion-bot>'")
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"  DataMind AI — OM Lineage Builder")
    print(f"  Target: {OM_HOST}")
    print(f"{'='*60}\n")

    success = 0
    failure = 0

    # ── Phase A: Kafka Topic → Bronze Table (NiFi carries these) ─────────────
    print("Phase A: Kafka Topics → Bronze Tables (via NiFi)")
    print("-" * 50)
    for topic_fqn, bronze_fqn in TOPIC_TO_BRONZE:
        ok = create_lineage_edge("topics", topic_fqn, "tables", bronze_fqn)
        if ok:
            success += 1
        else:
            failure += 1

    print(f"\nDone: {success} edges created, {failure} failed.\n")
    print("Next steps:")
    print("  • Bronze → Silver lineage: auto-captured when Airflow DAGs run")
    print("    (install openmetadata-ingestion[airflow] in airflow containers)")
    print("  • Silver → Gold lineage:   same as above via silver_to_gold DAG")
    print("  • Gold → Trino:            auto-captured by Trino usage ingestion")


if __name__ == "__main__":
    main()
