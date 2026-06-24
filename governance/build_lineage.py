"""
DataMind AI — OpenMetadata Lineage Builder
==========================================
Manually wires the Kafka topic → Bronze table lineage edges
that cannot be auto-detected (NiFi has no OM connector).

Each edge attaches the corresponding NiFi pipeline entity via
`lineageDetails.pipeline`, so the NiFi hop shows up as a node in the
OM lineage graph instead of a same-meaning direct topic→table edge:
  Kafka Topic → [NiFi pipeline] → Bronze Table → [Spark/Airflow] → Silver → Gold

Usage (run from project root on the target machine):
    python governance/scripts/build_lineage.py

Prerequisites:
    pip install requests
    Set OM_JWT_TOKEN env var (copy from OM UI → Settings → Bots → ingestion-bot)
    Run AFTER phases 3 (iceberg ingestion) and 4 (kafka ingestion), AND after
    governance/scripts/register_nifi_pipelines.py (creates the NiFi pipeline
    entities referenced below).
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
# Format: (kafka_topic_fqn, bronze_table_fqn, nifi_pipeline_fqn)
# nifi_pipeline_fqn must already exist — run register_nifi_pipelines.py first.
NIFI_SERVICE = "datamind-nifi"
TOPIC_TO_BRONZE = [
    (
        "datamind-kafka.customer_topic",
        "datamind-iceberg-bronze.bronze.default.customers",
        f"{NIFI_SERVICE}.nifi_crm_ingestion",
    ),
    (
        "datamind-kafka.calls_topic",
        "datamind-iceberg-bronze.bronze.default.calls",
        f"{NIFI_SERVICE}.nifi_calls_ingestion",
    ),
    (
        "datamind-kafka.sms_topic",
        "datamind-iceberg-bronze.bronze.default.sms",
        f"{NIFI_SERVICE}.nifi_sms_ingestion",
    ),
    (
        "datamind-kafka.data_usage_topic",
        "datamind-iceberg-bronze.bronze.default.data_usage",
        f"{NIFI_SERVICE}.nifi_data_usage_ingestion",
    ),
    (
        "datamind-kafka.network_metrics_topic",
        "datamind-iceberg-bronze.bronze.default.network_metrics",
        f"{NIFI_SERVICE}.nifi_network_ingestion",
    ),
    (
        "datamind-kafka.payments_topic",
        "datamind-iceberg-bronze.bronze.default.payments",
        f"{NIFI_SERVICE}.nifi_payment_ingestion",
    ),
    (
        "datamind-kafka.recharge_topic",
        "datamind-iceberg-bronze.bronze.default.recharge",
        f"{NIFI_SERVICE}.nifi_recharge_ingestion",
    ),
    (
        "datamind-kafka.roaming_topic",
        "datamind-iceberg-bronze.bronze.default.roaming",
        f"{NIFI_SERVICE}.nifi_roaming_ingestion",
    ),
    (
        "datamind-kafka.tickets_topic",
        "datamind-iceberg-bronze.bronze.default.tickets",
        f"{NIFI_SERVICE}.nifi_support_ingestion",
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
    from_type: str, from_fqn: str, to_type: str, to_fqn: str, pipeline_fqn: str = None
) -> bool:
    """POST a lineage edge between two entities, optionally attributed to a pipeline."""
    from_entity = resolve_entity(from_type, from_fqn)
    to_entity = resolve_entity(to_type, to_fqn)

    if not from_entity or not to_entity:
        return False

    edge = {
        "fromEntity": {"id": from_entity["id"], "type": from_entity["type"]},
        "toEntity": {"id": to_entity["id"], "type": to_entity["type"]},
    }

    label = f"{from_fqn} → {to_fqn}"
    if pipeline_fqn:
        pipeline_entity = resolve_entity("pipelines", pipeline_fqn)
        if pipeline_entity:
            edge["lineageDetails"] = {
                "pipeline": {"id": pipeline_entity["id"], "type": "pipeline"}
            }
            label = f"{from_fqn} → [{pipeline_fqn.split('.')[-1]}] → {to_fqn}"
        else:
            print(f"    ~ NiFi pipeline '{pipeline_fqn}' not found — edge created without it")
            print(f"      → Run register_nifi_pipelines.py first.")

    payload = {"edge": edge}
    r = requests.put(f"{OM_HOST}/lineage", headers=HEADERS, json=payload, timeout=10)
    if r.status_code in (200, 201):
        print(f"  ✓ {label}")
        return True
    print(f"  ✗ Failed ({r.status_code}): {label}\n    {r.text[:200]}")
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

    # ── Phase A: Kafka Topic → [NiFi Pipeline] → Bronze Table ────────────────
    print("Phase A: Kafka Topics → Bronze Tables (via NiFi)")
    print("-" * 50)
    for topic_fqn, bronze_fqn, pipeline_fqn in TOPIC_TO_BRONZE:
        ok = create_lineage_edge(
            "topics", topic_fqn, "tables", bronze_fqn, pipeline_fqn=pipeline_fqn
        )
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
