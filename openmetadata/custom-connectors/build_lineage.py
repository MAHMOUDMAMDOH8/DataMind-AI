"""
DataMind AI — OpenMetadata Lineage Builder
==========================================
Wires the full lineage graph:

  Kafka Topic → [NiFi Pipeline] → Bronze Table
                                  → [Spark: bronze_to_silver] → Silver Table
                                                               → [Spark: silver_to_gold] → Gold Table

Each edge is attributed to the corresponding pipeline entity so the
pipeline hop shows up as a node in the OM lineage graph.

Usage:
    set OM_JWT_TOKEN=<token>
    python openmetadata/custom-connectors/build_lineage.py

Prerequisites (run in order):
    1. OM ingestion pipelines for Kafka, Trino (Iceberg tables), and NiFi
    2. register_nifi_pipelines.py (creates NiFi pipeline entities)
    3. This script (creates lineage edges)
"""

import os
import sys

import requests

# ── Config ────────────────────────────────────────────────────────────────────
OM_HOST = os.getenv("OM_HOST", "http://openmetadata-server:8585/api/v1")
JWT_TOKEN = os.getenv("OM_JWT_TOKEN", "REPLACE_WITH_INGESTION_BOT_JWT")
HEADERS = {
    "Authorization": f"Bearer {JWT_TOKEN}",
    "Content-Type": "application/json",
}

# ── Service FQNs ──────────────────────────────────────────────────────────────
NIFI_SERVICE = "NiFi-Pipeline"
SPARK_SERVICE = "datamind-spark"
KAFKA_SERVICE = "Kafka"
TRINO_SERVICE = "trino"

# ── Phase A: Kafka Topic → [NiFi] → Bronze Table ────────────────────────────
# FQN format: <service>.<database>.<schema>.<table>
#   topics:  Kafka.<topic_name>
#   tables:  trino.iceberg.bronze.<table_name>
TOPIC_TO_BRONZE = [
    ("Kafka.customer_topic",       "trino.iceberg.bronze.crm",          f"{NIFI_SERVICE}.nifi_crm_ingestion"),
    ("Kafka.calls_topic",          "trino.iceberg.bronze.calls",        f"{NIFI_SERVICE}.nifi_calls_ingestion"),
    ("Kafka.sms_topic",            "trino.iceberg.bronze.sms",          f"{NIFI_SERVICE}.nifi_sms_ingestion"),
    ("Kafka.data_usage_topic",     "trino.iceberg.bronze.data_usage",   f"{NIFI_SERVICE}.nifi_data_usage_ingestion"),
    ("Kafka.network_metrics_topic","trino.iceberg.bronze.network",      f"{NIFI_SERVICE}.nifi_network_ingestion"),
    ("Kafka.payments_topic",       "trino.iceberg.bronze.payments",     f"{NIFI_SERVICE}.nifi_payment_ingestion"),
    ("Kafka.recharge_topic",       "trino.iceberg.bronze.recharge",     f"{NIFI_SERVICE}.nifi_recharge_ingestion"),
    ("Kafka.roaming_topic",        "trino.iceberg.bronze.roaming",      f"{NIFI_SERVICE}.nifi_roaming_ingestion"),
    ("Kafka.tickets_topic",        "trino.iceberg.bronze.support",      f"{NIFI_SERVICE}.nifi_support_ingestion"),
]

# ── Phase B: Bronze Table → [Spark: bronze_to_silver] → Silver Table ────────
BRONZE_TO_SILVER = [
    ("trino.iceberg.bronze.calls",        "trino.iceberg.silver.calls",              f"{SPARK_SERVICE}.bronze_to_silver"),
    ("trino.iceberg.bronze.sms",          "trino.iceberg.silver.sms",                f"{SPARK_SERVICE}.bronze_to_silver"),
    ("trino.iceberg.bronze.crm",          "trino.iceberg.silver.crm_registration",   f"{SPARK_SERVICE}.bronze_to_silver"),
    ("trino.iceberg.bronze.network",      "trino.iceberg.silver.network_metrics",    f"{SPARK_SERVICE}.bronze_to_silver"),
    ("trino.iceberg.bronze.payments",     "trino.iceberg.silver.payments",           f"{SPARK_SERVICE}.bronze_to_silver"),
    ("trino.iceberg.bronze.recharge",     "trino.iceberg.silver.recharges",          f"{SPARK_SERVICE}.bronze_to_silver"),
    ("trino.iceberg.bronze.roaming",      "trino.iceberg.silver.roaming",            f"{SPARK_SERVICE}.bronze_to_silver"),
    ("trino.iceberg.bronze.data_usage",   "trino.iceberg.silver.data_usage",         f"{SPARK_SERVICE}.bronze_to_silver"),
    ("trino.iceberg.bronze.support",      "trino.iceberg.silver.support_tickets",    f"{SPARK_SERVICE}.bronze_to_silver"),
]

# ── Phase C: Silver Table → [Spark: silver_to_gold] → Gold Table ────────────
SILVER_TO_GOLD = [
    ("trino.iceberg.silver.calls",          "trino.iceberg.gold.customer_360",          f"{SPARK_SERVICE}.customer_360"),
    ("trino.iceberg.silver.sms",            "trino.iceberg.gold.customer_360",          f"{SPARK_SERVICE}.customer_360"),
    ("trino.iceberg.silver.crm_registration","trino.iceberg.gold.customer_360",         f"{SPARK_SERVICE}.customer_360"),
    ("trino.iceberg.silver.payments",       "trino.iceberg.gold.customer_360",          f"{SPARK_SERVICE}.customer_360"),
    ("trino.iceberg.silver.recharges",      "trino.iceberg.gold.customer_360",          f"{SPARK_SERVICE}.customer_360"),
    ("trino.iceberg.silver.roaming",        "trino.iceberg.gold.customer_360",          f"{SPARK_SERVICE}.customer_360"),
    ("trino.iceberg.silver.data_usage",     "trino.iceberg.gold.customer_360",          f"{SPARK_SERVICE}.customer_360"),
    ("trino.iceberg.silver.support_tickets","trino.iceberg.gold.customer_360",          f"{SPARK_SERVICE}.customer_360"),
    ("trino.iceberg.silver.calls",          "trino.iceberg.gold.customer_usage_daily",  f"{SPARK_SERVICE}.customer_usage_daily"),
    ("trino.iceberg.silver.data_usage",     "trino.iceberg.gold.customer_usage_daily",  f"{SPARK_SERVICE}.customer_usage_daily"),
    ("trino.iceberg.silver.calls",          "trino.iceberg.gold.daily_revenue",         f"{SPARK_SERVICE}.daily_revenue"),
    ("trino.iceberg.silver.payments",       "trino.iceberg.gold.daily_revenue",         f"{SPARK_SERVICE}.daily_revenue"),
    ("trino.iceberg.silver.recharges",      "trino.iceberg.gold.daily_revenue",         f"{SPARK_SERVICE}.daily_revenue"),
    ("trino.iceberg.silver.roaming",        "trino.iceberg.gold.roaming_analytics",     f"{SPARK_SERVICE}.roaming_analytics"),
    ("trino.iceberg.silver.payments",       "trino.iceberg.gold.payment_analytics",     f"{SPARK_SERVICE}.payment_analytics"),
    ("trino.iceberg.silver.recharges",      "trino.iceberg.gold.recharge_analytics",    f"{SPARK_SERVICE}.recharge_analytics"),
    ("trino.iceberg.silver.network_metrics","trino.iceberg.gold.network_performance",   f"{SPARK_SERVICE}.network_performance"),
    ("trino.iceberg.silver.support_tickets","trino.iceberg.gold.support_analytics",     f"{SPARK_SERVICE}.support_analytics"),
    ("trino.iceberg.silver.crm_registration","trino.iceberg.gold.fraud_monitoring",     f"{SPARK_SERVICE}.fraud_monitoring"),
]

# ── Phase D: Dim tables via load_dims / silver_to_gold_dims ──────────────────
DIM_LINEAGE = [
    ("trino.iceberg.bronze.dim_user",      "trino.iceberg.silver.dim_user",       f"{SPARK_SERVICE}.load_dims"),
    ("trino.iceberg.bronze.dim_device",    "trino.iceberg.silver.dim_device",     f"{SPARK_SERVICE}.load_dims"),
    ("trino.iceberg.bronze.dim_agent",     "trino.iceberg.silver.dim_agent",      f"{SPARK_SERVICE}.load_dims"),
    ("trino.iceberg.bronze.dim_cell_site", "trino.iceberg.silver.dim_cell_site",  f"{SPARK_SERVICE}.load_dims"),
]


def resolve_entity(entity_type: str, fqn: str) -> dict | None:
    url = f"{OM_HOST}/{entity_type}/name/{fqn}"
    r = requests.get(url, headers=HEADERS, timeout=10)
    if r.status_code == 200:
        data = r.json()
        return {"id": data["id"], "type": entity_type.rstrip("s")}
    print(f"  - Could not resolve {entity_type} '{fqn}' (HTTP {r.status_code})")
    return None


def create_lineage_edge(
    from_type: str, from_fqn: str, to_type: str, to_fqn: str,
    pipeline_fqn: str = None,
) -> bool:
    from_entity = resolve_entity(from_type, from_fqn)
    to_entity = resolve_entity(to_type, to_fqn)

    if not from_entity or not to_entity:
        return False

    edge = {
        "fromEntity": {"id": from_entity["id"], "type": from_entity["type"]},
        "toEntity": {"id": to_entity["id"], "type": to_entity["type"]},
    }

    label = f"{from_fqn}  ->  {to_fqn}"
    if pipeline_fqn:
        pipeline = resolve_entity("pipelines", pipeline_fqn)
        if pipeline:
            edge["lineageDetails"] = {
                "pipeline": {"id": pipeline["id"], "type": "pipeline"}
            }
            label = f"{from_fqn}  ->  [{pipeline_fqn.split('.')[-1]}]  ->  {to_fqn}"
        else:
            print(f"    ~ pipeline '{pipeline_fqn}' not found")

    payload = {"edge": edge}
    r = requests.put(f"{OM_HOST}/lineage", headers=HEADERS, json=payload, timeout=10)
    if r.status_code in (200, 201):
        print(f"  + {label}")
        return True
    print(f"  ! FAILED ({r.status_code}): {label}\n    {r.text[:200]}")
    return False


def run_phase(name: str, edges: list, from_type: str, to_type: str) -> tuple:
    print(f"\n--- {name} ---")
    ok, fail = 0, 0
    for from_fqn, to_fqn, pipeline_fqn in edges:
        if create_lineage_edge(from_type, from_fqn, to_type, to_fqn, pipeline_fqn):
            ok += 1
        else:
            fail += 1
    return ok, fail


def main():
    if JWT_TOKEN == "REPLACE_WITH_INGESTION_BOT_JWT":
        print("ERROR: Set the OM_JWT_TOKEN environment variable first.")
        print("  export OM_JWT_TOKEN='<token from OM UI > Settings > Bots > ingestion-bot>'")
        sys.exit(1)

    print(f"{'='*60}")
    print(f"  DataMind AI — OM Lineage Builder")
    print(f"  Target: {OM_HOST}")
    print(f"{'='*60}")

    total_ok = total_fail = 0

    ok, fail = run_phase("Phase A: Kafka Topic -> [NiFi] -> Bronze Table", TOPIC_TO_BRONZE, "topics", "tables")
    total_ok += ok; total_fail += fail

    ok, fail = run_phase("Phase B: Bronze Table -> [Spark] -> Silver Table", BRONZE_TO_SILVER, "tables", "tables")
    total_ok += ok; total_fail += fail

    ok, fail = run_phase("Phase C: Silver Table -> [Spark] -> Gold Table", SILVER_TO_GOLD, "tables", "tables")
    total_ok += ok; total_fail += fail

    ok, fail = run_phase("Phase D: Bronze Dim -> [Spark] -> Silver Dim", DIM_LINEAGE, "tables", "tables")
    total_ok += ok; total_fail += fail

    print(f"\n{'='*60}")
    print(f"  Done: {total_ok} edges created, {total_fail} failed.")
    print(f"{'='*60}")
    print()
    print("Notes:")
    print("  - Failures for non-existent entities are expected until the")
    print("    corresponding ingestion pipelines have run at least once.")
    print("  - Re-run this script after each new ingestion pipeline completes.")


if __name__ == "__main__":
    main()
