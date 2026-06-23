"""
DataMind AI — OpenMetadata Bulk Owner Assignment
=================================================
Assigns team ownership to all registered assets in bulk via the OM REST API.

Ownership map:
  data-engineering team  →  All Bronze + Silver tables, Airflow pipelines
  analytics team         →  All Gold tables
  data-governance team   →  Kafka topics (data stewardship)

Usage (run from project root on the target machine):
    python governance/scripts/bulk_assign_owners.py

Prerequisites:
    pip install requests
    Set OM_JWT_TOKEN env var (copy from OM UI → Settings → Bots → ingestion-bot)
    Run AFTER all ingestion phases (3–7) are complete so all assets exist in OM.
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

# ── Ownership assignments ─────────────────────────────────────────────────────
# Team names must match exactly what you create in OM UI (Phase 2)
TEAM_TABLES = {
    "data-engineering": [
        # ── Bronze tables ──────────────────────────────────────────────────
        "datamind-iceberg-bronze.bronze.default.customers",
        "datamind-iceberg-bronze.bronze.default.calls",
        "datamind-iceberg-bronze.bronze.default.sms",
        "datamind-iceberg-bronze.bronze.default.data_usage",
        "datamind-iceberg-bronze.bronze.default.network_metrics",
        "datamind-iceberg-bronze.bronze.default.payments",
        "datamind-iceberg-bronze.bronze.default.recharge",
        "datamind-iceberg-bronze.bronze.default.roaming",
        "datamind-iceberg-bronze.bronze.default.tickets",
        # ── Silver tables ──────────────────────────────────────────────────
        "datamind-iceberg-silver.silver.default.crm_customer_registrations",
        "datamind-iceberg-silver.silver.default.crm_profile_updates",
        "datamind-iceberg-silver.silver.default.billing_calls",
        "datamind-iceberg-silver.silver.default.billing_sms",
        "datamind-iceberg-silver.silver.default.network_data_sessions",
        "datamind-iceberg-silver.silver.default.network_metrics",
        "datamind-iceberg-silver.silver.default.qos_reports",
        "datamind-iceberg-silver.silver.default.payments",
        "datamind-iceberg-silver.silver.default.recharges",
        "datamind-iceberg-silver.silver.default.roaming_events",
        "datamind-iceberg-silver.silver.default.support_tickets",
        "datamind-iceberg-silver.silver.default.complaints",
        "datamind-iceberg-silver.silver.default.ticket_resolutions",
    ],
    "analytics": [
        # ── Gold tables ────────────────────────────────────────────────────
        "datamind-iceberg-gold.gold.default.customer_360",
        "datamind-iceberg-gold.gold.default.daily_revenue",
        "datamind-iceberg-gold.gold.default.customer_usage_daily",
        "datamind-iceberg-gold.gold.default.payment_analytics",
        "datamind-iceberg-gold.gold.default.recharge_analytics",
        "datamind-iceberg-gold.gold.default.roaming_analytics",
        "datamind-iceberg-gold.gold.default.network_performance",
        "datamind-iceberg-gold.gold.default.support_analytics",
        "datamind-iceberg-gold.gold.default.fraud_monitoring",
    ],
    "data-governance": [
        # ── Kafka topics ────────────────────────────────────────────────────
        "datamind-kafka.customer_topic",
        "datamind-kafka.calls_topic",
        "datamind-kafka.sms_topic",
        "datamind-kafka.data_usage_topic",
        "datamind-kafka.network_metrics_topic",
        "datamind-kafka.payments_topic",
        "datamind-kafka.recharge_topic",
        "datamind-kafka.roaming_topic",
        "datamind-kafka.tickets_topic",
    ],
}


def get_team_id(team_name: str) -> str | None:
    """Resolve a team ID from its name."""
    r = requests.get(
        f"{OM_HOST}/teams/name/{team_name}", headers=HEADERS, timeout=10
    )
    if r.status_code == 200:
        return r.json()["id"]
    print(f"  ✗ Team '{team_name}' not found (HTTP {r.status_code})")
    print(f"    → Create it in OM UI → Settings → Teams first.")
    return None


def get_table_id(fqn: str) -> str | None:
    """Resolve a table entity ID from its FQN."""
    r = requests.get(f"{OM_HOST}/tables/name/{fqn}", headers=HEADERS, timeout=10)
    if r.status_code == 200:
        return r.json()["id"]
    return None


def get_topic_id(fqn: str) -> str | None:
    """Resolve a topic entity ID from its FQN."""
    r = requests.get(f"{OM_HOST}/topics/name/{fqn}", headers=HEADERS, timeout=10)
    if r.status_code == 200:
        return r.json()["id"]
    return None


def assign_owner(entity_type: str, entity_id: str, team_id: str, fqn: str) -> bool:
    """PATCH an entity to assign a team as owner."""
    payload = [
        {
            "op": "add",
            "path": "/owner",
            "value": {"id": team_id, "type": "team"},
        }
    ]
    r = requests.patch(
        f"{OM_HOST}/{entity_type}/{entity_id}",
        headers={**HEADERS, "Content-Type": "application/json-patch+json"},
        json=payload,
        timeout=10,
    )
    if r.status_code in (200, 201):
        print(f"    ✓ {fqn}")
        return True
    print(f"    ✗ {fqn} — HTTP {r.status_code}: {r.text[:150]}")
    return False


def main():
    if JWT_TOKEN == "REPLACE_WITH_INGESTION_BOT_JWT":
        print("ERROR: Set the OM_JWT_TOKEN environment variable first.")
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"  DataMind AI — OM Bulk Owner Assignment")
    print(f"  Target: {OM_HOST}")
    print(f"{'='*60}\n")

    total_ok = 0
    total_fail = 0

    for team_name, fqns in TEAM_TABLES.items():
        print(f"Team: {team_name}")
        print("-" * 50)

        team_id = get_team_id(team_name)
        if not team_id:
            total_fail += len(fqns)
            continue

        # Determine entity type per FQN
        for fqn in fqns:
            if "kafka" in fqn:
                entity_id = get_topic_id(fqn)
                entity_type = "topics"
            else:
                entity_id = get_table_id(fqn)
                entity_type = "tables"

            if not entity_id:
                print(f"    ✗ Not found in OM: {fqn}")
                print(f"      → Run ingestion workflows first (phases 3–4).")
                total_fail += 1
                continue

            ok = assign_owner(entity_type, entity_id, team_id, fqn)
            if ok:
                total_ok += 1
            else:
                total_fail += 1

        print()

    print(f"Summary: {total_ok} assigned, {total_fail} failed.\n")


if __name__ == "__main__":
    main()
