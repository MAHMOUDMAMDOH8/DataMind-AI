"""
DataMind AI — OpenMetadata PII Auto-Tagger
==========================================
Tags known PII columns across Bronze, Silver, and Gold tables.
Applies the built-in OM classification tag 'PII.Sensitive'.

PII columns identified per arch/3-source-systems.md and silver/gold models:
  - phone_number, caller_phone_number, receiver_phone_number,
    sender_phone_number, national_id, address, message_body,
    customer_id (as a sensitive identifier), invoice_number

Usage (run from project root on the target machine):
    python governance/scripts/tag_pii_columns.py

Prerequisites:
    pip install requests
    Set OM_JWT_TOKEN env var
    Run AFTER ingestion phases 3–4.
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

PII_TAG_FQN = "PII.Sensitive"

# ── PII column map ────────────────────────────────────────────────────────────
# Format: (table_fqn, [column_names])
PII_COLUMNS = [
    # Bronze
    (
        "datamind-iceberg-bronze.bronze.default.customers",
        ["phone_number", "national_id", "address", "customer_id"],
    ),
    (
        "datamind-iceberg-bronze.bronze.default.calls",
        ["caller_phone_number", "receiver_phone_number", "customer_id"],
    ),
    (
        "datamind-iceberg-bronze.bronze.default.sms",
        ["sender_phone_number", "receiver_phone_number", "customer_id", "message_body"],
    ),
    (
        "datamind-iceberg-bronze.bronze.default.payments",
        ["customer_id", "phone_number", "invoice_number"],
    ),
    # Silver
    (
        "datamind-iceberg-silver.silver.default.crm_customer_registrations",
        ["phone_number", "customer_id"],
    ),
    (
        "datamind-iceberg-silver.silver.default.crm_profile_updates",
        ["phone_number", "customer_id"],
    ),
    (
        "datamind-iceberg-silver.silver.default.billing_calls",
        ["caller_phone_number", "receiver_phone_number", "customer_id"],
    ),
    (
        "datamind-iceberg-silver.silver.default.billing_sms",
        ["sender_phone_number", "receiver_phone_number", "customer_id", "message_body"],
    ),
    (
        "datamind-iceberg-silver.silver.default.payments",
        ["customer_id", "phone_number", "invoice_number"],
    ),
    (
        "datamind-iceberg-silver.silver.default.recharges",
        ["customer_id", "phone_number"],
    ),
    (
        "datamind-iceberg-silver.silver.default.roaming_events",
        ["customer_id", "phone_number"],
    ),
    (
        "datamind-iceberg-silver.silver.default.support_tickets",
        ["customer_id", "phone_number"],
    ),
    (
        "datamind-iceberg-silver.silver.default.complaints",
        ["customer_id", "phone_number"],
    ),
    (
        "datamind-iceberg-silver.silver.default.ticket_resolutions",
        ["customer_id", "phone_number"],
    ),
    # Gold
    (
        "datamind-iceberg-gold.gold.default.customer_360",
        ["customer_id", "phone_number"],
    ),
]


def get_table(fqn: str) -> dict | None:
    r = requests.get(f"{OM_HOST}/tables/name/{fqn}", headers=HEADERS, timeout=10)
    if r.status_code == 200:
        return r.json()
    print(f"  ✗ Table not found: {fqn}")
    return None


def patch_column_tag(table_id: str, column_name: str, tag_fqn: str) -> bool:
    """Add a tag to a specific column via JSON Patch."""
    # First GET the table to find column index
    r = requests.get(f"{OM_HOST}/tables/{table_id}", headers=HEADERS, timeout=10)
    if r.status_code != 200:
        return False

    table = r.json()
    columns = table.get("columns", [])
    col_idx = next(
        (i for i, c in enumerate(columns) if c["name"].lower() == column_name.lower()),
        None,
    )
    if col_idx is None:
        print(f"    ~ Column '{column_name}' not found in schema (may differ from arch name)")
        return False

    existing_tags = columns[col_idx].get("tags", [])
    if any(t.get("tagFQN") == tag_fqn for t in existing_tags):
        print(f"    ~ Already tagged: {column_name}")
        return True

    existing_tags.append({"tagFQN": tag_fqn, "source": "Classification", "labelType": "Manual"})

    patch = [
        {
            "op": "replace",
            "path": f"/columns/{col_idx}/tags",
            "value": existing_tags,
        }
    ]

    r2 = requests.patch(
        f"{OM_HOST}/tables/{table_id}",
        headers={**HEADERS, "Content-Type": "application/json-patch+json"},
        json=patch,
        timeout=10,
    )
    if r2.status_code in (200, 201):
        print(f"    ✓ Tagged: {column_name} [{tag_fqn}]")
        return True
    print(f"    ✗ Failed: {column_name} — HTTP {r2.status_code}: {r2.text[:120]}")
    return False


def main():
    if JWT_TOKEN == "REPLACE_WITH_INGESTION_BOT_JWT":
        print("ERROR: Set the OM_JWT_TOKEN environment variable first.")
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"  DataMind AI — PII Column Tagger")
    print(f"  Target: {OM_HOST}")
    print(f"  Tag:    {PII_TAG_FQN}")
    print(f"{'='*60}\n")

    total_ok = 0
    total_fail = 0

    for table_fqn, col_names in PII_COLUMNS:
        print(f"Table: {table_fqn.split('.')[-1]}")
        table = get_table(table_fqn)
        if not table:
            total_fail += len(col_names)
            continue

        table_id = table["id"]
        for col in col_names:
            ok = patch_column_tag(table_id, col, PII_TAG_FQN)
            if ok:
                total_ok += 1
            else:
                total_fail += 1
        print()

    print(f"Summary: {total_ok} columns tagged, {total_fail} failed.")


if __name__ == "__main__":
    main()
