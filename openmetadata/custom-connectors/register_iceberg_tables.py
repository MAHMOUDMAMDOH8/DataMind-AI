"""
Register Nessie/Iceberg tables (silver + gold) in OpenMetadata via the OM API.
Gold tables may already exist from Trino ingestion; silver tables likely don't.

Usage:
    set OM_JWT_TOKEN=<token>
    python openmetadata/custom-connectors/register_iceberg_tables.py
"""
import os, sys, json
import requests

OM_HOST = os.getenv("OM_HOST", "http://openmetadata-server:8585/api/v1")
JWT_TOKEN = os.getenv("OM_JWT_TOKEN", "")
HEADERS = {"Authorization": f"Bearer {JWT_TOKEN}", "Content-Type": "application/json"}

TRINO_SERVICE = "trino"

# Silver tables in Nessie catalog under local.silver.*
SILVER_TABLES = [
    "calls", "sms", "crm_registration", "crm_profile_update",
    "network_metrics", "network_qos_reports",
    "payments", "recharges", "roaming", "data_usage", "support_tickets",
    "dim_user", "dim_device", "dim_agent", "dim_cell_site",
]

# Gold tables in Nessie catalog under local.gold.*
GOLD_TABLES = [
    "customer_360", "customer_usage_daily", "daily_revenue",
    "roaming_analytics", "payment_analytics", "recharge_analytics",
    "network_performance", "support_analytics", "fraud_monitoring",
    "dim_user", "dim_device", "dim_agent", "dim_cell_site",
    "dim_date", "dim_time",
]

# Bronze tables don't exist as Iceberg tables, but we need OM entities for lineage
BRONZE_TABLES = [
    "crm", "calls", "sms", "data_usage", "network",
    "payments", "recharge", "roaming", "support",
    "dim_user", "dim_device", "dim_agent", "dim_cell_site",
]


def resolve_entity(entity_type, fqn):
    url = f"{OM_HOST}/{entity_type}/name/{fqn}"
    r = requests.get(url, headers=HEADERS, timeout=10)
    if r.status_code == 200:
        return r.json()
    return None


def create_table(name, schema_fqn, columns=None):
    """Create a table entity in OM under a given database schema."""
    payload = {
        "name": name,
        "databaseSchema": schema_fqn,
        "tableType": "Regular",
        "columns": columns or [{"name": "col_placeholder", "dataType": "STRING"}],
    }
    r = requests.post(f"{OM_HOST}/tables", headers=HEADERS, json=payload, timeout=10)
    if r.status_code in (200, 201):
        print(f"  + Created table {schema_fqn}.{name}")
        return r.json()
    elif r.status_code == 409:
        print(f"  ~ Already exists: {schema_fqn}.{name}")
        return resolve_entity("tables", f"{schema_fqn}.{name}")
    else:
        print(f"  ! Failed ({r.status_code}): {schema_fqn}.{name} - {r.text[:200]}")
        return None


def find_or_create_database(service_name, database_name):
    """Find or create a database entity in OM under the given service."""
    fqn = f"{service_name}.{database_name}"
    existing = resolve_entity("databases", fqn)
    if existing:
        return existing["id"], fqn
    # Try to find the service
    service = resolve_entity("services/databaseServices", service_name)
    if not service:
        print(f"  ! Service '{service_name}' not found in OM")
        return None, None
    payload = {
        "name": database_name,
        "service": service_name,
    }
    r = requests.post(f"{OM_HOST}/databases", headers=HEADERS, json=payload, timeout=10)
    if r.status_code in (200, 201):
        data = r.json()
        print(f"  + Created database {fqn}")
        return data["id"], fqn
    print(f"  ! Failed to create database {fqn}: {r.status_code} {r.text[:200]}")
    return None, None


def find_or_create_database_schema(schema_name, database_fqn):
    """Find or create a database schema entity in OM."""
    fqn = f"{database_fqn}.{schema_name}"
    existing = resolve_entity("databaseSchemas", fqn)
    if existing:
        return existing["id"], fqn
    db = resolve_entity("databases", database_fqn)
    if not db:
        print(f"  ! Database '{database_fqn}' not found")
        return None, None
    payload = {
        "name": schema_name,
        "database": database_fqn,
    }
    r = requests.post(f"{OM_HOST}/databaseSchemas", headers=HEADERS, json=payload, timeout=10)
    if r.status_code in (200, 201):
        data = r.json()
        print(f"  + Created schema {fqn}")
        return data["id"], fqn
    print(f"  ! Failed to create schema {fqn}: {r.status_code} {r.text[:200]}")
    return None, None


def register_tables(schema_fqn, tables):
    """Register a list of tables under a schema."""
    ok, fail = 0, 0
    for table in tables:
        tbl = create_table(table, schema_fqn)
        if tbl:
            ok += 1
        else:
            fail += 1
    return ok, fail


def main():
    if not JWT_TOKEN:
        print("ERROR: Set OM_JWT_TOKEN environment variable")
        sys.exit(1)

    print("=" * 60)
    print("  Registering Iceberg tables in OpenMetadata")
    print("=" * 60)

    # Ensure trino service has iceberg database with bronze/silver/gold schemas
    for schema_name, tables in [("bronze", BRONZE_TABLES), ("silver", SILVER_TABLES), ("gold", GOLD_TABLES)]:
        print(f"\n--- Schema: {TRINO_SERVICE}.iceberg.{schema_name} ---")
        db_id, db_fqn = find_or_create_database(TRINO_SERVICE, "iceberg")
        if not db_id:
            continue
        schema_id, schema_fqn = find_or_create_database_schema(schema_name, db_fqn)
        if not schema_id:
            continue
        ok, fail = register_tables(schema_fqn, tables)
        print(f"  Result: {ok} created, {fail} failed")


if __name__ == "__main__":
    main()
