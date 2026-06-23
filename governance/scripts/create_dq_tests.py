"""
DataMind AI — OpenMetadata DQ Test Suite Bootstrap
===================================================
Creates data quality test cases for Silver and Gold tables via the OM REST API.

Test suites:
  - gold-customer-tests:  customer_360 integrity checks
  - gold-revenue-tests:   daily_revenue positivity checks
  - gold-payment-tests:   payment_analytics success rate sanity
  - silver-billing-tests: billing_calls duration + amount non-negative
  - silver-roaming-tests: roaming_events charges non-negative
  - silver-network-tests: QoS scores in valid range

Usage (run from project root on the target machine):
    python governance/scripts/create_dq_tests.py

Prerequisites:
    pip install requests
    Set OM_JWT_TOKEN env var
    Run AFTER profiler workflows (phases 13) since DQ tests use the profiler.
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

# ── Test definitions ──────────────────────────────────────────────────────────
# Each entry is a test case to create.
# entityLink format: <#E::table::<fqn>::columns::<col>>  for column-level
#                    <#E::table::<fqn>>                   for table-level
TEST_CASES = [
    # ── gold.customer_360 ────────────────────────────────────────────────────
    {
        "name": "customer_360__customer_id_not_null",
        "displayName": "customer_id must not be null",
        "description": "Every Customer 360 row must have a valid customer_id.",
        "entityLink": "<#E::table::datamind-iceberg-gold.gold.default.customer_360::columns::customer_id>",
        "testDefinition": "columnValuesNotInSet",
        "parameterValues": [],
    },
    {
        "name": "customer_360__churn_score_range",
        "displayName": "churn_score must be between 0 and 1",
        "description": "ML churn score must be a valid probability (0.0 – 1.0).",
        "entityLink": "<#E::table::datamind-iceberg-gold.gold.default.customer_360::columns::churn_score>",
        "testDefinition": "columnValuesToBeBetween",
        "parameterValues": [
            {"name": "minValue", "value": "0"},
            {"name": "maxValue", "value": "1"},
        ],
    },
    {
        "name": "customer_360__total_revenue_not_negative",
        "displayName": "total_revenue must be >= 0",
        "description": "A customer's total revenue cannot be negative.",
        "entityLink": "<#E::table::datamind-iceberg-gold.gold.default.customer_360::columns::total_revenue>",
        "testDefinition": "columnValuesToBeBetween",
        "parameterValues": [
            {"name": "minValue", "value": "0"},
        ],
    },
    # ── gold.daily_revenue ───────────────────────────────────────────────────
    {
        "name": "daily_revenue__total_revenue_positive",
        "displayName": "daily total_revenue must be > 0",
        "description": "Each revenue date must have a positive total revenue.",
        "entityLink": "<#E::table::datamind-iceberg-gold.gold.default.daily_revenue::columns::total_revenue>",
        "testDefinition": "columnValuesToBeBetween",
        "parameterValues": [
            {"name": "minValue", "value": "0"},
        ],
    },
    {
        "name": "daily_revenue__failed_payments_not_negative",
        "displayName": "failed_payments must be >= 0",
        "description": "Failed payment count cannot be negative.",
        "entityLink": "<#E::table::datamind-iceberg-gold.gold.default.daily_revenue::columns::failed_payments>",
        "testDefinition": "columnValuesToBeBetween",
        "parameterValues": [
            {"name": "minValue", "value": "0"},
        ],
    },
    # ── gold.payment_analytics ───────────────────────────────────────────────
    {
        "name": "payment_analytics__success_rate_range",
        "displayName": "success_rate must be between 0 and 1",
        "description": "Payment success rate is a ratio and must be 0.0 – 1.0.",
        "entityLink": "<#E::table::datamind-iceberg-gold.gold.default.payment_analytics::columns::success_rate>",
        "testDefinition": "columnValuesToBeBetween",
        "parameterValues": [
            {"name": "minValue", "value": "0"},
            {"name": "maxValue", "value": "1"},
        ],
    },
    {
        "name": "payment_analytics__total_amount_not_negative",
        "displayName": "total_amount must be >= 0",
        "description": "Aggregate payment amount per day cannot be negative.",
        "entityLink": "<#E::table::datamind-iceberg-gold.gold.default.payment_analytics::columns::total_amount>",
        "testDefinition": "columnValuesToBeBetween",
        "parameterValues": [
            {"name": "minValue", "value": "0"},
        ],
    },
    # ── silver.billing_calls ─────────────────────────────────────────────────
    {
        "name": "billing_calls__duration_not_negative",
        "displayName": "call_duration_seconds must be >= 0",
        "description": "A call cannot have a negative duration.",
        "entityLink": "<#E::table::datamind-iceberg-silver.silver.default.billing_calls::columns::call_duration_seconds>",
        "testDefinition": "columnValuesToBeBetween",
        "parameterValues": [
            {"name": "minValue", "value": "0"},
        ],
    },
    {
        "name": "billing_calls__amount_not_negative",
        "displayName": "call amount must be >= 0",
        "description": "Billing amount for a call record cannot be negative.",
        "entityLink": "<#E::table::datamind-iceberg-silver.silver.default.billing_calls::columns::amount>",
        "testDefinition": "columnValuesToBeBetween",
        "parameterValues": [
            {"name": "minValue", "value": "0"},
        ],
    },
    {
        "name": "billing_calls__call_sid_not_null",
        "displayName": "call_sid must not be null",
        "description": "Business key call_sid must always be present.",
        "entityLink": "<#E::table::datamind-iceberg-silver.silver.default.billing_calls::columns::call_sid>",
        "testDefinition": "columnValuesNotInSet",
        "parameterValues": [],
    },
    # ── silver.roaming_events ────────────────────────────────────────────────
    {
        "name": "roaming__charges_not_negative",
        "displayName": "roaming_charges must be >= 0",
        "description": "Roaming charges cannot be negative.",
        "entityLink": "<#E::table::datamind-iceberg-silver.silver.default.roaming_events::columns::roaming_charges>",
        "testDefinition": "columnValuesToBeBetween",
        "parameterValues": [
            {"name": "minValue", "value": "0"},
        ],
    },
    {
        "name": "roaming__duration_not_negative",
        "displayName": "roaming duration_seconds must be >= 0",
        "description": "A roaming session duration cannot be negative.",
        "entityLink": "<#E::table::datamind-iceberg-silver.silver.default.roaming_events::columns::duration_seconds>",
        "testDefinition": "columnValuesToBeBetween",
        "parameterValues": [
            {"name": "minValue", "value": "0"},
        ],
    },
    # ── silver.qos_reports ───────────────────────────────────────────────────
    {
        "name": "qos__mos_score_range",
        "displayName": "mos_score_avg must be between 1 and 5",
        "description": "MOS (Mean Opinion Score) is defined in the 1.0–5.0 range.",
        "entityLink": "<#E::table::datamind-iceberg-silver.silver.default.qos_reports::columns::mos_score_avg>",
        "testDefinition": "columnValuesToBeBetween",
        "parameterValues": [
            {"name": "minValue", "value": "1"},
            {"name": "maxValue", "value": "5"},
        ],
    },
    {
        "name": "qos__packet_loss_range",
        "displayName": "packet_loss_pct_avg must be 0–100",
        "description": "Packet loss percentage is a valid value between 0 and 100.",
        "entityLink": "<#E::table::datamind-iceberg-silver.silver.default.qos_reports::columns::packet_loss_pct_avg>",
        "testDefinition": "columnValuesToBeBetween",
        "parameterValues": [
            {"name": "minValue", "value": "0"},
            {"name": "maxValue", "value": "100"},
        ],
    },
    # ── gold.network_performance ─────────────────────────────────────────────
    {
        "name": "network__health_score_range",
        "displayName": "network_health_score must be in valid range",
        "description": "Composite network health score derived from MOS/jitter/latency/loss.",
        "entityLink": "<#E::table::datamind-iceberg-gold.gold.default.network_performance::columns::network_health_score>",
        "testDefinition": "columnValuesToBeBetween",
        "parameterValues": [
            {"name": "minValue", "value": "0"},
            {"name": "maxValue", "value": "100"},
        ],
    },
    # ── gold.fraud_monitoring ────────────────────────────────────────────────
    {
        "name": "fraud__risk_score_range",
        "displayName": "avg_risk_score must be 0–1",
        "description": "Fraud risk score is a probability between 0 and 1.",
        "entityLink": "<#E::table::datamind-iceberg-gold.gold.default.fraud_monitoring::columns::avg_risk_score>",
        "testDefinition": "columnValuesToBeBetween",
        "parameterValues": [
            {"name": "minValue", "value": "0"},
            {"name": "maxValue", "value": "1"},
        ],
    },
]


def create_test_case(test: dict) -> bool:
    """POST a test case to OM."""
    payload = {
        "name": test["name"],
        "displayName": test["displayName"],
        "description": test["description"],
        "entityLink": test["entityLink"],
        "testDefinition": {"type": "testDefinition", "name": test["testDefinition"]},
        "parameterValues": test["parameterValues"],
    }
    r = requests.post(
        f"{OM_HOST}/dataQuality/testCases", headers=HEADERS, json=payload, timeout=10
    )
    if r.status_code in (200, 201):
        print(f"  ✓ {test['displayName']}")
        return True
    if r.status_code == 409:
        print(f"  ~ Already exists: {test['name']}")
        return True
    print(f"  ✗ {test['name']} — HTTP {r.status_code}: {r.text[:150]}")
    return False


def main():
    if JWT_TOKEN == "REPLACE_WITH_INGESTION_BOT_JWT":
        print("ERROR: Set the OM_JWT_TOKEN environment variable first.")
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"  DataMind AI — DQ Test Suite Bootstrap")
    print(f"  Target: {OM_HOST}")
    print(f"{'='*60}\n")
    print(f"Creating {len(TEST_CASES)} test cases...\n")

    ok = sum(create_test_case(t) for t in TEST_CASES)
    print(f"\n{ok}/{len(TEST_CASES)} test cases created.")
    print("\nNext: Run tests in OM UI → Data Quality → <table> → Run All Tests")


if __name__ == "__main__":
    main()
