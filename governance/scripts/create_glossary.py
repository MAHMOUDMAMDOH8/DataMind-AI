"""
DataMind AI — OpenMetadata Glossary Bootstrap
=============================================
Creates the DataMind Telecom Glossary with all domain-specific terms
and links them to physical table columns via the OM REST API.

Glossary terms:
  Customer Lifetime Value (CLV), CDR, ARPU, Churn Score,
  QoS Score, Roaming Session, Network KPI,
  Payment Success Rate, MSISDN

Usage (run from project root on the target machine):
    python governance/scripts/create_glossary.py

Prerequisites:
    pip install requests
    Set OM_JWT_TOKEN env var
    Run AFTER ingestion phases 3–4 so tables exist in OM.
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

GLOSSARY_NAME = "DataMind Telecom Glossary"

# ── Term definitions ──────────────────────────────────────────────────────────
GLOSSARY_TERMS = [
    {
        "name": "Customer Lifetime Value",
        "displayName": "CLV — Customer Lifetime Value",
        "description": (
            "The total revenue a business can reasonably expect from a single customer "
            "account throughout the business relationship. Calculated from historical "
            "revenue patterns and ML-based future projections."
        ),
        "synonyms": ["CLV", "LTV", "Lifetime Value"],
        "relatedTerms": [],
    },
    {
        "name": "CDR",
        "displayName": "CDR — Call Detail Record",
        "description": (
            "A record produced by a telephone exchange or other telecom equipment "
            "documenting the details of a telephone call. Contains caller/callee numbers, "
            "cell sites, call duration, QoS metrics, and billing amount."
        ),
        "synonyms": ["Call Detail Record", "Call Record", "XDR"],
        "relatedTerms": [],
    },
    {
        "name": "ARPU",
        "displayName": "ARPU — Average Revenue Per User",
        "description": (
            "A measure of revenue generated per user/customer, usually per month. "
            "Formula: SUM(total_revenue) / COUNT(DISTINCT customer_id). "
            "Key telecom KPI for monetisation performance."
        ),
        "synonyms": ["Average Revenue Per User", "ARPU"],
        "relatedTerms": [],
    },
    {
        "name": "Churn Score",
        "displayName": "Churn Score",
        "description": (
            "An ML-derived probability score between 0.0 and 1.0 indicating the likelihood "
            "that a customer will discontinue their service within the next 30 days. "
            "Higher values indicate greater churn risk."
        ),
        "synonyms": ["Churn Probability", "Churn Risk"],
        "relatedTerms": [],
    },
    {
        "name": "QoS Score",
        "displayName": "QoS Score — Quality of Service Score",
        "description": (
            "A composite network quality score derived from MOS (Mean Opinion Score), "
            "jitter, packet loss, and latency measurements. Used to assess network health "
            "per cell site. Higher is better."
        ),
        "synonyms": ["Quality of Service", "QoS", "Network Quality Score"],
        "relatedTerms": [],
    },
    {
        "name": "Roaming Session",
        "displayName": "Roaming Session",
        "description": (
            "A data or voice session conducted by a subscriber on a foreign (visited) "
            "operator network while their home network is the DataMind carrier. "
            "Subject to inter-operator settlement charges (TAP3 records)."
        ),
        "synonyms": ["International Roaming", "Visitor Roaming"],
        "relatedTerms": [],
    },
    {
        "name": "Network KPI",
        "displayName": "Network KPI — Key Performance Indicator",
        "description": (
            "Key Performance Indicators for cell site and network health, including: "
            "avg_throughput_mbps, cpu_utilization_pct, memory_utilization_pct, "
            "avg_mos_score, avg_jitter_ms, avg_packet_loss_pct, avg_latency_ms, "
            "and the composite network_health_score."
        ),
        "synonyms": ["Network KPI", "Cell Site KPI", "NOC Metric"],
        "relatedTerms": [],
    },
    {
        "name": "Payment Success Rate",
        "displayName": "Payment Success Rate",
        "description": (
            "The percentage of payment transactions that completed successfully. "
            "Formula: SUM(successful_transactions) / SUM(transaction_count). "
            "Tracked daily by payment method in gold.payment_analytics."
        ),
        "synonyms": ["Payment Conversion Rate", "Transaction Success Rate"],
        "relatedTerms": [],
    },
    {
        "name": "MSISDN",
        "displayName": "MSISDN — Mobile Subscriber Number",
        "description": (
            "Mobile Station International Subscriber Directory Number. "
            "The complete phone number of a mobile subscriber, including country code "
            "and national destination code. Used as the primary identifier across "
            "CDR, billing, payments, and roaming records."
        ),
        "synonyms": ["Phone Number", "Mobile Number", "MSISDN", "phone_number"],
        "relatedTerms": [],
    },
]


def create_glossary(name: str, description: str) -> str | None:
    """Create the top-level glossary. Returns its ID."""
    payload = {
        "name": name.replace(" ", "_"),
        "displayName": name,
        "description": description,
    }
    r = requests.post(f"{OM_HOST}/glossaries", headers=HEADERS, json=payload, timeout=10)
    if r.status_code in (200, 201):
        gid = r.json()["id"]
        print(f"✓ Glossary created: '{name}' (id={gid})")
        return gid
    if r.status_code == 409:
        # Already exists — fetch it
        r2 = requests.get(
            f"{OM_HOST}/glossaries/name/{name.replace(' ', '_')}",
            headers=HEADERS,
            timeout=10,
        )
        if r2.status_code == 200:
            gid = r2.json()["id"]
            print(f"✓ Glossary already exists: '{name}' (id={gid})")
            return gid
    print(f"✗ Could not create glossary: HTTP {r.status_code} — {r.text[:200]}")
    return None


def create_term(glossary_id: str, term: dict) -> bool:
    """Create a glossary term under the given glossary."""
    payload = {
        "name": term["name"],
        "displayName": term["displayName"],
        "description": term["description"],
        "synonyms": term.get("synonyms", []),
        "glossary": {"id": glossary_id, "type": "glossary"},
    }
    r = requests.post(
        f"{OM_HOST}/glossaryTerms", headers=HEADERS, json=payload, timeout=10
    )
    if r.status_code in (200, 201):
        print(f"  ✓ Term: {term['displayName']}")
        return True
    if r.status_code == 409:
        print(f"  ~ Term already exists: {term['name']}")
        return True
    print(f"  ✗ Failed: {term['name']} — HTTP {r.status_code}: {r.text[:150]}")
    return False


def main():
    if JWT_TOKEN == "REPLACE_WITH_INGESTION_BOT_JWT":
        print("ERROR: Set the OM_JWT_TOKEN environment variable first.")
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"  DataMind AI — Glossary Bootstrap")
    print(f"  Target: {OM_HOST}")
    print(f"{'='*60}\n")

    glossary_id = create_glossary(
        GLOSSARY_NAME,
        "Business terminology for the DataMind AI telecom data platform. "
        "Covers CRM, Billing, Network, Payments, Roaming, and Support domains.",
    )
    if not glossary_id:
        sys.exit(1)

    print(f"\nCreating {len(GLOSSARY_TERMS)} terms...")
    print("-" * 50)
    ok = sum(create_term(glossary_id, t) for t in GLOSSARY_TERMS)
    print(f"\n{ok}/{len(GLOSSARY_TERMS)} terms created successfully.")
    print("\nNext: Link terms to columns in OM UI → Glossary → <term> → Add Assets")


if __name__ == "__main__":
    main()
