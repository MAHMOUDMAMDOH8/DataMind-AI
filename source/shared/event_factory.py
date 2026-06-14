"""
Event factory — creates telecom event dictionaries for all source systems.

Each factory function returns a raw event dict matching the Bronze schema
defined in the data model. These are consumed by system-specific producers
which handle Kafka serialization and publishing.
"""

import random
from datetime import datetime, timedelta

from shared.utils import (
    BEHAVIOR_PROFILES,
    FRAUD_PATTERNS,
    ROAMING_COUNTRIES,
    egyptian_cities,
    sample_messages,
    apply_seasonal_patterns,
    generate_user_info,
    generate_network_metrics,
    get_clean_phone_number,
    introduce_data_quality_issues,
)

# ---------------------------------------------------------------------------
# CRM System
# ---------------------------------------------------------------------------

def create_customer_registration(customers, base_time=None):
    if base_time is None:
        base_time = datetime.now() - timedelta(days=random.randint(0, 365))
    phone = get_clean_phone_number(customers)
    city_info = random.choice(egyptian_cities)
    return {
        "event_type": "customer_registration",
        "sid": f"REG{random.randint(10**8, 10**9)}",
        "customer": phone, "phone_number": phone,
        "plan_type": random.choice(["Prepaid", "Postpaid"]),
        "behavior_profile": random.choice(list(BEHAVIOR_PROFILES.keys())),
        "city": city_info["city"], "region": city_info["region"],
        "registration_date": base_time.strftime("%Y-%m-%d"),
        "timestamp": base_time.strftime("%Y-%m-%d %H:%M:%S"),
        "status": "active",
        "channel": random.choice(["web", "app", "store", "agent"]),
        "seasonal_multiplier": round(apply_seasonal_patterns(base_time), 2),
    }


def create_customer_profile_update(customers, base_time=None):
    if base_time is None:
        base_time = datetime.now() - timedelta(days=random.randint(0, 30))
    phone = get_clean_phone_number(customers)
    return {
        "event_type": "customer_profile_update",
        "sid": f"PRF{random.randint(10**8, 10**9)}",
        "customer": phone, "phone_number": phone,
        "updated_fields": random.sample(
            ["plan_type", "behavior_profile", "city", "status", "contact_email"],
            k=random.randint(1, 3)),
        "timestamp": base_time.strftime("%Y-%m-%d %H:%M:%S"),
        "seasonal_multiplier": round(apply_seasonal_patterns(base_time), 2),
    }

# ---------------------------------------------------------------------------
# Billing System — Call CDRs
# ---------------------------------------------------------------------------

def create_call_cdr(customers, cell_sites, base_time=None, allow_data_issues=True, force_clean=False):
    customer = random.choice(customers)
    profile = BEHAVIOR_PROFILES.get(customer.get("behavior_profile", "light_user"), BEHAVIOR_PROFILES["light_user"])
    if base_time is None:
        base_time = datetime.now() - timedelta(days=random.randint(0, 30))
        base_time += timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59), seconds=random.randint(0, 59))

    sm = apply_seasonal_patterns(base_time)
    status = random.choice(["initiated", "ringing", "in-progress", "completed", "failed", "busy", "no-answer"])
    cw = 2.0 if customer.get("behavior_profile") == "business_user" and 8 <= base_time.hour <= 18 else 1.0

    if status == "completed":
        dur = int(random.randint(30, 3600) * profile.get("call_multiplier", 1.0) * cw)
        amount = round((dur / 60) * 0.16 * sm, 2)
    elif status == "in-progress":
        dur = random.randint(1, 300)
        amount = round((dur / 60) * 0.16 * sm, 2)
    elif status == "ringing":
        dur = random.randint(1, 30)
        amount = 0.0
    else:
        dur = 0
        amount = 0.0

    from_info = generate_user_info(customers, cell_sites, allow_data_issues, force_clean)
    to_info = generate_user_info(customers, cell_sites, allow_data_issues, force_clean)

    event = {
        "event_type": "call",
        "sid": f"CA{random.randint(10**8, 10**9)}",
        "from": {"phone_number": from_info["number"], "cell_site": from_info["cell_site"], "imei": from_info["imei"]},
        "to": {"phone_number": to_info["number"], "cell_site": to_info["cell_site"], "imei": to_info["imei"]},
        "call_duration_seconds": dur,
        "status": status,
        "timestamp": base_time.strftime("%Y-%m-%d %H:%M:%S"),
        "call_type": "International" if customer.get("behavior_profile") == "international" and random.random() < 0.3 else "Local",
        "phone_number": from_info["number"],
        "customer": from_info["number"],
        "behavior_profile": customer.get("behavior_profile"),
        "seasonal_multiplier": round(sm, 2),
        "billing_info": {"amount": amount, "currency": "EGP"},
        "qos_metrics": {
            "mos_score": round(random.uniform(1.0, 5.0), 2),
            "jitter_ms": round(random.uniform(0, 50), 2),
            "packet_loss_percent": round(random.uniform(0, 5), 2),
            "codec": random.choice(["AMR", "EVS", "OPUS", "G.711"]),
        },
    }

    if allow_data_issues:
        introduce_data_quality_issues(event, "call", force_clean)
        if not force_clean and random.random() < 0.04:
            event["call_duration_seconds"] = None if random.random() < 0.5 else -10

    if event["call_type"] == "International" and dur < 30 and random.random() < 0.05:
        event["fraud_indicator"] = FRAUD_PATTERNS["international_short_calls"]["marker"]
        event["risk_score"] = random.randint(70, 95)

    return event

# ---------------------------------------------------------------------------
# Billing System — SMS
# ---------------------------------------------------------------------------

def create_sms_event(customers, cell_sites, base_time=None, allow_data_issues=True, force_clean=False):
    if base_time is None:
        base_time = datetime.now() - timedelta(days=random.randint(0, 30))
        base_time += timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59), seconds=random.randint(0, 59))

    sm = apply_seasonal_patterns(base_time)
    status = random.choice(["queued", "sending", "sent", "delivered", "failed"])
    amount = round(random.uniform(0.10, 0.50) * sm, 2) if status == "delivered" else (round(random.uniform(0.05, 0.25) * sm, 2) if status == "sent" else 0.0)

    from_info = generate_user_info(customers, cell_sites, allow_data_issues, force_clean)
    to_info = generate_user_info(customers, cell_sites, allow_data_issues, force_clean)

    event = {
        "event_type": "sms",
        "sid": f"SM{random.randint(10**8, 10**9)}",
        "from": {"phone_number": from_info["number"], "cell_site": from_info["cell_site"], "imei": from_info["imei"]},
        "to": {"phone_number": to_info["number"], "cell_site": to_info["cell_site"], "imei": to_info["imei"]},
        "body": random.choice(sample_messages),
        "status": status,
        "timestamp": base_time.strftime("%Y-%m-%d %H:%M:%S"),
        "phone_number": from_info["number"],
        "customer": from_info["number"],
        "registration_date": (base_time - timedelta(days=random.randint(1, 1095))).strftime("%Y-%m-%d"),
        "seasonal_multiplier": round(sm, 2),
        "billing_info": {"amount": amount, "currency": "EGP"},
    }

    if random.random() < 0.5:
        event["network_metrics"] = generate_network_metrics()
    if allow_data_issues:
        introduce_data_quality_issues(event, "sms", force_clean)

    return event

# ---------------------------------------------------------------------------
# Network Monitoring System — Data Usage
# ---------------------------------------------------------------------------

def create_data_session_event(customers, cell_sites, base_time=None, allow_data_issues=True, force_clean=False):
    user = generate_user_info(customers, cell_sites, allow_data_issues, force_clean)
    profile = BEHAVIOR_PROFILES.get(user.get("behavior_profile"), BEHAVIOR_PROFILES["light_user"])
    if base_time is None:
        base_time = datetime.now() - timedelta(days=random.randint(0, 30))
        base_time += timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59), seconds=random.randint(0, 59))

    sm = apply_seasonal_patterns(base_time)
    data_mb = round(random.uniform(10, 1024) * profile.get("data_multiplier", 1.0) * sm, 2)
    session_sec = random.randint(60, 7200)

    event = {
        "event_type": "data_usage",
        "sid": f"DU{random.randint(10**8, 10**9)}",
        "customer": user["number"],
        "data_used_mb": data_mb,
        "data_type": random.choice(["Browsing", "Streaming", "Upload", "Download", "Background Sync"]),
        "network_type": random.choice(["3G", "4G", "LTE", "5G"]),
        "status": random.choice(["active", "completed", "throttled", "exceeded"]),
        "phone_number": user["number"],
        "behavior_profile": user.get("behavior_profile"),
        "session_duration_seconds": session_sec,
        "session_start": base_time.strftime("%Y-%m-%d %H:%M:%S"),
        "session_end": (base_time + timedelta(seconds=session_sec)).strftime("%Y-%m-%d %H:%M:%S"),
        "timestamp": base_time.strftime("%Y-%m-%d %H:%M:%S"),
        "seasonal_multiplier": round(sm, 2),
        "billing_info": {"amount": round(data_mb * 0.05 * sm, 2), "currency": "EGP"},
        "network_metrics": generate_network_metrics(),
    }

    if not force_clean and random.random() < 0.03:
        event["fraud_indicator"] = FRAUD_PATTERNS["midnight_data_spike"]["marker"]
        event["risk_score"] = random.randint(70, 95)

    return event


def create_network_metric(customers, cell_sites, base_time=None):
    if base_time is None:
        base_time = datetime.now() - timedelta(days=random.randint(0, 7))
    cell = random.choice(cell_sites)
    city = random.choice(egyptian_cities)
    return {
        "event_type": "network_metric",
        "sid": f"NM{random.randint(10**8, 10**9)}",
        "cell_site_id": cell["cell_id"],
        "city": city["city"], "region": city["region"],
        "network_type": random.choice(["3G", "4G", "LTE", "5G"]),
        "timestamp": base_time.strftime("%Y-%m-%d %H:%M:%S"),
        "metrics": generate_network_metrics(),
        "active_subscribers": random.randint(50, 5000),
        "total_throughput_mbps": round(random.uniform(10, 1000), 2),
        "cpu_utilization_pct": round(random.uniform(20, 95), 1),
        "memory_utilization_pct": round(random.uniform(30, 90), 1),
    }


def create_qos_report(customers, cell_sites, base_time=None):
    if base_time is None:
        base_time = datetime.now() - timedelta(days=random.randint(0, 7))
    cell = random.choice(cell_sites)
    city = random.choice(egyptian_cities)
    return {
        "event_type": "qos_report",
        "sid": f"QoS{random.randint(10**8, 10**9)}",
        "cell_site_id": cell["cell_id"],
        "city": city["city"], "region": city["region"],
        "network_type": random.choice(["4G", "LTE", "5G"]),
        "timestamp": base_time.strftime("%Y-%m-%d %H:%M:%S"),
        "mos_score_avg": round(random.uniform(2.0, 4.8), 2),
        "jitter_ms_avg": round(random.uniform(5, 45), 2),
        "packet_loss_pct_avg": round(random.uniform(0, 3), 2),
        "latency_ms_avg": random.randint(15, 300),
        "throughput_mbps_avg": round(random.uniform(5, 500), 2),
        "rtt_ms_avg": random.randint(20, 400),
        "sample_size": random.randint(100, 10000),
    }

# ---------------------------------------------------------------------------
# Payment Gateway
# ---------------------------------------------------------------------------

def create_payment_event(customers, base_time=None, allow_data_issues=True, force_clean=False):
    if base_time is None:
        base_time = datetime.now() - timedelta(days=random.randint(0, 30))
        base_time += timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59), seconds=random.randint(0, 59))
    sm = apply_seasonal_patterns(base_time)
    ptype = random.choice(["Bill Payment", "Subscription", "Plan Upgrade", "Service Fee", "Late Fee"])
    amounts = {"Bill Payment": (50, 500), "Subscription": (100, 300), "Plan Upgrade": (200, 1000)}
    lo, hi = amounts.get(ptype, (10, 100))
    phone = get_clean_phone_number(customers)
    event = {
        "event_type": "payment",
        "sid": f"PAY{random.randint(10**8, 10**9)}",
        "customer": phone, "payment_type": ptype,
        "payment_amount": round(random.uniform(lo, hi) * sm, 2),
        "payment_method": random.choice(["Credit Card", "Mobile Wallet", "Bank Transfer", "Auto-Debit"]),
        "status": random.choice(["success", "pending", "failed", "refunded"]),
        "timestamp": base_time.strftime("%Y-%m-%d %H:%M:%S"),
        "phone_number": phone,
        "transaction_id": f"PAY{random.randint(10**10, 10**11)}",
        "invoice_number": f"INV{random.randint(10**6, 10**7)}" if ptype == "Bill Payment" else None,
        "seasonal_multiplier": round(sm, 2),
        "billing_info": {"amount": round(random.uniform(lo, hi) * sm, 2), "currency": "EGP"},
    }
    return event

# ---------------------------------------------------------------------------
# Recharge Platform
# ---------------------------------------------------------------------------

def create_recharge_event(customers, base_time=None, allow_data_issues=True, force_clean=False):
    if base_time is None:
        base_time = datetime.now() - timedelta(days=random.randint(0, 30))
        base_time += timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59), seconds=random.randint(0, 59))
    sm = apply_seasonal_patterns(base_time)
    amount = random.choice([10, 20, 50, 100, 200, 500, 1000])
    phone = get_clean_phone_number(customers)
    event = {
        "event_type": "recharge",
        "sid": f"RC{random.randint(10**8, 10**9)}",
        "customer": phone, "recharge_amount": amount,
        "balance_before": round(random.uniform(0, 50), 2),
        "balance_after": round(random.uniform(amount, amount + 500) * sm, 2),
        "payment_method": random.choice(["Credit Card", "Mobile Wallet", "Bank Transfer", "Cash"]),
        "status": random.choice(["success", "pending", "failed", "processing"]),
        "timestamp": base_time.strftime("%Y-%m-%d %H:%M:%S"),
        "phone_number": phone,
        "transaction_id": f"TXN{random.randint(10**10, 10**11)}",
        "seasonal_multiplier": round(sm, 2),
        "billing_info": {"amount": amount, "currency": "EGP"},
    }
    if event["status"] == "failed" and random.random() < 0.7:
        event["_requires_followup"] = True
    return event

# ---------------------------------------------------------------------------
# Roaming Management System
# ---------------------------------------------------------------------------

def create_roaming_event(customers, base_time=None, allow_data_issues=True, force_clean=False):
    if base_time is None:
        base_time = datetime.now() - timedelta(days=random.randint(0, 30))
        base_time += timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59), seconds=random.randint(0, 59))
    sm = apply_seasonal_patterns(base_time)
    phone = get_clean_phone_number(customers)
    dest = random.choice(ROAMING_COUNTRIES)
    rtype = random.choice(["voice", "data", "sms", "all"])
    dur = random.randint(30, 3600) if rtype in ("voice", "all") else 0
    data = round(random.uniform(1, 500) * sm, 2) if rtype in ("data", "all") else 0.0
    charges = (dur / 60) * dest["rate_per_min"] + data * dest["rate_per_mb"]
    if rtype == "sms":
        charges += random.uniform(0.5, 2.0)
    return {
        "event_type": "roaming",
        "sid": f"RO{random.randint(10**8, 10**9)}",
        "customer": phone, "phone_number": phone,
        "roaming_country": dest["country"], "roaming_operator": dest["operator"],
        "roaming_type": rtype, "duration_seconds": dur,
        "data_used_mb": data,
        "roaming_charges": round(charges * sm, 2),
        "status": random.choice(["active", "completed", "terminated"]),
        "timestamp": base_time.strftime("%Y-%m-%d %H:%M:%S"),
        "local_time": (base_time + timedelta(hours=random.choice([-5, -2, 0, 1, 2, 3]))).strftime("%Y-%m-%d %H:%M:%S"),
        "seasonal_multiplier": round(sm, 2),
        "billing_info": {"amount": round(charges * sm, 2), "currency": "EGP"},
    }

# ---------------------------------------------------------------------------
# Customer Support System
# ---------------------------------------------------------------------------

def create_ticket_created(customers, base_time=None, allow_data_issues=True, force_clean=False):
    if base_time is None:
        base_time = datetime.now() - timedelta(days=random.randint(0, 30))
        base_time += timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59), seconds=random.randint(0, 59))
    phone = get_clean_phone_number(customers)
    return {
        "event_type": "ticket_created",
        "sid": f"TCK{random.randint(10**8, 10**9)}",
        "customer": phone, "phone_number": phone,
        "channel": random.choice(["phone", "chat", "email", "store", "social_media", "ivr"]),
        "reason": random.choice(["billing", "technical", "account", "complaint", "activation", "general"]),
        "priority": random.choice(["low", "medium", "high", "critical"]),
        "status": "open",
        "timestamp": base_time.strftime("%Y-%m-%d %H:%M:%S"),
        "ticket_id": f"TKT{random.randint(10**7, 10**8)}",
    }


def create_complaint_filed(customers, base_time=None, allow_data_issues=True, force_clean=False):
    if base_time is None:
        base_time = datetime.now() - timedelta(days=random.randint(0, 30))
        base_time += timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59), seconds=random.randint(0, 59))
    phone = get_clean_phone_number(customers)
    cat = random.choice(["network_quality", "billing_dispute", "service_outage", "customer_service", "product_issue"])
    sev = {"network_quality": "medium", "billing_dispute": "high", "service_outage": "critical",
           "customer_service": "low", "product_issue": "medium"}
    return {
        "event_type": "complaint_filed",
        "sid": f"CMP{random.randint(10**8, 10**9)}",
        "customer": phone, "phone_number": phone,
        "complaint_category": cat, "severity": sev[cat],
        "description": f"Complaint regarding {cat.replace('_', ' ')}",
        "timestamp": base_time.strftime("%Y-%m-%d %H:%M:%S"),
    }


def create_ticket_resolved(customers, base_time=None, allow_data_issues=True, force_clean=False):
    if base_time is None:
        base_time = datetime.now() - timedelta(days=random.randint(0, 30))
        base_time += timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59), seconds=random.randint(0, 59))
    phone = get_clean_phone_number(customers)
    return {
        "event_type": "ticket_resolved",
        "sid": f"RES{random.randint(10**8, 10**9)}",
        "customer": phone, "phone_number": phone,
        "agent_id": f"AG{random.randint(1000, 9999)}",
        "resolution_time_seconds": random.randint(60, 86400),
        "wait_time_seconds": random.randint(0, 3600),
        "satisfaction_score": random.randint(1, 5) if random.random() < 0.7 else None,
        "first_call_resolution": random.choice([True, False]),
        "escalated": random.random() < 0.2,
        "call_back_requested": random.random() < 0.3,
        "timestamp": base_time.strftime("%Y-%m-%d %H:%M:%S"),
    }
