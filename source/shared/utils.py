"""
Shared utility functions and data catalogs for all source system producers.

Includes geographic data, behavior profiles, seasonal patterns,
network metrics generation, device/TAC catalog, and data quality injection.
"""

import random
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Geographic catalog
# ---------------------------------------------------------------------------
egyptian_cities: List[Dict[str, Any]] = [
    {"city": "Cairo", "region": "Cairo", "lat_range": (29.9, 30.1), "lon_range": (31.2, 31.4)},
    {"city": "Alexandria", "region": "Alexandria", "lat_range": (31.0, 31.3), "lon_range": (29.8, 30.0)},
    {"city": "Giza", "region": "Giza", "lat_range": (29.9, 30.1), "lon_range": (31.1, 31.3)},
    {"city": "Luxor", "region": "Upper Egypt", "lat_range": (25.6, 25.7), "lon_range": (32.6, 32.7)},
    {"city": "Aswan", "region": "Upper Egypt", "lat_range": (24.0, 24.1), "lon_range": (32.8, 32.9)},
    {"city": "Port Said", "region": "Suez Canal", "lat_range": (31.2, 31.3), "lon_range": (32.2, 32.3)},
]

# ---------------------------------------------------------------------------
# Customer behavior profiles
# ---------------------------------------------------------------------------
BEHAVIOR_PROFILES: Dict[str, Dict[str, Any]] = {
    "heavy_streamer": {
        "weights": {"sms": 20, "call": 25, "data_usage": 40, "recharge": 5, "payment": 5},
        "data_multiplier": 3.0, "call_multiplier": 0.5,
        "typical_apps": ["YouTube", "Netflix", "Spotify", "TikTok", "Twitch"],
    },
    "business_user": {
        "weights": {"sms": 20, "call": 50, "data_usage": 15, "recharge": 5, "payment": 5},
        "call_multiplier": 2.5, "sms_multiplier": 1.5, "international_multiplier": 3.0,
        "typical_hours": [8, 9, 10, 11, 14, 15, 16, 17],
    },
    "light_user": {
        "weights": {"sms": 40, "call": 30, "data_usage": 10, "recharge": 10, "payment": 5},
        "all_multiplier": 0.3,
        "typical_days": ["Saturday", "Sunday"],
    },
    "international": {
        "weights": {"sms": 15, "call": 40, "data_usage": 20, "recharge": 10, "payment": 5},
        "roaming_weight": 15, "international_multiplier": 5.0,
    },
    "gamer": {
        "weights": {"sms": 10, "call": 10, "data_usage": 60, "recharge": 10, "payment": 5},
        "data_multiplier": 4.0, "latency_sensitive": True,
        "typical_apps": ["Steam", "Discord", "Twitch", "PUBG", "Fortnite"],
    },
    "social_media": {
        "weights": {"sms": 30, "call": 20, "data_usage": 35, "recharge": 5, "payment": 5},
        "sms_multiplier": 2.0, "data_multiplier": 2.5,
        "typical_apps": ["Facebook", "Instagram", "WhatsApp", "Twitter", "Snapchat"],
    },
}

# ---------------------------------------------------------------------------
# Fraud detection markers
# ---------------------------------------------------------------------------
FRAUD_PATTERNS: Dict[str, Dict[str, Any]] = {
    "international_short_calls": {"marker": "WANGIRI_FRAUD"},
    "midnight_data_spike": {"marker": "BOTNET_ACTIVITY"},
    "velocity_anomaly": {"marker": "CLONED_SIM"},
    "roaming_activation_fraud": {"marker": "ROAMING_FRAUD"},
}

# ---------------------------------------------------------------------------
# Roaming partner catalog
# ---------------------------------------------------------------------------
ROAMING_COUNTRIES: List[Dict[str, Any]] = [
    {"country": "USA", "operator": "AT&T", "rate_per_min": 2.5, "rate_per_mb": 0.15},
    {"country": "UK", "operator": "Vodafone", "rate_per_min": 1.8, "rate_per_mb": 0.12},
    {"country": "UAE", "operator": "Etisalat", "rate_per_min": 1.2, "rate_per_mb": 0.08},
    {"country": "KSA", "operator": "STC", "rate_per_min": 1.0, "rate_per_mb": 0.06},
    {"country": "Germany", "operator": "Deutsche Telekom", "rate_per_min": 2.0, "rate_per_mb": 0.10},
    {"country": "France", "operator": "Orange", "rate_per_min": 1.8, "rate_per_mb": 0.11},
    {"country": "Italy", "operator": "TIM", "rate_per_min": 1.5, "rate_per_mb": 0.09},
    {"country": "Qatar", "operator": "Ooredoo", "rate_per_min": 1.3, "rate_per_mb": 0.07},
]

# ---------------------------------------------------------------------------
# Security event catalog
# ---------------------------------------------------------------------------
SECURITY_EVENTS: List[Dict[str, Any]] = [
    {"type": "failed_login", "severity": "medium", "typical_count": 3},
    {"type": "sim_swap_request", "severity": "high", "typical_count": 1},
    {"type": "number_porting_request", "severity": "high", "typical_count": 1},
]

# ---------------------------------------------------------------------------
# Device / TAC catalog
# ---------------------------------------------------------------------------
sample_messages: List[str] = [
    "Hello!",
    "Your verification code is 123456",
    "Service outage in your area",
    "Network maintenance scheduled",
    "Your bill is ready for review",
    "Special offer: 200% bonus on recharge",
]

tac_to_manufacturer: Dict[str, str] = {
    "35846279": "Samsung", "35846270": "Apple", "86945303": "Huawei",
    "35846103": "Xiaomi", "35944803": "Oppo",
}
tac_codes: List[str] = list(tac_to_manufacturer.keys())

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def apply_seasonal_patterns(timestamp: datetime) -> float:
    hour = timestamp.hour
    day = timestamp.weekday()
    month = timestamp.month
    multiplier = 1.0
    if 8 <= hour <= 18:
        multiplier *= 1.3
    elif 19 <= hour <= 23:
        multiplier *= 1.5
    elif 0 <= hour <= 6:
        multiplier *= 0.3
    if day >= 5:
        multiplier *= 1.4
    if month == 9:
        multiplier *= 1.6
        if 20 <= hour <= 24 or 0 <= hour <= 3:
            multiplier *= 1.5
    if 6 <= month <= 8:
        multiplier *= 1.2
    return multiplier


def get_customer_behavior_profile(customer_id: str) -> str:
    profiles = list(BEHAVIOR_PROFILES.keys())
    return profiles[hash(customer_id) % len(profiles)]


def generate_location_data(city_info: Optional[dict] = None) -> dict:
    if not city_info:
        city_info = random.choice(egyptian_cities)
    lat = random.uniform(*city_info["lat_range"])
    lon = random.uniform(*city_info["lon_range"])
    return {
        "latitude": round(lat, 6),
        "longitude": round(lon, 6),
        "city": city_info["city"],
        "region": city_info["region"],
        "accuracy_meters": random.randint(10, 500),
        "location_source": random.choice(["GPS", "CELL", "WIFI", "IP"]),
    }


def generate_network_metrics() -> dict:
    return {
        "signal_strength_dbm": random.randint(-120, -50),
        "latency_ms": random.randint(10, 500),
        "packet_loss_percent": round(random.uniform(0, 5), 2),
        "cell_congestion": random.choice(["low", "medium", "high"]),
        "handover_success": random.choice([True, False]),
        "mos_score": round(random.uniform(1.0, 5.0), 2),
        "jitter_ms": round(random.uniform(0, 50), 2),
        "throughput_mbps": round(random.uniform(1, 100), 2),
    }


def generate_imei_with_manufacturer(allow_invalid: bool = False):
    if allow_invalid and random.random() < 0.05:
        if random.random() < 0.5:
            return "".join(str(random.randint(0, 9)) for _ in range(10)), None
        return "".join(random.choice("0123456789ABCDEF") for _ in range(15)), None
    tac = random.choice(tac_codes)
    serial = "".join(str(random.randint(0, 9)) for _ in range(7))
    return tac + serial, tac_to_manufacturer[tac]


def generate_user_info(customers: List[dict], cell_sites: List[dict],
                       allow_data_issues: bool = False, force_clean: bool = False) -> dict:
    customer = random.choice(customers)
    imei, manufacturer = generate_imei_with_manufacturer(
        allow_invalid=(allow_data_issues and not force_clean))
    phone_number = customer["phone_number"]
    if allow_data_issues and not force_clean:
        if random.random() < 0.06:
            choice = random.random()
            if choice < 0.25:
                phone_number = None
            elif choice < 0.5:
                phone_number = "0000000000"
            elif choice < 0.75:
                phone_number = f"+20{random.randint(100000000, 999999999)}"
            else:
                phone_number = f"  {phone_number}  "
    cell_site_id = random.choice(cell_sites)["cell_id"]
    if allow_data_issues and not force_clean and random.random() < 0.04:
        cell_site_id = None if random.random() < 0.5 else "INVALID_CELL"
    return {
        "number": phone_number,
        "cell_site": cell_site_id,
        "imei": imei,
        "customer_id": customer.get("customer_id"),
        "plan_type": customer.get("plan_type"),
        "behavior_profile": customer.get("behavior_profile"),
        "location": {"city": customer.get("city"), "region": customer.get("region"),
                     "latitude": customer.get("latitude"), "longitude": customer.get("longitude")},
        "manufacturer": manufacturer,
    }


def get_clean_phone_number(customers: List[dict]) -> str:
    return random.choice(customers)["phone_number"]


def introduce_data_quality_issues(event: dict, event_type: str, force_clean: bool = False) -> None:
    if force_clean:
        return
    if random.random() < 0.08:
        if isinstance(event.get("from"), dict):
            event["from"][random.choice(["phone_number", "cell_site", "imei"])] = None
        else:
            event["from"] = None
    if random.random() < 0.03:
        event["timestamp"] = random.choice(["INVALID_DATE", "2025-13-45 25:99:99"])
    if random.random() < 0.02:
        event["sid"] = None
