"""
Dimension data loader — loads customer master and cell site data
from JSON dimension files into memory for event generation.
"""

import json
import random
from pathlib import Path
from typing import Any, Dict, List, Optional

from shared.utils import egyptian_cities, generate_location_data, get_customer_behavior_profile

ROOT = Path(__file__).resolve().parent.parent


def load_customers(filepath: str = "DIM_USER.json") -> List[Dict[str, Any]]:
    path = ROOT / filepath if not Path(filepath).is_absolute() else Path(filepath)
    with path.open("r", encoding="utf-8") as f:
        users = json.load(f)
    for idx, user in enumerate(users, start=1001):
        if "msisdn" in user and "phone_number" not in user:
            user["phone_number"] = user["msisdn"]
        if "phone_number" not in user:
            user["phone_number"] = user.get("msisdn", f"0100000000{idx}")
        user["customer_id"] = user["phone_number"]
        user["number"] = user["phone_number"]
        if "customer_type" in user and "plan_type" not in user:
            user["plan_type"] = user["customer_type"]
        user.setdefault("plan_type", random.choice(["Prepaid", "Postpaid"]))
        if "status" not in user or not user["status"]:
            user["status"] = random.choice(["Active", "Inactive", "Suspended"])
        user["behavior_profile"] = get_customer_behavior_profile(user["customer_id"])
        city_info = random.choice(egyptian_cities)
        location = generate_location_data(city_info)
        user.update(location)
    return users


def load_cell_sites(filepath: str = "dim_cell_site.json") -> List[Dict[str, Any]]:
    path = ROOT / filepath if not Path(filepath).is_absolute() else Path(filepath)
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)
