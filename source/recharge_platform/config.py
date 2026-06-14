"""
Recharge Platform configuration.
"""

TOPIC = "recharge_topic"
PARTITIONS = 6
REPLICATION = 3
SYSTEM_ID = "RCH-01"
OWNER = "Digital Commerce"

EVENT_TYPES = {
    "recharge_event": {"schema": "rch_v1", "description": "Balance recharge / top-up event"},
}
