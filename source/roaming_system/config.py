"""
Roaming Management System configuration.
"""

TOPIC = "roaming_topic"
PARTITIONS = 6
REPLICATION = 3
SYSTEM_ID = "RMG-01"
OWNER = "International Services"

EVENT_TYPES = {
    "roaming_event": {"schema": "rmg_v1", "description": "International roaming session event"},
}
