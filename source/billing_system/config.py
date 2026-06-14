"""
Billing System configuration.
"""

CALLS_TOPIC = "calls_topic"
SMS_TOPIC = "sms_topic"
PARTITIONS = 12
REPLICATION = 3
SYSTEM_ID = "BIL-01"
OWNER = "Finance / Revenue Assurance"

EVENT_TYPES = {
    "call_cdr": {"topic": CALLS_TOPIC, "schema": "cdr_call_v2", "description": "Call Detail Record"},
    "sms_event": {"topic": SMS_TOPIC, "schema": "cdr_sms_v2", "description": "SMS record"},
}
