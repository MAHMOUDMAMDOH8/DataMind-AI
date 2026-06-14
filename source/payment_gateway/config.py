"""
Payment Gateway configuration.
"""

TOPIC = "payments_topic"
PARTITIONS = 8
REPLICATION = 3
SYSTEM_ID = "PAY-01"
OWNER = "Finance"
COMPLIANCE = "PCI-DSS Level 1"

EVENT_TYPES = {
    "payment_event": {"schema": "pay_v1", "description": "Payment transaction event"},
}
