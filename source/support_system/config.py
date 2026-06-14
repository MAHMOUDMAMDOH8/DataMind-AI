"""
Customer Support System configuration.
"""

TOPIC = "tickets_topic"
PARTITIONS = 8
REPLICATION = 3
SYSTEM_ID = "CSU-01"
OWNER = "Customer Experience"

EVENT_TYPES = {
    "ticket_created": {"schema": "csu_ticket_v1", "description": "New support ticket"},
    "complaint_filed": {"schema": "csu_complaint_v1", "description": "Customer complaint"},
    "ticket_resolved": {"schema": "csu_resolution_v1", "description": "Ticket resolution event"},
}
