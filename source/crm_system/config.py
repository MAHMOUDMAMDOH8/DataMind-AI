"""
CRM System configuration.
"""

TOPIC = "customer_topic"
PARTITIONS = 6
REPLICATION = 3
SYSTEM_ID = "CRM-01"
OWNER = "Customer Operations"

EVENT_TYPES = {
    "customer_registration": {"schema": "cus_reg_v1", "description": "New customer sign-up"},
    "customer_profile_update": {"schema": "cus_prof_v1", "description": "Profile attribute changes"},
}
