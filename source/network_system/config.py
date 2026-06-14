"""
Network Monitoring System configuration.
"""

DATA_USAGE_TOPIC = "data_usage_topic"
NETWORK_METRICS_TOPIC = "network_metrics_topic"
PARTITIONS = 10
REPLICATION = 3
SYSTEM_ID = "NMS-01"
OWNER = "Network Operations Center"

EVENT_TYPES = {
    "data_session_event": {"topic": DATA_USAGE_TOPIC, "schema": "nms_data_v2", "description": "Data session usage record"},
    "network_metric": {"topic": NETWORK_METRICS_TOPIC, "schema": "nms_metric_v1", "description": "Aggregate network performance metric"},
    "qos_report": {"topic": NETWORK_METRICS_TOPIC, "schema": "nms_qos_v1", "description": "Quality of Service report"},
}
