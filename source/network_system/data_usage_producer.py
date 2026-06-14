"""
Network Monitoring System — Data usage event producer.

Publishes data session events to the data_usage_topic Kafka topic.
"""

import logging
from typing import List, Optional

from shared.kafka_client import SourceSystemProducer
from shared.event_factory import create_data_session_event
from shared.dimensions import load_customers, load_cell_sites

from network_system.config import DATA_USAGE_TOPIC

logger = logging.getLogger("nms_data_producer")


class DataUsageProducer:
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.producer = SourceSystemProducer(bootstrap_servers, client_id="nms-data-system")
        self.customers: Optional[List] = None
        self.cell_sites: Optional[List] = None

    def load_data(self):
        self.customers = load_customers()
        self.cell_sites = load_cell_sites()

    def publish_data_session(self, allow_data_issues: bool = True) -> bool:
        event = create_data_session_event(self.customers, self.cell_sites, allow_data_issues=allow_data_issues)
        ok = self.producer.publish(DATA_USAGE_TOPIC, event, key=event.get("sid"))
        logger.info("Published data_session_event [sid=%s]", event["sid"])
        return ok

    def close(self):
        self.producer.close()
