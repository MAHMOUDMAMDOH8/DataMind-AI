"""
Roaming Management System — Roaming event producer.

Publishes international roaming session events to the roaming_topic Kafka topic.
"""

import logging
from typing import List, Optional

from shared.kafka_client import SourceSystemProducer
from shared.event_factory import create_roaming_event
from shared.dimensions import load_customers

from roaming_system.config import TOPIC

logger = logging.getLogger("roaming_producer")


class RoamingProducer:
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.producer = SourceSystemProducer(bootstrap_servers, client_id="roaming-system")
        self.customers: Optional[List] = None

    def load_data(self):
        self.customers = load_customers()

    def publish_roaming(self, allow_data_issues: bool = True) -> bool:
        event = create_roaming_event(self.customers, allow_data_issues=allow_data_issues)
        ok = self.producer.publish(TOPIC, event, key=event.get("sid"))
        logger.info("Published roaming_event [sid=%s]", event["sid"])
        return ok

    def close(self):
        self.producer.close()
