"""
Recharge Platform — Recharge event producer.

Publishes balance recharge and top-up events to the recharge_topic Kafka topic.
"""

import logging
from typing import List, Optional

from shared.kafka_client import SourceSystemProducer
from shared.event_factory import create_recharge_event
from shared.dimensions import load_customers

from recharge_platform.config import TOPIC

logger = logging.getLogger("recharge_producer")


class RechargeProducer:
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.producer = SourceSystemProducer(bootstrap_servers, client_id="recharge-platform")
        self.customers: Optional[List] = None

    def load_data(self):
        self.customers = load_customers()

    def publish_recharge(self, allow_data_issues: bool = True) -> bool:
        event = create_recharge_event(self.customers, allow_data_issues=allow_data_issues)
        ok = self.producer.publish(TOPIC, event, key=event.get("sid"))
        logger.info("Published recharge_event [sid=%s]", event["sid"])
        return ok

    def close(self):
        self.producer.close()
