"""
Payment Gateway — Payment event producer.

Publishes payment transaction events to the payments_topic Kafka topic.
"""

import logging
from typing import List, Optional

from shared.kafka_client import SourceSystemProducer
from shared.event_factory import create_payment_event
from shared.dimensions import load_customers

from payment_gateway.config import TOPIC

logger = logging.getLogger("payment_producer")


class PaymentProducer:
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.producer = SourceSystemProducer(bootstrap_servers, client_id="payment-gateway")
        self.customers: Optional[List] = None

    def load_data(self):
        self.customers = load_customers()

    def publish_payment(self, allow_data_issues: bool = True) -> bool:
        event = create_payment_event(self.customers, allow_data_issues=allow_data_issues)
        ok = self.producer.publish(TOPIC, event, key=event.get("sid"))
        logger.info("Published payment_event [sid=%s]", event["sid"])
        return ok

    def close(self):
        self.producer.close()
