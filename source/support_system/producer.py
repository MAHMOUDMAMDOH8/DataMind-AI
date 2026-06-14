"""
Customer Support System — Ticket & complaint event producer.

Publishes support ticket, complaint, and resolution events
to the tickets_topic Kafka topic.
"""

import logging
from typing import List, Optional

from shared.kafka_client import SourceSystemProducer
from shared.event_factory import create_ticket_created, create_complaint_filed, create_ticket_resolved
from shared.dimensions import load_customers

from support_system.config import TOPIC

logger = logging.getLogger("support_producer")


class SupportProducer:
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.producer = SourceSystemProducer(bootstrap_servers, client_id="support-system")
        self.customers: Optional[List] = None

    def load_data(self):
        self.customers = load_customers()

    def publish_ticket_created(self) -> bool:
        event = create_ticket_created(self.customers)
        ok = self.producer.publish(TOPIC, event, key=event.get("sid"))
        logger.info("Published ticket_created [sid=%s]", event["sid"])
        return ok

    def publish_complaint(self) -> bool:
        event = create_complaint_filed(self.customers)
        ok = self.producer.publish(TOPIC, event, key=event.get("sid"))
        logger.info("Published complaint_filed [sid=%s]", event["sid"])
        return ok

    def publish_resolution(self) -> bool:
        event = create_ticket_resolved(self.customers)
        ok = self.producer.publish(TOPIC, event, key=event.get("sid"))
        logger.info("Published ticket_resolved [sid=%s]", event["sid"])
        return ok

    def close(self):
        self.producer.close()
