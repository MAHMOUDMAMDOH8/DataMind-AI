"""
CRM System — Customer Management producer.

Publishes customer registration and profile update events
to the customer_topic Kafka topic.
"""

import logging
from typing import List, Optional

from shared.kafka_client import SourceSystemProducer
from shared.event_factory import create_customer_registration, create_customer_profile_update
from shared.dimensions import load_customers

from crm_system.config import TOPIC

logger = logging.getLogger("crm_producer")


class CRMProducer:
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.producer = SourceSystemProducer(bootstrap_servers, client_id="crm-system")
        self.customers: Optional[List] = None

    def load_data(self):
        self.customers = load_customers()

    def publish_registration(self) -> bool:
        event = create_customer_registration(self.customers)
        ok = self.producer.publish(TOPIC, event)
        logger.info("Published customer_registration [sid=%s]", event["sid"])
        return ok

    def publish_profile_update(self) -> bool:
        event = create_customer_profile_update(self.customers)
        ok = self.producer.publish(TOPIC, event)
        logger.info("Published customer_profile_update [sid=%s]", event["sid"])
        return ok

    def close(self):
        self.producer.close()
