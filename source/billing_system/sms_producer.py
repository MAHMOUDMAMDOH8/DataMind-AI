"""
Billing System — SMS event producer.

Publishes SMS events to the sms_topic Kafka topic.
"""

import logging
from typing import List, Optional

from shared.kafka_client import SourceSystemProducer
from shared.event_factory import create_sms_event
from shared.dimensions import load_customers, load_cell_sites

from billing_system.config import SMS_TOPIC

logger = logging.getLogger("billing_sms_producer")


class SMSProducer:
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.producer = SourceSystemProducer(bootstrap_servers, client_id="billing-sms-system")
        self.customers: Optional[List] = None
        self.cell_sites: Optional[List] = None

    def load_data(self):
        self.customers = load_customers()
        self.cell_sites = load_cell_sites()

    def publish_sms(self, allow_data_issues: bool = True) -> bool:
        event = create_sms_event(self.customers, self.cell_sites, allow_data_issues=allow_data_issues)
        ok = self.producer.publish(SMS_TOPIC, event, key=event.get("sid"))
        logger.info("Published sms_event [sid=%s]", event["sid"])
        return ok

    def close(self):
        self.producer.close()
