"""
Billing System — Call CDR producer.

Publishes Call Detail Records to the calls_topic Kafka topic.
"""

import logging
from typing import List, Optional

from shared.kafka_client import SourceSystemProducer
from shared.event_factory import create_call_cdr
from shared.dimensions import load_customers, load_cell_sites

from billing_system.config import CALLS_TOPIC

logger = logging.getLogger("billing_call_producer")


class CallCDRProducer:
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.producer = SourceSystemProducer(bootstrap_servers, client_id="billing-call-system")
        self.customers: Optional[List] = None
        self.cell_sites: Optional[List] = None

    def load_data(self):
        self.customers = load_customers()
        self.cell_sites = load_cell_sites()

    def publish_cdr(self, allow_data_issues: bool = True) -> bool:
        event = create_call_cdr(self.customers, self.cell_sites, allow_data_issues=allow_data_issues)
        ok = self.producer.publish(CALLS_TOPIC, event, key=event.get("sid"))
        logger.info("Published call_cdr [sid=%s]", event["sid"])
        return ok

    def close(self):
        self.producer.close()
