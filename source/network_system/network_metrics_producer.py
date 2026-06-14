"""
Network Monitoring System — Network metrics & QoS producer.

Publishes aggregate network metrics and QoS reports
to the network_metrics_topic Kafka topic.
"""

import logging
from typing import List, Optional

from shared.kafka_client import SourceSystemProducer
from shared.event_factory import create_network_metric, create_qos_report
from shared.dimensions import load_customers, load_cell_sites

from network_system.config import NETWORK_METRICS_TOPIC

logger = logging.getLogger("nms_metrics_producer")


class NetworkMetricsProducer:
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.producer = SourceSystemProducer(bootstrap_servers, client_id="nms-metrics-system")
        self.customers: Optional[List] = None
        self.cell_sites: Optional[List] = None

    def load_data(self):
        self.customers = load_customers()
        self.cell_sites = load_cell_sites()

    def publish_metric(self) -> bool:
        event = create_network_metric(self.customers, self.cell_sites)
        ok = self.producer.publish(NETWORK_METRICS_TOPIC, event, key=event.get("sid"))
        logger.info("Published network_metric [sid=%s]", event["sid"])
        return ok

    def publish_qos_report(self) -> bool:
        event = create_qos_report(self.customers, self.cell_sites)
        ok = self.producer.publish(NETWORK_METRICS_TOPIC, event, key=event.get("sid"))
        logger.info("Published qos_report [sid=%s]", event["sid"])
        return ok

    def close(self):
        self.producer.close()
