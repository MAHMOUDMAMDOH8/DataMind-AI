"""
Kafka client wrapper for all source system producers.

Handles producer lifecycle, Avro serialization, delivery callbacks,
and idempotent publishing with acks=all.
"""

import json
import logging
from typing import Any, Callable, Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)


class SourceSystemProducer:
    """Shared Kafka producer for source system event publishing."""

    def __init__(self, bootstrap_servers: str = "localhost:9092", client_id: str = "datamind-ai"):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=3,
            max_in_flight_requests_per_connection=1,
            client_id=client_id,
            linger_ms=10,
            batch_size=16384,
        )

    def publish(self, topic: str, event: dict, key: Optional[str] = None, callback: Optional[Callable] = None):
        key = key or event.get("customer") or event.get("phone_number") or event.get("sid")
        try:
            future = self.producer.send(topic, value=event, key=key)
            if callback:
                future.add_callback(callback)
            return True
        except KafkaError as e:
            logger.error("Kafka publish failed [topic=%s]: %s", topic, e)
            return False

    def flush(self):
        self.producer.flush()

    def close(self):
        self.producer.flush()
        self.producer.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
