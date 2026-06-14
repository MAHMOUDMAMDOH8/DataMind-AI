"""
Stream Runner — Source Systems Kafka Publisher

Publishes events from all 7 enterprise source systems to their
designated Kafka topics, simulating production telecom traffic.
"""

import asyncio
import json
import random
import signal
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from kafka import KafkaProducer
from kafka.errors import KafkaError

from shared.event_base import load_customers, load_cell_sites

from crm_system.generator import generate_customer_registration, generate_customer_profile_update
from billing_system.generator import generate_call_cdr, generate_sms_event
from network_system.generator import generate_data_session_event, generate_network_metric, generate_qos_report
from payment_gateway.generator import generate_payment_event
from recharge_platform.generator import generate_recharge_event
from roaming_system.generator import generate_roaming_event
from support_system.generator import generate_ticket_created, generate_complaint_filed, generate_ticket_resolved


# Topic mapping — mirrors arch/03-source-systems.md
EVENT_TOPICS = {
    # CRM System
    "customer_registration": "customer_topic",
    "customer_profile_update": "customer_topic",
    # Billing System
    "call": "calls_topic",
    "sms": "sms_topic",
    # Network Monitoring System
    "data_usage": "data_usage_topic",
    "network_metric": "network_metrics_topic",
    "qos_report": "network_metrics_topic",
    # Payment Gateway
    "payment": "payments_topic",
    # Recharge Platform
    "recharge": "recharge_topic",
    # Roaming Management
    "roaming": "roaming_topic",
    # Customer Support
    "ticket_created": "tickets_topic",
    "complaint_filed": "tickets_topic",
    "ticket_resolved": "tickets_topic",
}

# Event generation weights (relative frequency per system source)
EVENT_GENERATORS = {
    "customer_registration": (generate_customer_registration, 2),
    "customer_profile_update": (generate_customer_profile_update, 4),
    "call": (generate_call_cdr, 25),
    "sms": (generate_sms_event, 20),
    "data_usage": (generate_data_session_event, 18),
    "network_metric": (generate_network_metric, 5),
    "qos_report": (generate_qos_report, 3),
    "payment": (generate_payment_event, 8),
    "recharge": (generate_recharge_event, 10),
    "roaming": (generate_roaming_event, 3),
    "ticket_created": (generate_ticket_created, 4),
    "complaint_filed": (generate_complaint_filed, 2),
    "ticket_resolved": (generate_ticket_resolved, 3),
}

producer = None
customers = None
cell_sites = None
running = True


def signal_handler(sig, frame):
    global running
    print("\nShutting down gracefully...")
    running = False
    if producer:
        producer.close()


def init_kafka_producer(bootstrap_servers="localhost:9092"):
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        max_in_flight_requests_per_connection=1,
    )


def select_event_type():
    event_types = list(EVENT_GENERATORS.keys())
    weights = [w for _, w in EVENT_GENERATORS.values()]
    return random.choices(event_types, weights=weights, k=1)[0]


def generate_and_publish(event_type, allow_data_issues=True):
    global customers, cell_sites, producer
    gen_func = EVENT_GENERATORS[event_type][0]
    topic = EVENT_TOPICS[event_type]

    try:
        event = gen_func(customers, cell_sites, allow_data_issues=allow_data_issues)
        key = event.get("customer") or event.get("phone_number") or event.get("sid")
        producer.send(topic, value=event, key=key)
        return True
    except Exception as e:
        print(f"Error publishing {event_type}: {e}")
        return False


async def event_loop(events_per_second=10, allow_data_issues=True):
    global customers, cell_sites
    customers = load_customers("DIM_USER.json")
    cell_sites = load_cell_sites("dim_cell_site.json")
    print(f"Loaded {len(customers)} customers and {len(cell_sites)} cell sites")
    print(f"Streaming to Kafka at {events_per_second} events/sec ...")

    delay = 1.0 / events_per_second
    count = 0

    while running:
        event_type = select_event_type()
        generate_and_publish(event_type, allow_data_issues=allow_data_issues)
        count += 1
        if count % 100 == 0:
            print(f"Published {count} events")
        await asyncio.sleep(delay)


def run_stream(events_per_second=10, bootstrap_servers="localhost:9092", allow_data_issues=True):
    global producer
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    print(f"Connecting to Kafka at {bootstrap_servers} ...")
    producer = init_kafka_producer(bootstrap_servers)
    print("Kafka producer initialized")

    try:
        asyncio.run(event_loop(events_per_second, allow_data_issues))
    except KeyboardInterrupt:
        pass
    finally:
        if producer:
            producer.flush()
            producer.close()
        print("Stream stopped.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=int, default=10)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--clean", action="store_true")
    args = parser.parse_args()
    run_stream(args.rate, args.bootstrap_servers, not args.clean)
