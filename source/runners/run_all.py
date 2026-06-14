"""
Master runner — publishes events from all 7 source systems to Kafka.

Simulates a production telecom environment with realistic event distribution.

Usage:
    python -m runners.run_all --rate 20 --duration-seconds 60 --bootstrap-servers localhost:9092
"""

import argparse
import asyncio
import random
import signal
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from crm_system.producer import CRMProducer
from billing_system.call_producer import CallCDRProducer
from billing_system.sms_producer import SMSProducer
from network_system.data_usage_producer import DataUsageProducer
from network_system.network_metrics_producer import NetworkMetricsProducer
from payment_gateway.producer import PaymentProducer
from recharge_platform.producer import RechargeProducer
from roaming_system.producer import RoamingProducer
from support_system.producer import SupportProducer

running = True

# Event weights matching production traffic distribution
PRODUCERS = None


def signal_handler(sig, frame):
    global running
    print("\nShutting down all source systems ...")
    running = False


def weighted_publish(allow_data_issues):
    r = random.random()
    if r < 0.02:
        PRODUCERS["crm"].publish_registration()
    elif r < 0.06:
        PRODUCERS["crm"].publish_profile_update()
    elif r < 0.26:
        PRODUCERS["call"].publish_cdr(allow_data_issues)
    elif r < 0.44:
        PRODUCERS["sms"].publish_sms(allow_data_issues)
    elif r < 0.60:
        PRODUCERS["data_usage"].publish_data_session(allow_data_issues)
    elif r < 0.65:
        PRODUCERS["net_metric"].publish_metric()
    elif r < 0.68:
        PRODUCERS["net_metric"].publish_qos_report()
    elif r < 0.76:
        PRODUCERS["payment"].publish_payment(allow_data_issues)
    elif r < 0.86:
        PRODUCERS["recharge"].publish_recharge(allow_data_issues)
    elif r < 0.89:
        PRODUCERS["roaming"].publish_roaming(allow_data_issues)
    elif r < 0.93:
        PRODUCERS["support"].publish_ticket_created()
    elif r < 0.95:
        PRODUCERS["support"].publish_complaint()
    else:
        PRODUCERS["support"].publish_resolution()


async def run_all(rate, duration_sec, bootstrap_servers, allow_data_issues):
    global PRODUCERS

    print(f"DataMind AI — All Source Systems")
    print(f"Kafka: {bootstrap_servers}  |  Rate: {rate} events/sec  |  Duration: {duration_sec}s")
    print()

    # Initialize all producers
    PRODUCERS = {
        "crm": CRMProducer(bootstrap_servers),
        "call": CallCDRProducer(bootstrap_servers),
        "sms": SMSProducer(bootstrap_servers),
        "data_usage": DataUsageProducer(bootstrap_servers),
        "net_metric": NetworkMetricsProducer(bootstrap_servers),
        "payment": PaymentProducer(bootstrap_servers),
        "recharge": RechargeProducer(bootstrap_servers),
        "roaming": RoamingProducer(bootstrap_servers),
        "support": SupportProducer(bootstrap_servers),
    }

    for name, prod in PRODUCERS.items():
        prod.load_data()
    print(f"Loaded dimension data across {len(PRODUCERS)} producers")

    delay = 1.0 / rate
    start = time.time()
    count = 0

    while running:
        elapsed = time.time() - start
        if elapsed >= duration_sec:
            break
        weighted_publish(allow_data_issues)
        count += 1
        if count % 200 == 0:
            elapsed = time.time() - start
            print(f"  Published {count} events ({count/elapsed:.0f} evt/sec)")
        await asyncio.sleep(delay)

    print(f"\nPublished {count} events total")
    for prod in PRODUCERS.values():
        prod.close()


def main():
    parser = argparse.ArgumentParser(description="DataMind AI — All Source Systems")
    parser.add_argument("--rate", type=int, default=20, help="Events per second")
    parser.add_argument("--duration-seconds", type=int, default=60, help="Run duration")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--clean", action="store_true", help="Generate clean data")
    args = parser.parse_args()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        asyncio.run(run_all(args.rate, args.duration_seconds, args.bootstrap_servers, not args.clean))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
