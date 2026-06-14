"""
Network Monitoring System runner — publishes data usage, metrics, and QoS reports.

Usage:
    python -m runners.run_network --rate 3 --bootstrap-servers localhost:9092
"""

import argparse
import asyncio
import random
import signal
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from network_system.data_usage_producer import DataUsageProducer
from network_system.network_metrics_producer import NetworkMetricsProducer

running = True


def signal_handler(sig, frame):
    global running
    running = False


async def loop(events_per_sec, allow_data_issues):
    data_prod = DataUsageProducer()
    metrics_prod = NetworkMetricsProducer()
    data_prod.load_data()
    metrics_prod.load_data()
    delay = 1.0 / events_per_sec
    while running:
        r = random.random()
        if r < 0.6:
            data_prod.publish_data_session(allow_data_issues)
        elif r < 0.85:
            metrics_prod.publish_metric()
        else:
            metrics_prod.publish_qos_report()
        await asyncio.sleep(delay)
    data_prod.close()
    metrics_prod.close()


def main():
    parser = argparse.ArgumentParser(description="NMS — Data usage & metrics stream")
    parser.add_argument("--rate", type=int, default=3)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--clean", action="store_true")
    args = parser.parse_args()
    signal.signal(signal.SIGINT, signal_handler)
    asyncio.run(loop(args.rate, not args.clean))


if __name__ == "__main__":
    main()
