"""
Payment & Recharge runner — publishes payment and recharge events.

Usage:
    python -m runners.run_payments --rate 4 --bootstrap-servers localhost:9092
"""

import argparse
import asyncio
import random
import signal
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from payment_gateway.producer import PaymentProducer
from recharge_platform.producer import RechargeProducer

running = True


def signal_handler(sig, frame):
    global running
    running = False


async def loop(events_per_sec, allow_data_issues):
    pay_prod = PaymentProducer()
    rech_prod = RechargeProducer()
    pay_prod.load_data()
    rech_prod.load_data()
    delay = 1.0 / events_per_sec
    while running:
        if random.random() < 0.45:
            pay_prod.publish_payment(allow_data_issues)
        else:
            rech_prod.publish_recharge(allow_data_issues)
        await asyncio.sleep(delay)
    pay_prod.close()
    rech_prod.close()


def main():
    parser = argparse.ArgumentParser(description="Payment & Recharge stream")
    parser.add_argument("--rate", type=int, default=4)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--clean", action="store_true")
    args = parser.parse_args()
    signal.signal(signal.SIGINT, signal_handler)
    asyncio.run(loop(args.rate, not args.clean))


if __name__ == "__main__":
    main()
