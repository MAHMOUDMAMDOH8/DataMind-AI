"""
Billing System runner — publishes call CDRs and SMS events.

Usage:
    python -m runners.run_billing --rate 5 --bootstrap-servers localhost:9092
"""

import argparse
import asyncio
import random
import signal
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from billing_system.call_producer import CallCDRProducer
from billing_system.sms_producer import SMSProducer

running = True


def signal_handler(sig, frame):
    global running
    running = False


async def loop(events_per_sec, allow_data_issues):
    call_prod = CallCDRProducer()
    sms_prod = SMSProducer()
    call_prod.load_data()
    sms_prod.load_data()
    delay = 1.0 / events_per_sec
    while running:
        if random.random() < 0.55:
            call_prod.publish_cdr(allow_data_issues)
        else:
            sms_prod.publish_sms(allow_data_issues)
        await asyncio.sleep(delay)
    call_prod.close()
    sms_prod.close()


def main():
    parser = argparse.ArgumentParser(description="Billing System — CDR & SMS stream")
    parser.add_argument("--rate", type=int, default=5)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--clean", action="store_true")
    args = parser.parse_args()
    signal.signal(signal.SIGINT, signal_handler)
    asyncio.run(loop(args.rate, not args.clean))


if __name__ == "__main__":
    main()
