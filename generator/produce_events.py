import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone
from dotenv import load_dotenv
from azure.eventhub import EventHubProducerClient, EventData


EVENT_TYPES = ["deposit_completed", "withdrawal_completed"]
CHANNELS = ["web", "mobile", "api"]


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def amount_gbp() -> float:
    return round(random.uniform(5.0, 500.0), 2)


def make_user_id(n: int) -> str:
    return f"user_{n:05d}"


def main() -> None:
    load_dotenv()

    conn_str = os.getenv("EVENT_HUB_CONNECTION_STRING")
    hub_name = os.getenv("EVENT_HUB_NAME")

    parser = argparse.ArgumentParser(
        description="Event producer for Azure Event Hubs (financial transactions)"
    )
    parser.add_argument("--connection-string", default=conn_str, help="Event Hub connection string")
    parser.add_argument("--event-hub-name", default=hub_name, help="Event Hub name")
    parser.add_argument("--num-users", type=int, default=500, help="Total distinct users")
    parser.add_argument("--eps", type=int, default=100, help="Events per second")
    args = parser.parse_args()

    if not args.connection_string:
        raise ValueError("EVENT_HUB_CONNECTION_STRING not set in .env or args")
    if not args.event_hub_name:
        raise ValueError("EVENT_HUB_NAME not set in .env or args")

    producer = EventHubProducerClient.from_connection_string(
        conn_str=args.connection_string,
        eventhub_name=args.event_hub_name
    )

    tick = 1.0 / args.eps
    next_emit_at = time.monotonic()
    events_sent = 0
    started_at = time.time()

    print(f"Starting event generation at {args.eps} events/sec...")

    try:
        while True:
            now = time.monotonic()

            if now < next_emit_at:
                time.sleep(0.001)
                continue

            ts = utc_iso()
            event = {
                "event_id": str(uuid.uuid4()),
                "user_id": make_user_id(random.randint(1, args.num_users)),
                "event_time": ts,
                "ingest_time": ts,
                "event_type": random.choice(EVENT_TYPES),
                "amount": amount_gbp(),
                "currency": "GBP",
                "channel": random.choice(CHANNELS),
            }

            producer.send_event(EventData(json.dumps(event)))
            events_sent += 1

            if events_sent % 100 == 0:
                duration = time.time() - started_at
                print(f"Events sent: {events_sent} | Actual rate: {events_sent/duration:.1f} eps")

            next_emit_at += tick
            if now - next_emit_at > 1.0:
                next_emit_at = now + tick

    except KeyboardInterrupt:
        print("\nStopping generator...")
    finally:
        producer.close()
        duration = time.time() - started_at
        print(f"Generator stopped. Total events: {events_sent} | Duration: {duration:.1f}s | Avg rate: {events_sent/duration:.1f} eps")


if __name__ == "__main__":
    main()