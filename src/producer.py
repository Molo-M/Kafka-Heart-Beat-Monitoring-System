"""
Kafka producer for heart rate readings.

Uses the data generator to create readings for every customer and sends
each one to the Kafka topic as a JSON message, continuously, to simulate
a real-time feed from heart rate monitors.

If something goes wrong (e.g. Kafka becomes unreachable), it retries
PIPELINE_RETRIES times before giving up and sending an alert email.

Run from the project root:
    python src/producer.py
Stop with Ctrl+C.
"""

import json
import sys
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError

import config
from data_generator import create_customers, generate_reading
from retry import run_with_retries


def create_producer():
    """Connect to Kafka and return a producer."""
    return KafkaProducer(bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS)


def to_message(reading):
    """
    Convert a reading into the key and value Kafka will store.

    Kafka only stores bytes, so:
      - key   : the customer ID string -> bytes
      - value : the reading dictionary -> JSON text -> bytes
    """
    key = reading["customer_id"].encode("utf-8")
    value = json.dumps(reading).encode("utf-8")
    return key, value


def run_producer(customers, stats):
    """
    One attempt: connect, then send readings until an error happens.

    stats is a dictionary of counters, shared across attempts so the
    totals keep growing after a retry.
    """
    producer = create_producer()
    print(f"Connected to Kafka. Sending to topic '{config.KAFKA_TOPIC}'.")
    print(f"Simulating {len(customers)} customers. Press Ctrl+C to stop.\n")

    try:
        while True:
            results = []
            for customer_id, resting_rate in customers.items():
                reading = generate_reading(customer_id, resting_rate)

                # The customer ID is used as the message key
                key, value = to_message(reading)
                results.append(producer.send(config.KAFKA_TOPIC, key=key, value=value))
                stats["sent"] += 1
                print(f"Sent #{stats['sent']}: {reading}")

            # send() works in the background; flush() waits until this
            # round of messages has actually reached Kafka (or failed)
            producer.flush()

            # Raise an error if any message in this round could not be delivered
            for result in results:
                result.get()
                stats["delivered"] += 1

            time.sleep(config.SEND_INTERVAL_SECONDS)

    finally:
        # Deliver anything still waiting (giving up after 10 seconds if
        # Kafka is unreachable), then disconnect
        try:
            producer.flush(timeout=10)
        except KafkaError:
            print("Some messages could not be delivered before shutdown.")
        producer.close(timeout=10)


def main():
    # Created once, so the same customers continue after a retry
    customers = create_customers(config.NUM_CUSTOMERS)
    stats = {"sent": 0, "delivered": 0}
    succeeded = True

    try:
        succeeded = run_with_retries(
            "Producer",
            attempt=lambda: run_producer(customers, stats),
            get_progress=lambda: stats["delivered"],
        )
    except KeyboardInterrupt:
        print("\nStopping producer.")
    finally:
        print(f"Total messages delivered to Kafka: {stats['delivered']}")

    if not succeeded:
        sys.exit(1)


if __name__ == "__main__":
    main()