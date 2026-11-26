"""
Kafka producer for heart rate readings.

Uses the data generator to create readings for every customer and sends
each one to the Kafka topic as a JSON message, continuously, to simulate
a real-time feed from heart rate monitors.

Run from the project root:
    python src/producer.py
Stop with Ctrl+C.
"""

import json
import sys
import time
import traceback

from kafka import KafkaProducer
from kafka.errors import KafkaError

import config
from alerts import send_alert
from data_generator import create_customers, generate_reading


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


def main():
    try:
        producer = create_producer()
    except KafkaError as error:
        print(f"Could not connect to Kafka at {config.KAFKA_BOOTSTRAP_SERVERS}.")
        print("Is Docker running? Check with: docker compose ps")
        send_alert(
            "Producer could not start",
            f"Could not connect to Kafka at {config.KAFKA_BOOTSTRAP_SERVERS}:\n{error!r}",
        )
        sys.exit(1)

    customers = create_customers(config.NUM_CUSTOMERS)
    print(f"Connected to Kafka. Sending to topic '{config.KAFKA_TOPIC}'.")
    print(f"Simulating {len(customers)} customers. Press Ctrl+C to stop.\n")

    sent_count = 0
    failed = False
    try:
        while True:
            results = []
            for customer_id, resting_rate in customers.items():
                reading = generate_reading(customer_id, resting_rate)

                # The customer ID is used as the message key
                key, value = to_message(reading)
                results.append(producer.send(config.KAFKA_TOPIC, key=key, value=value))
                sent_count += 1
                print(f"Sent #{sent_count}: {reading}")

            # send() works in the background; flush() waits until this
            # round of messages has actually reached Kafka (or failed)
            producer.flush()

            # Raise an error if any message in this round could not be delivered
            for result in results:
                result.get()

            time.sleep(config.SEND_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print(f"\nStopping producer. Total messages sent: {sent_count}")

    except Exception as error:
        # Anything else is a real failure (e.g. Kafka became unreachable)
        failed = True
        print(f"\nPRODUCER FAILED: {error!r}")
        send_alert(
            "Producer failed",
            f"The producer stopped because of an error:\n{error!r}\n\n"
            f"Messages sent before the failure: {sent_count}\n\n"
            f"Details:\n{traceback.format_exc()}",
        )

    finally:
        # Deliver anything still waiting (giving up after 10 seconds if
        # Kafka is unreachable), then disconnect
        try:
            producer.flush(timeout=10)
        except KafkaError:
            print("Some messages could not be delivered before shutdown.")
        producer.close(timeout=10)

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()