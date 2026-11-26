"""
End-to-end test of the full pipeline:
    test messages -> Kafka topic -> consumer -> PostgreSQL

It sends a set of different readings (normal, too high, too low and
several kinds of invalid data) to Kafka, waits for the consumer to
process them, then checks what was stored in the database.

Requirements:
  - Docker containers running (docker compose up -d)
  - The consumer running in another window (python src/consumer.py)

Run from the project root:
    pytest tests/test_pipeline.py -v

All test rows use customer IDs starting with a unique prefix for this
run, and are deleted from the database at the end.
"""

import json
import time
import uuid
from datetime import datetime, timezone

import psycopg2
import pytest
from kafka import KafkaProducer
from kafka.errors import KafkaError

import config
from consumer import connect_to_database

# Unique prefix for this test run, e.g. "T3fa9c1_"
RUN_PREFIX = f"T{uuid.uuid4().hex[:6]}_"

# How long to wait for the consumer to process the test messages
WAIT_SECONDS = 30


def make_id(name):
    """Customer ID for one test case, e.g. 'T3fa9c1_NORMAL'."""
    return RUN_PREFIX + name


def now():
    return datetime.now(timezone.utc).isoformat()


# Readings that should be STORED: customer ID -> (heart rate, expected is_anomaly)
EXPECTED_STORED = {
    make_id("NORMAL"): (72, False),
    make_id("HIGH"): (165, True),
    make_id("LOW"): (40, True),
}

# Messages that should be REJECTED: each one is the raw bytes sent to Kafka
INVALID_MESSAGES = {
    "impossible heart rate": {"customer_id": make_id("IMPOSSIBLE"), "timestamp": now(), "heart_rate": 500},
    "missing heart_rate": {"customer_id": make_id("NOFIELD"), "timestamp": now()},
    "bad timestamp": {"customer_id": make_id("BADTIME"), "timestamp": "not-a-date", "heart_rate": 70},
    "heart rate as text": {"customer_id": make_id("TEXTRATE"), "timestamp": now(), "heart_rate": "70"},
}
INVALID_CUSTOMER_IDS = [message["customer_id"] for message in INVALID_MESSAGES.values()]


def encode(data):
    return json.dumps(data).encode("utf-8")


@pytest.fixture(scope="module")
def db_connection():
    """Connect to PostgreSQL; delete this run's test rows afterwards."""
    try:
        connection = connect_to_database()
    except psycopg2.OperationalError as error:
        pytest.skip(f"PostgreSQL not available: {error}")
    connection.autocommit = True

    yield connection

    with connection.cursor() as cursor:
        cursor.execute(
            "DELETE FROM heartbeat_readings WHERE starts_with(customer_id, %s)",
            (RUN_PREFIX,),
        )
    connection.close()


@pytest.fixture(scope="module")
def stored_rows(db_connection):
    """
    Send all test messages to Kafka, wait for the consumer to store them,
    and return the stored rows as {customer_id: (heart_rate, is_anomaly)}.
    """
    try:
        producer = KafkaProducer(bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS)
    except KafkaError:
        pytest.skip(f"Kafka not available at {config.KAFKA_BOOTSTRAP_SERVERS}")

    # Invalid messages first, valid ones last. With one partition the consumer
    # handles messages in order, so once the valid rows appear in the database,
    # every invalid message before them has already been processed.
    producer.send(config.KAFKA_TOPIC, value=b"this is not json")
    for message in INVALID_MESSAGES.values():
        producer.send(config.KAFKA_TOPIC, value=encode(message))
    for customer_id, (heart_rate, _) in EXPECTED_STORED.items():
        reading = {"customer_id": customer_id, "timestamp": now(), "heart_rate": heart_rate}
        producer.send(config.KAFKA_TOPIC, value=encode(reading))
    producer.flush()
    producer.close()

    # Check the database every second until the valid rows arrive
    query = """
        SELECT customer_id, heart_rate, is_anomaly
        FROM heartbeat_readings
        WHERE starts_with(customer_id, %s)
    """
    deadline = time.time() + WAIT_SECONDS
    while True:
        with db_connection.cursor() as cursor:
            cursor.execute(query, (RUN_PREFIX,))
            rows = {customer_id: (rate, anomaly) for customer_id, rate, anomaly in cursor.fetchall()}

        if all(customer_id in rows for customer_id in EXPECTED_STORED):
            return rows
        if time.time() > deadline:
            pytest.fail(
                f"Test readings did not reach the database within {WAIT_SECONDS} seconds. "
                "Is the consumer running? (python src/consumer.py)"
            )
        time.sleep(1)


def test_normal_reading_is_stored_without_flag(stored_rows):
    assert stored_rows[make_id("NORMAL")] == (72, False)


def test_high_reading_is_stored_as_anomaly(stored_rows):
    assert stored_rows[make_id("HIGH")] == (165, True)


def test_low_reading_is_stored_as_anomaly(stored_rows):
    assert stored_rows[make_id("LOW")] == (40, True)


def test_invalid_messages_are_not_stored(stored_rows):
    for customer_id in INVALID_CUSTOMER_IDS:
        assert customer_id not in stored_rows


def test_only_expected_rows_are_stored(stored_rows):
    assert set(stored_rows) == set(EXPECTED_STORED)
