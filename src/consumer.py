"""
Kafka consumer for heart rate readings.

Reads messages from the Kafka topic and, for each one:
  1. Validates it (correct JSON, all fields present, sensible values).
  2. Flags it as an anomaly if the heart rate is outside the normal range.
  3. Inserts valid readings into PostgreSQL; invalid ones are logged and skipped.

If something goes wrong (e.g. the database goes down), it retries
PIPELINE_RETRIES times before giving up and sending an alert email.

Run from the project root:
    python src/consumer.py
Stop with Ctrl+C.
"""

import json
import sys
from datetime import datetime, timezone

import psycopg2
from kafka import KafkaConsumer

import config
from retry import run_with_retries

# --- Validation rules ---
# Readings outside this range are impossible or broken data -> rejected
MIN_VALID_HEART_RATE = 30
MAX_VALID_HEART_RATE = 220

# Readings outside this range are possible but worrying -> stored as anomalies
NORMAL_MIN_HEART_RATE = 50
NORMAL_MAX_HEART_RATE = 120

# Must match VARCHAR(20) in sql/schema.sql
MAX_CUSTOMER_ID_LENGTH = 20

INSERT_SQL = """
    INSERT INTO heartbeat_readings (customer_id, recorded_at, heart_rate, is_anomaly)
    VALUES (%s, %s, %s, %s)
"""


def parse_and_validate(raw_value):
    """
    Turn a raw Kafka message (bytes) into a clean reading.

    Returns a dictionary with customer_id, recorded_at (a datetime) and
    heart_rate. Raises ValueError with a short reason if the message is invalid.
    """
    # 1. Must be valid JSON describing an object (a dictionary)
    try:
        data = json.loads(raw_value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise ValueError("not valid JSON")
    if not isinstance(data, dict):
        raise ValueError("JSON is not an object")

    # 2. All three fields must be present
    for field in ("customer_id", "timestamp", "heart_rate"):
        if field not in data:
            raise ValueError(f"missing field '{field}'")

    # 3. customer_id: a non-empty string that fits the database column
    customer_id = data["customer_id"]
    if not isinstance(customer_id, str) or not customer_id.strip():
        raise ValueError("customer_id must be a non-empty string")
    if len(customer_id) > MAX_CUSTOMER_ID_LENGTH:
        raise ValueError("customer_id is too long")

    # 4. timestamp: must be a readable ISO 8601 date and time
    try:
        recorded_at = datetime.fromisoformat(data["timestamp"])
    except (TypeError, ValueError):
        raise ValueError("timestamp is not a valid ISO 8601 date")
    if recorded_at.tzinfo is None:
        # No time zone given: assume UTC, like the generator uses
        recorded_at = recorded_at.replace(tzinfo=timezone.utc)

    # 5. heart_rate: a whole number in the physically possible range
    #    (bool is excluded because Python treats True/False as 1/0)
    heart_rate = data["heart_rate"]
    if isinstance(heart_rate, bool) or not isinstance(heart_rate, int):
        raise ValueError("heart_rate must be a whole number")
    if not MIN_VALID_HEART_RATE <= heart_rate <= MAX_VALID_HEART_RATE:
        raise ValueError(f"heart_rate {heart_rate} is outside the valid range")

    return {
        "customer_id": customer_id,
        "recorded_at": recorded_at,
        "heart_rate": heart_rate,
    }


def is_anomaly(heart_rate):
    """Return True if a valid heart rate is outside the normal range."""
    return heart_rate < NORMAL_MIN_HEART_RATE or heart_rate > NORMAL_MAX_HEART_RATE


def connect_to_database():
    """Open a connection to PostgreSQL using the settings from .env."""
    return psycopg2.connect(
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT,
        dbname=config.POSTGRES_DB,
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
    )


def create_consumer():
    """Connect to Kafka and return a consumer subscribed to the topic."""
    return KafkaConsumer(
        config.KAFKA_TOPIC,
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        # Consumers in the same group share the work and remember their position
        group_id="heartbeat-consumer-group",
        # First run of this group: start from the oldest message in the topic
        auto_offset_reset="earliest",
        # We tell Kafka a message is done ourselves, only after it is handled
        enable_auto_commit=False,
    )


def run_consumer(stats):
    """
    One attempt: connect, then process messages until an error happens.

    stats is a dictionary of counters, shared across attempts so the
    totals keep growing after a retry.
    """
    db_connection = connect_to_database()
    try:
        consumer = create_consumer()
    except Exception:
        db_connection.close()
        raise

    cursor = db_connection.cursor()
    print(f"Connected. Listening to topic '{config.KAFKA_TOPIC}'. Press Ctrl+C to stop.\n")

    try:
        # This loop waits for new messages and never ends on its own
        for message in consumer:
            try:
                reading = parse_and_validate(message.value)
            except ValueError as reason:
                stats["rejected"] += 1
                print(f"REJECTED ({reason}): {message.value!r}")
            else:
                anomaly = is_anomaly(reading["heart_rate"])
                try:
                    cursor.execute(
                        INSERT_SQL,
                        (
                            reading["customer_id"],
                            reading["recorded_at"],
                            reading["heart_rate"],
                            anomaly,
                        ),
                    )
                    db_connection.commit()
                except (psycopg2.DataError, psycopg2.IntegrityError) as error:
                    # The database refused the row (e.g. the CHECK constraint)
                    db_connection.rollback()
                    stats["rejected"] += 1
                    print(f"REJECTED by database ({error.pgerror}): {reading}")
                else:
                    stats["inserted"] += 1
                    label = "ANOMALY " if anomaly else "Inserted"
                    if anomaly:
                        stats["anomalies"] += 1
                    print(
                        f"{label} | {reading['customer_id']} | "
                        f"{reading['heart_rate']} bpm | {reading['recorded_at']}"
                    )

            # The message has been handled (stored or rejected), so tell
            # Kafka not to deliver it to this group again
            consumer.commit()

    # Any other error (e.g. the database went down) leaves this function.
    # The current message was NOT committed, so Kafka will deliver it
    # again on the next attempt: nothing is lost.
    finally:
        consumer.close()
        cursor.close()
        db_connection.close()


def main():
    stats = {"inserted": 0, "anomalies": 0, "rejected": 0}
    succeeded = True

    try:
        succeeded = run_with_retries(
            "Consumer",
            attempt=lambda: run_consumer(stats),
            get_progress=lambda: stats["inserted"] + stats["rejected"],
        )
    except KeyboardInterrupt:
        print("\nStopping consumer.")
    finally:
        print(
            f"Summary: {stats['inserted']} inserted "
            f"({stats['anomalies']} anomalies), {stats['rejected']} rejected."
        )

    if not succeeded:
        sys.exit(1)


if __name__ == "__main__":
    main()