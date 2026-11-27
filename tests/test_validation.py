"""
Unit tests for the processing logic.

These tests do not need Kafka or PostgreSQL: they check the functions
that generate, validate and classify heart rate readings.

Run from the project root:
    pytest tests/test_validation.py -v
"""

import json
from datetime import timezone

import pytest

import data_generator
from consumer import is_anomaly, parse_and_validate


def encode(data):
    """Turn a dictionary into bytes, the same way the producer does."""
    return json.dumps(data).encode("utf-8")


VALID_READING = {
    "customer_id": "CUST_001",
    "timestamp": "2026-11-25T10:00:00+00:00",
    "heart_rate": 72,
}


# ---------------------------------------------------------------------
# Valid messages
# ---------------------------------------------------------------------

def test_valid_reading_is_accepted():
    reading = parse_and_validate(encode(VALID_READING))

    assert reading["customer_id"] == "CUST_001"
    assert reading["heart_rate"] == 72
    assert reading["recorded_at"].year == 2026


def test_timestamp_without_timezone_is_treated_as_utc():
    data = {**VALID_READING, "timestamp": "2026-11-25T10:00:00"}

    reading = parse_and_validate(encode(data))

    assert reading["recorded_at"].tzinfo == timezone.utc


# ---------------------------------------------------------------------
# Invalid messages: each must be rejected with the right reason
# ---------------------------------------------------------------------

@pytest.mark.parametrize(
    "raw_message, expected_reason",
    [
        (b"hello heartbeat", "not valid JSON"),
        (b"\xff\xfe", "not valid JSON"),
        (encode([1, 2, 3]), "JSON is not an object"),
        (encode({"timestamp": "2026-11-25T10:00:00+00:00", "heart_rate": 72}),
         "missing field 'customer_id'"),
        (encode({"customer_id": "CUST_001", "heart_rate": 72}),
         "missing field 'timestamp'"),
        (encode({"customer_id": "CUST_001", "timestamp": "2026-11-25T10:00:00+00:00"}),
         "missing field 'heart_rate'"),
        (encode({**VALID_READING, "customer_id": ""}), "non-empty string"),
        (encode({**VALID_READING, "customer_id": 123}), "non-empty string"),
        (encode({**VALID_READING, "customer_id": "C" * 21}), "too long"),
        (encode({**VALID_READING, "timestamp": "yesterday"}), "not a valid ISO 8601"),
        (encode({**VALID_READING, "timestamp": None}), "not a valid ISO 8601"),
        (encode({**VALID_READING, "heart_rate": "72"}), "whole number"),
        (encode({**VALID_READING, "heart_rate": 72.5}), "whole number"),
        (encode({**VALID_READING, "heart_rate": True}), "whole number"),
        (encode({**VALID_READING, "heart_rate": 0}), "outside the valid range"),
        (encode({**VALID_READING, "heart_rate": -10}), "outside the valid range"),
        (encode({**VALID_READING, "heart_rate": 500}), "outside the valid range"),
    ],
    ids=[
        "plain text", "not UTF-8", "JSON list", "missing customer_id",
        "missing timestamp", "missing heart_rate", "empty customer_id",
        "numeric customer_id", "customer_id too long", "unreadable timestamp",
        "null timestamp", "heart_rate as text", "heart_rate decimal",
        "heart_rate boolean", "heart_rate zero", "heart_rate negative",
        "heart_rate 500",
    ],
)
def test_invalid_messages_are_rejected(raw_message, expected_reason):
    with pytest.raises(ValueError, match=expected_reason):
        parse_and_validate(raw_message)


# ---------------------------------------------------------------------
# Anomaly detection, including the exact boundaries
# ---------------------------------------------------------------------

@pytest.mark.parametrize(
    "heart_rate, expected",
    [
        (30, True),    # lowest valid value: very low
        (49, True),    # just below normal
        (50, False),   # lowest normal value
        (72, False),   # typical resting rate
        (120, False),  # highest normal value
        (121, True),   # just above normal
        (220, True),   # highest valid value: very high
    ],
)
def test_is_anomaly(heart_rate, expected):
    assert is_anomaly(heart_rate) == expected


# ---------------------------------------------------------------------
# Data generator
# ---------------------------------------------------------------------

def test_generator_creates_requested_customers():
    customers = data_generator.create_customers(5)

    assert list(customers) == ["CUST_001", "CUST_002", "CUST_003", "CUST_004", "CUST_005"]
    for resting_rate in customers.values():
        assert 62 <= resting_rate <= 80


def test_generated_reading_has_required_fields():
    reading = data_generator.generate_reading("CUST_001", 70)

    assert set(reading) == {"customer_id", "timestamp", "heart_rate"}
    assert reading["customer_id"] == "CUST_001"


def test_generated_readings_always_pass_validation():
    """Everything the generator produces must be accepted by the consumer."""
    customers = data_generator.create_customers(10)

    for _ in range(200):
        for customer_id, resting_rate in customers.items():
            reading = data_generator.generate_reading(customer_id, resting_rate)
            parse_and_validate(encode(reading))  # raises if invalid


def test_generator_produces_both_normal_and_anomalous_readings():
    heart_rates = [
        data_generator.generate_reading("CUST_001", 70)["heart_rate"]
        for _ in range(2000)
    ]
    anomalies = [rate for rate in heart_rates if is_anomaly(rate)]

    # About 5% should be anomalies; allow a wide margin for randomness
    assert 0.01 < len(anomalies) / len(heart_rates) < 0.10
