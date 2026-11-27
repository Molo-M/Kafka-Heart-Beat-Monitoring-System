"""
Shared settings for the heartbeat pipeline.

All values are read from the .env file in the project root, so every
script (generator, producer, consumer, tests) uses the same settings.
The second argument of os.getenv() is the default used if a value is missing.
"""

import os

from dotenv import load_dotenv

# Find the .env file (searching upward from this file) and load its values
load_dotenv()

# --- Kafka ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "heartbeat-data")

# --- PostgreSQL ---
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "heartbeat_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "heartbeat_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "heartbeat_pass")

# --- Data generator ---
NUM_CUSTOMERS = int(os.getenv("NUM_CUSTOMERS", "10"))
SEND_INTERVAL_SECONDS = float(os.getenv("SEND_INTERVAL_SECONDS", "1"))

# --- Email alerts ---
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "465"))
ALERT_EMAIL_SENDER = os.getenv("ALERT_EMAIL_SENDER", "")
# Gmail shows App Passwords with spaces ("abcd efgh ..."); remove them
ALERT_EMAIL_PASSWORD = os.getenv("ALERT_EMAIL_PASSWORD", "").replace(" ", "")
ALERT_EMAIL_RECIPIENT = os.getenv("ALERT_EMAIL_RECIPIENT", "")

# --- Retries (like Airflow's "retries" and "retry_delay") ---
# How many times to retry after the first failure before sending an alert
PIPELINE_RETRIES = int(os.getenv("PIPELINE_RETRIES", "3"))
# Seconds to wait between attempts
PIPELINE_RETRY_DELAY_SECONDS = float(os.getenv("PIPELINE_RETRY_DELAY_SECONDS", "10"))