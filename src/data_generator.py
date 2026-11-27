"""
Synthetic heart rate data generator.

Creates fake customers, each with their own resting heart rate, and
produces realistic readings for them:
  - Most readings stay close to the customer's resting rate.
  - A small share are anomalies (too high or too low), so the consumer
    has something to detect.

Run this file directly to preview the data in the terminal:
    python src/data_generator.py
"""

import random
import time
from datetime import datetime, timezone

import config

# Share of readings that are anomalies (0.05 = 5%)
ANOMALY_PROBABILITY = 0.05


def create_customers(num_customers):
    """
    Create fake customers.

    Returns a dictionary mapping each customer ID to that customer's
    resting heart rate, e.g. {"CUST_001": 68, "CUST_002": 74, ...}
    """
    customers = {}
    for i in range(1, num_customers + 1):
        customer_id = f"CUST_{i:03d}"  # CUST_001, CUST_002, ...
        customers[customer_id] = random.randint(62, 80)
    return customers


def generate_reading(customer_id, resting_rate):
    """
    Generate one heart rate reading for a customer.

    Returns a dictionary with the three fields required by the project:
    customer_id, timestamp and heart_rate.
    """
    if random.random() < ANOMALY_PROBABILITY:
        # Anomaly: half the time too high, half the time too low
        if random.random() < 0.5:
            heart_rate = random.randint(130, 190)
        else:
            heart_rate = random.randint(35, 45)
    else:
        # Normal: vary a little around the customer's resting rate
        heart_rate = round(random.gauss(resting_rate, 4))

    return {
        "customer_id": customer_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "heart_rate": heart_rate,
    }


def main():
    """Print readings for all customers every few seconds until Ctrl+C."""
    customers = create_customers(config.NUM_CUSTOMERS)
    print(f"Created {len(customers)} customers: {customers}")
    print("Generating readings... press Ctrl+C to stop.\n")

    try:
        while True:
            for customer_id, resting_rate in customers.items():
                print(generate_reading(customer_id, resting_rate))
            time.sleep(config.SEND_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
