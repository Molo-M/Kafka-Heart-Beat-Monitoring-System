# Real-Time Customer Heart Beat Monitoring System

## Proposed Repo Structure

```
heartbeat-monitoring/
├── docker-compose.yml      (Step 2)
├── requirements.txt
├── .gitignore
├── README.md               (Step 8)
├── sql/
│   └── schema.sql          (Step 3)
├── src/
│   ├── config.py           (shared settings)
│   ├── data_generator.py   (Step 4)
│   ├── producer.py         (Step 5)
│   └── consumer.py         (Step 6)
├── tests/                  (Step 7)
└── docs/
    └── images/             (diagrams and screenshots)
```

## How the generator works

Each customer has their own resting heart rate. `create_customers()` gives every fake customer a resting rate between 62 and 80 bpm. Real people differ, so customers don't all hover around the same number, which makes the data look realistic in queries and dashboards.

Normal readings vary a little around that rate. `random.gauss(resting_rate, 4)` picks a number from a bell curve centred on the customer's resting rate. Most values land within a few beats of it, and bigger jumps are rare, just like a real heart rate. Plain `random.randint(60, 100)` would jump around unrealistically from second to second.

About 5% of readings are anomalies. These are either too high (130 to 190 bpm) or too low (35 to 45 bpm). They're still physically possible values, which is the point: the consumer has to flag them, not reject them.
