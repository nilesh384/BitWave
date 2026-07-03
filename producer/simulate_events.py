import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# fixed pool of fake users so some repeat (needed to see HLL/CMS behave realistically)
NUM_USERS = 500
users = [f"user_{i}" for i in range(NUM_USERS)]

products = ["iphone", "samsung", "pixel", "oneplus", "nokia"]
weights = [50, 30, 15, 4, 1]  # iphone trends highest, nokia rarely

print("Starting producer... Ctrl+C to stop.")
event_count = 0

try:
    while True:
        event = {
            "user_id": random.choice(users),
            "item_id": random.choices(products, weights=weights)[0],
            "timestamp": time.time(),
        }
        producer.send("user-events", value=event)
        event_count += 1

        if event_count % 50 == 0:
            print(f"Sent {event_count} events... latest: {event}")

        time.sleep(random.uniform(0.02, 0.15))  # irregular timing, mimics real traffic
except KeyboardInterrupt:
    print(f"\nStopped. Total events sent: {event_count}")
    producer.flush()
    producer.close()