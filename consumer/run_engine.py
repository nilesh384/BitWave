import json
import sys
import os
import time
import threading

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "engine"))
from windowed_engine import WindowedSketchEngine

from kafka import KafkaConsumer

# 60-second buckets, keep up to 60 buckets (1 hour of history)
engine = WindowedSketchEngine(bucket_seconds=60, max_window_buckets=60)

consumer = KafkaConsumer(
    "user-events",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="latest",   # only read NEW events from now on, ignore old backlog
)


def print_snapshot_periodically():
    """Runs on a separate thread — prints current window stats every 10s,
    independent of how fast events are arriving."""
    while True:
        time.sleep(10)
        result_5min = engine.query_window(window_seconds=300)
        print("\n--- Snapshot (last 5 min) ---")
        print(f"Unique users : {result_5min['unique_users_estimate']:.0f}")
        print(f"Top items    : {result_5min['top_items']}")


# start the snapshot printer in the background
threading.Thread(target=print_snapshot_periodically, daemon=True).start()

print("Consumer started. Listening for events on 'user-events'...\n")

for message in consumer:
    event = message.value
    engine.record_event(event["user_id"], event["item_id"], event["timestamp"])