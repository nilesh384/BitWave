import json
import os
import sys
import time
import threading
import psycopg2

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from windowed_sketch_engine import WindowedSketchEngine

from kafka import KafkaConsumer

# 60-second buckets, keep up to 60 buckets (1 hour of history)
engine = WindowedSketchEngine(bucket_seconds=60, max_window_buckets=60)

consumer = KafkaConsumer(
    "user-events",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="latest",
)

DB_CONFIG = dict(
    host="localhost",
    port=5433,
    user="cip_user",
    password="cip_password",
    dbname="cip_db",
)


def write_snapshot(window_seconds, result):
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO fact_realtime_metrics
                    (window_seconds, unique_users_estimate, top_items)
                VALUES (%s, %s, %s)
                """,
                (
                    window_seconds,
                    result["unique_users_estimate"],
                    json.dumps(result["top_items"]),
                ),
            )
        conn.commit()
    finally:
        conn.close()


def snapshot_loop():
    """Runs on a separate thread. Every 10s, query a 5-min window and persist it."""
    while True:
        time.sleep(10)
        result = engine.query_window(window_seconds=300)
        write_snapshot(300, result)
        print(f"\n[snapshot written] unique_users={result['unique_users_estimate']:.0f}  "
              f"top_items={result['top_items']}")


threading.Thread(target=snapshot_loop, daemon=True).start()

print("Consumer started. Listening for events on 'user-events'...\n")

for message in consumer:
    event = message.value
    engine.record_event(event["user_id"], event["item_id"], event["timestamp"])