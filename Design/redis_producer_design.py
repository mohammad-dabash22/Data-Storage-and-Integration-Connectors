"""
Milestone 2 Design - Redis Producer Script
Reads records from Labels.csv, serializes each row to JSON,
and publishes to the Pub/Sub topic 'Image2Redis-dabash'.
Each record uses a unique ordering key (e.g., sensor_1, sensor_2, ...)
which the Redis Sink Connector maps to a Redis key.
"""

from google.cloud import pubsub_v1
import glob
import json
import os
import csv
import time

# Search for the service account JSON key in the current directory
files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# Configuration
project_id = "eda-project-509219"
topic_name = "Image2Redis-dabash"

# Create publisher with ordering enabled
publisher_options = pubsub_v1.types.PublisherOptions(enable_message_ordering=True)
publisher = pubsub_v1.PublisherClient(publisher_options=publisher_options)
topic_path = publisher.topic_path(project_id, topic_name)
print(f"[PRODUCER] Publishing to topic: {topic_path}")

# Read CSV records
csv_path = os.path.join(os.path.dirname(__file__), "Labels.csv")
with open(csv_path, mode="r", encoding="utf-8") as f:
    reader = list(csv.DictReader(f))
    total = len(reader)
    print(f"[PRODUCER] Found {total} records to publish.\n")

# Publish each record with a unique Redis key
for index, row in enumerate(reader, start=1):
    record = {
        "time": int(float(row.get("time", 0))),
        "profile_name": row.get("profileName", "").strip(),
        "temperature": float(row["temperature"]) if row.get("temperature") else None,
        "humidity": float(row["humidity"]) if row.get("humidity") else None,
        "pressure": float(row["pressure"]) if row.get("pressure") else None,
    }

    # The ordering key becomes the Redis key via the Sink Connector
    redis_key = f"sensor_{index}"
    data = json.dumps(record).encode("utf-8")

    try:
        future = publisher.publish(topic_path, data, ordering_key=redis_key)
        future.result()
        print(f"[{index}/{total}] Published key='{redis_key}': {record}")
    except Exception as e:
        print(f"[{index}/{total}] Failed: {e}")

    time.sleep(0.5)

print("\n[PRODUCER] All records published.")
