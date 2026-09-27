"""
Milestone 2 Design - Producer Script
Reads records from Labels.csv, serializes each row to JSON,
and publishes to the Pub/Sub topic 'smartMeterReadings-dabash'.
The MySQL Sink Connector (built in MS2) automatically inserts
each message into the SmartMeter table on GKE.
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
topic_name = "smartMeterReadings-dabash"

# Create publisher and resolve topic path
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
print(f"[PRODUCER] Publishing to topic: {topic_path}")

# Read CSV records
csv_path = os.path.join(os.path.dirname(__file__), "Labels.csv")
with open(csv_path, mode="r", encoding="utf-8") as f:
    reader = list(csv.DictReader(f))
    total = len(reader)
    print(f"[PRODUCER] Found {total} records to publish.\n")

# Publish each record with a unique incrementing ID
ID = 1
for index, row in enumerate(reader, start=1):
    # Build the message matching the SmartMeter table schema
    record = {
        "ID": ID,
        "time": int(float(row.get("time", 0))),
        "profile_name": row.get("profileName", "").strip(),
        "temperature": float(row["temperature"]) if row.get("temperature") else None,
        "humidity": float(row["humidity"]) if row.get("humidity") else None,
        "pressure": float(row["pressure"]) if row.get("pressure") else None,
    }
    ID += 1

    record_bytes = json.dumps(record).encode("utf-8")

    try:
        future = publisher.publish(topic_path, record_bytes)
        future.result()
        print(f"[{index}/{total}] Published: {record}")
    except Exception as e:
        print(f"[{index}/{total}] Failed: {e}")

    time.sleep(0.5)

print("\n[PRODUCER] All records published.")
