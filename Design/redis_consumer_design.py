"""
Milestone 2 Design - Redis Consumer Script
Connects to the Redis server on GKE, retrieves all sensor records
stored by the Sink Connector under keys 'sensor_1', 'sensor_2', etc.,
and displays statistics per city profile.
"""

import redis  # pip install redis
import json

# Redis connection details (from redis.yaml and kubectl get service)
REDIS_HOST = "34.60.5.157"
REDIS_PORT = 6379
REDIS_PASS = "sofe4630u"

print("[CONSUMER] Connecting to Redis on GKE...")
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, password=REDIS_PASS)

# Get all sensor keys
keys = sorted([k.decode() for k in r.keys("sensor_*")])
print(f"[CONSUMER] Found {len(keys)} sensor records in Redis.\n")

if not keys:
    print("[CONSUMER] No records found. Has the producer run yet?")
    exit()

# Retrieve and parse all records
records = []
for key in keys:
    value = r.get(key)
    if value:
        record = json.loads(value.decode("utf-8"))
        records.append((key, record))

# Display latest records
print(f"{'='*70}")
print(f"  Sensor Records Stored in Redis")
print(f"{'='*70}")
print(f"  {'Key':<12} {'Profile':<10} {'Temp':<10} {'Humidity':<10} {'Pressure':<10}")
print(f"  {'-'*12} {'-'*10} {'-'*10} {'-'*10} {'-'*10}")
for key, rec in records[-10:]:  # Show last 10
    print(f"  {key:<12} {str(rec.get('profile_name','')):<10} {str(rec.get('temperature','')):<10.5} {str(rec.get('humidity','')):<10.5} {str(rec.get('pressure','')):<10.5}")

# Calculate per-city statistics
print(f"\n{'='*70}")
print(f"  Average Readings Per City Profile")
print(f"{'='*70}")

stats = {}
for _, rec in records:
    city = rec.get("profile_name", "unknown")
    if city not in stats:
        stats[city] = {"count": 0, "temp": [], "humidity": [], "pressure": []}
    stats[city]["count"] += 1
    if rec.get("temperature") is not None:
        stats[city]["temp"].append(rec["temperature"])
    if rec.get("humidity") is not None:
        stats[city]["humidity"].append(rec["humidity"])
    if rec.get("pressure") is not None:
        stats[city]["pressure"].append(rec["pressure"])

print(f"  {'Profile':<10} {'Count':<8} {'Avg Temp':<12} {'Avg Humidity':<14} {'Avg Pressure':<14}")
print(f"  {'-'*10} {'-'*8} {'-'*12} {'-'*14} {'-'*14}")
for city, data in stats.items():
    avg_temp = round(sum(data["temp"]) / len(data["temp"]), 2) if data["temp"] else "N/A"
    avg_hum = round(sum(data["humidity"]) / len(data["humidity"]), 2) if data["humidity"] else "N/A"
    avg_pres = round(sum(data["pressure"]) / len(data["pressure"]), 4) if data["pressure"] else "N/A"
    print(f"  {city:<10} {data['count']:<8} {str(avg_temp):<12} {str(avg_hum):<14} {str(avg_pres):<14}")

print(f"\n[CONSUMER] Total records: {len(records)}")
