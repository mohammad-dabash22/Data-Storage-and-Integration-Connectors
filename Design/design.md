# Milestone 2 — Design: Data Storage and Integration Connectors

## System Architecture

In Milestone 1, the producer published sensor data directly to a Pub/Sub topic and a consumer subscribed to that topic in real time. The producer and consumer were tightly coupled through the message broker — if the consumer was offline, messages would pile up in the subscription backlog unprocessed, and there was no persistent queryable storage of historical data.

In Milestone 2, we introduce **two storage pipelines** using **GCP Sink Connectors** to store the same sensor data in two different ways:

### Pipeline 1: MySQL (Tabular Storage)

```
Producer (producer_design.py)
    │
    │  Publishes JSON sensor records
    ▼
Google Cloud Pub/Sub Topic (smartMeterReadings-dabash)
    │
    │  Automatic event trigger
    ▼
Application Integration + MySQL Sink Connector
    │
    │  Auto-INSERT into SmartMeter table
    ▼
MySQL Database on GKE (Readings.SmartMeter)
    │
    │  SQL queries
    ▼
Consumer (consumer_design.py)
```

### Pipeline 2: Redis (Key-Value Storage)

```
Producer (redis_producer_design.py)
    │
    │  Publishes JSON sensor records with unique ordering keys
    ▼
Google Cloud Pub/Sub Topic (Image2Redis-dabash)
    │
    │  Automatic event trigger
    ▼
Application Integration + Redis Sink Connector
    │
    │  Auto-SET: ordering key → Redis key, data → Redis value
    ▼
Redis on GKE (Keys: sensor_1, sensor_2, ...)
    │
    │  Key lookup
    ▼
Consumer (redis_consumer_design.py)
```

## Why Two Storage Systems?

Both pipelines store the same sensor data but demonstrate different storage paradigms:

- **MySQL** stores records in a **structured table** with a fixed schema. Each record becomes a row with typed columns (ID, time, profile_name, temperature, humidity, pressure). This allows SQL queries for filtering and aggregation (e.g., `AVG(temperature) GROUP BY profile_name`). Data is persisted to disk and is ACID compliant.

- **Redis** stores records as **key-value pairs**. Each sensor reading is stored as a JSON string under a unique key (`sensor_1`, `sensor_2`, ...). Redis is an in-memory store optimized for fast reads/writes. However, it lacks SQL's query capabilities — the consumer must retrieve and process data in application code.

Both pipelines share the same principle: the **producer never touches the database directly**. The GCP Sink Connector handles all database connectivity and insertion automatically.

## Database Schema (MySQL)

```sql
CREATE TABLE SmartMeter(
    ID INT PRIMARY KEY,
    time BIGINT,
    profile_name VARCHAR(100),
    temperature DOUBLE,
    humidity DOUBLE,
    pressure DOUBLE
);
```

## Redis Data Model

Each sensor record is stored as a key-value pair:

- **Key**: `sensor_1`, `sensor_2`, ... (mapped from the Pub/Sub message's `orderingKey`)
- **Value**: JSON string, e.g. `{"time": 1768708698, "profile_name": "denver", "temperature": 31.1, ...}`
- **Type**: `String`

## Components

### Pipeline 1: MySQL

#### Producer (`producer_design.py`)
- Reads sensor records from `Labels.csv` (same dataset from Milestone 1).
- Formats each row to match the SmartMeter table schema with a unique incrementing ID.
- Publishes each record as a JSON message to the `smartMeterReadings-dabash` Pub/Sub topic.
- The MySQL Sink Connector automatically inserts each message into the SmartMeter table.

#### Consumer (`consumer_design.py`)
- Connects directly to MySQL on GKE using the `mysql-connector-python` library.
- Polls the SmartMeter table every 3 seconds for new records.
- Displays the latest records and calculates per-city average statistics.

### Pipeline 2: Redis

#### Producer (`redis_producer_design.py`)
- Reads sensor records from the same `Labels.csv` file.
- Publishes each record as a JSON message to the `Image2Redis-dabash` Pub/Sub topic.
- Each record uses a unique `ordering_key` (e.g., `sensor_1`, `sensor_2`) which the Sink Connector maps to a Redis key.

#### Consumer (`redis_consumer_design.py`)
- Connects directly to Redis on GKE using the `redis` Python library.
- Retrieves all keys matching the pattern `sensor_*`.
- Parses the JSON values and displays the latest records.
- Calculates per-city average statistics in application code.

## How to Run

### Prerequisites
```bash
pip install google-cloud-pubsub mysql-connector-python redis
```

### MySQL Pipeline
1. Ensure `mysql-integration` is **published** in GCP Application Integration.
2. Copy your GCP service account JSON key into the `Design/` folder.
3. In one terminal, start the consumer:
   ```bash
   python consumer_design.py
   ```
4. In another terminal, start the producer:
   ```bash
   python producer_design.py
   ```

### Redis Pipeline
1. Ensure `redis-integration` is **published** in GCP Application Integration.
2. Run the producer to publish sensor records:
   ```bash
   python redis_producer_design.py
   ```
3. Wait a few seconds for the Sink Connector to process, then run the consumer:
   ```bash
   python redis_consumer_design.py
   ```
