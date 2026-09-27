# Milestone 2 — Design: Data Storage and Integration Connectors

## System Architecture

In Milestone 1, the producer published sensor data directly to a Pub/Sub topic and a consumer subscribed to that topic in real time. The producer and consumer were tightly coupled through the message broker — if the consumer was offline, messages would pile up in the subscription backlog unprocessed, and there was no persistent queryable storage of historical data.

In Milestone 2, we introduce a **MySQL storage layer on Google Kubernetes Engine (GKE)** between the producer and consumer using a **GCP Sink Connector**. The updated pipeline is:

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

## Why MySQL?

MySQL was chosen as the intermediate storage layer for the following reasons:

1. **Structured Data**: Smart meter readings have well-defined fields (ID, time, profile_name, temperature, humidity, pressure). A relational database with a fixed schema is the natural fit for structured tabular data.

2. **Persistence**: Unlike in-memory stores, MySQL persists data to disk. If the consumer goes offline or the cluster restarts, no historical data is lost.

3. **Queryable**: The consumer can run SQL queries to filter, aggregate, and analyze stored data (e.g., average temperature per city) rather than processing a raw message stream.

4. **Decoupling**: The producer never touches MySQL directly. The GCP Sink Connector handles all database connectivity, formatting, and insertion automatically. The consumer reads from MySQL independently on its own schedule.

## Database Schema

The SmartMeter table was created on the MySQL server deployed on GKE:

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

## Components

### Producer (`producer_design.py`)
- Reads sensor records from `Labels.csv` (same dataset from Milestone 1).
- Formats each row to match the SmartMeter table schema with a unique incrementing ID.
- Publishes each record as a JSON message to the `smartMeterReadings-dabash` Pub/Sub topic.
- The MySQL Sink Connector automatically consumes each message and inserts it into the SmartMeter table.

### Consumer (`consumer_design.py`)
- Connects directly to the MySQL database on GKE using the `mysql-connector-python` library.
- Polls the SmartMeter table every 3 seconds for new records.
- Displays the latest records and calculates per-city average statistics (temperature, humidity, pressure).
- Demonstrates that the consumer is fully decoupled from the producer — it reads from persistent storage, not from the live Pub/Sub stream.

## How to Run

### Prerequisites
```bash
pip install google-cloud-pubsub mysql-connector-python
```

### Steps
1. Ensure the MySQL Sink Connector integration (`mysql-integration`) is **published** in GCP Application Integration.
2. Copy your GCP service account JSON key into the `Design/` folder.
3. In one terminal, start the consumer:
   ```bash
   python consumer_design.py
   ```
4. In another terminal, start the producer:
   ```bash
   python producer_design.py
   ```
5. Watch the consumer terminal — as the producer publishes records, the Sink Connector stores them in MySQL, and the consumer detects and displays them with statistics.
6. After the demo, **unpublish** the integration and **suspend** the connector to save GCP credits.
