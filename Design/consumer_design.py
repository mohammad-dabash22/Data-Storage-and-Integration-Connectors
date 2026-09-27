"""
Milestone 2 Design - Consumer Script
Connects directly to the MySQL server on GKE to query and analyze
the SmartMeter records that were stored by the Sink Connector.
This demonstrates that the storage layer decouples producer and consumer.
"""

import mysql.connector  # pip install mysql-connector-python
import time

# MySQL connection details (from mysql-deploy.yaml and kubectl get service)
MYSQL_HOST = "35.224.195.92"
MYSQL_USER = "usr"
MYSQL_PASS = "sofe4630u"
MYSQL_DB = "Readings"


def get_connection():
    """Establish connection to MySQL on GKE."""
    return mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASS,
        database=MYSQL_DB,
    )


def show_latest_records(cursor, limit=10):
    """Display the most recent records inserted by the Sink Connector."""
    cursor.execute(f"SELECT * FROM SmartMeter ORDER BY ID DESC LIMIT {limit}")
    rows = cursor.fetchall()
    print(f"\n{'='*70}")
    print(f"  Latest {limit} Records in SmartMeter Table")
    print(f"{'='*70}")
    print(f"  {'ID':<8} {'Time':<12} {'Profile':<10} {'Temp':<10} {'Humidity':<10} {'Pressure':<10}")
    print(f"  {'-'*8} {'-'*12} {'-'*10} {'-'*10} {'-'*10} {'-'*10}")
    for row in rows:
        print(f"  {row[0]:<8} {row[1]:<12} {row[2]:<10} {str(row[3]):<10} {str(row[4]):<10} {str(row[5]):<10}")


def show_statistics(cursor):
    """Show average temperature, humidity, and pressure per city profile."""
    cursor.execute("""
        SELECT profile_name,
               COUNT(*) AS count,
               ROUND(AVG(temperature), 2) AS avg_temp,
               ROUND(AVG(humidity), 2) AS avg_humidity,
               ROUND(AVG(pressure), 4) AS avg_pressure
        FROM SmartMeter
        WHERE profile_name IS NOT NULL
        GROUP BY profile_name
    """)
    rows = cursor.fetchall()
    print(f"\n{'='*70}")
    print(f"  Average Readings Per City Profile")
    print(f"{'='*70}")
    print(f"  {'Profile':<10} {'Count':<8} {'Avg Temp':<12} {'Avg Humidity':<14} {'Avg Pressure':<14}")
    print(f"  {'-'*10} {'-'*8} {'-'*12} {'-'*14} {'-'*14}")
    for row in rows:
        print(f"  {row[0]:<10} {row[1]:<8} {row[2]:<12} {row[3]:<14} {row[4]:<14}")


def show_total_count(cursor):
    """Show total number of records stored."""
    cursor.execute("SELECT COUNT(*) FROM SmartMeter")
    count = cursor.fetchone()[0]
    print(f"\n  Total records stored in SmartMeter: {count}")
    return count


def main():
    print("[CONSUMER] Connecting to MySQL on GKE...")
    conn = get_connection()
    cursor = conn.cursor()
    print("[CONSUMER] Connected successfully.\n")

    try:
        # Poll MySQL periodically to show new records arriving via the Sink Connector
        print("[CONSUMER] Monitoring SmartMeter table for new records...")
        print("[CONSUMER] Press Ctrl+C to stop.\n")

        prev_count = 0
        while True:
            count = show_total_count(cursor)

            if count > prev_count:
                new = count - prev_count
                print(f"  >> {new} new record(s) detected!")
                show_latest_records(cursor)
                show_statistics(cursor)
                prev_count = count
            else:
                print("  Waiting for new records...")

            print()
            time.sleep(3)

    except KeyboardInterrupt:
        print("\n[CONSUMER] Stopped. Goodbye!")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
