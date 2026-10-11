#!/usr/bin/env python3
"""
Kafka → InfluxDB bridge: Reads F1 telemetry from Kafka and writes to InfluxDB.
Runs as a background service to populate the Grafana dashboard.
"""

import json
import logging
from kafka import KafkaConsumer
from influxdb import InfluxDBClient
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration (from environment variables)
import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
INFLUXDB_HOST = os.getenv('INFLUXDB_HOST', 'localhost')
INFLUXDB_PORT = int(os.getenv('INFLUXDB_PORT', 8086))
INFLUXDB_DATABASE = os.getenv('INFLUXDB_DATABASE', 'pitwall')

# Connect to InfluxDB
try:
    influx_client = InfluxDBClient(
        host=INFLUXDB_HOST,
        port=INFLUXDB_PORT,
        database=INFLUXDB_DATABASE,
        username='admin',
        password='admin_password'
    )
    # Try to create database (may fail if already exists or no permission)
    try:
        influx_client.create_database(INFLUXDB_DATABASE)
    except:
        pass
    logger.info(f"✓ Connected to InfluxDB at {INFLUXDB_HOST}:{INFLUXDB_PORT}")
except Exception as e:
    logger.error(f"✗ Failed to connect to InfluxDB: {e}")
    exit(1)

# Connect to Kafka
try:
    consumer = KafkaConsumer(
        'f1.telemetry',
        'f1.residuals',
        'f1.alerts',
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id='kafka-to-influxdb',
        session_timeout_ms=10000
    )
    logger.info(f"✓ Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
except Exception as e:
    logger.error(f"✗ Failed to connect to Kafka: {e}")
    exit(1)

logger.info("🚀 Starting Kafka → InfluxDB bridge...")
logger.info("Listening on topics: f1.telemetry, f1.residuals, f1.alerts")

message_count = 0
last_log_time = time.time()

try:
    for message in consumer:
        try:
            topic = message.topic
            value = message.value
            ts = value.get('timestamp', time.time())
            # Handle both float and int timestamps
            if isinstance(ts, str):
                ts = float(ts)
            timestamp = int(ts * 1e9) if ts < 1e10 else int(ts)

            # Convert Kafka message to InfluxDB point
            if topic == 'f1.telemetry':
                point = {
                    "measurement": "f1_telemetry",
                    "tags": {
                        "driver": value.get('driver', 'unknown'),
                    },
                    "time": timestamp,
                    "fields": {
                        "speed": float(value.get('speed') or 0),
                        "rpm": float(value.get('rpm') or 0),
                        "throttle": float(value.get('throttle') or 0),
                        "brake": float(value.get('brake') or 0),
                        "gear": int(value.get('gear') or 0),
                        "drs": int(value.get('drs') or 0),
                    }
                }

            elif topic == 'f1.residuals':
                residuals = value.get('residuals', {})
                point = {
                    "measurement": "f1_residuals",
                    "tags": {
                        "driver": value.get('driver', 'unknown'),
                    },
                    "time": timestamp,
                    "fields": {
                        "speed_residual": float(residuals.get('speed') or 0),
                        "rpm_residual": float(residuals.get('rpm') or 0),
                        "throttle_residual": float(residuals.get('throttle') or 0),
                        "brake_residual": float(residuals.get('brake') or 0),
                    }
                }

            elif topic == 'f1.alerts':
                alert_type = value.get('type', 'unknown')
                point = {
                    "measurement": "f1_alerts",
                    "tags": {
                        "driver": value.get('driver', 'unknown'),
                        "alert_type": alert_type,
                    },
                    "time": timestamp,
                    "fields": {
                        "severity": float(value.get('severity', 0)),
                    }
                }

            else:
                continue

            # Write to InfluxDB
            influx_client.write_points([point])
            message_count += 1

            # Log progress every 100 messages
            current_time = time.time()
            if current_time - last_log_time > 5:  # Log every 5 seconds
                logger.info(f"✓ Written {message_count} points to InfluxDB")
                last_log_time = current_time

        except Exception as e:
            logger.error(f"Error processing message: {e}")
            continue

except KeyboardInterrupt:
    logger.info("Shutting down...")
finally:
    consumer.close()
    influx_client.close()
    logger.info(f"✓ Bridge closed. Wrote {message_count} points total.")
