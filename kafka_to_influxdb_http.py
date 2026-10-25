#!/usr/bin/env python3
"""
Kafka → InfluxDB bridge using HTTP writes (no auth needed).
"""

import json
import time
import logging
import requests
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

KAFKA_SERVERS = 'kafka:29092'
INFLUXDB_URL = 'http://influxdb:8086'
DATABASE = 'f1'

logger.info(f"✓ Kafka: {KAFKA_SERVERS}")
logger.info(f"✓ InfluxDB: {INFLUXDB_URL}")

consumer = KafkaConsumer(
    'f1.telemetry', 'f1.residuals', 'f1.alerts',
    bootstrap_servers=KAFKA_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode()),
    auto_offset_reset='earliest',
    group_id='kafka-bridge'
)

points = []
count = 0

try:
    for msg in consumer:
        try:
            val = msg.value
            topic = msg.topic

            if topic == 'f1.telemetry':
                driver = val.get('driver', 'unknown')
                timestamp_str = val.get('timestamp', time.time())
                try:
                    from datetime import datetime
                    dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                    ts = int(dt.timestamp() * 1e9)
                except:
                    ts = int(float(timestamp_str) * 1e9) if isinstance(timestamp_str, (int, float)) else int(time.time() * 1e9)
                point = f"f1_telemetry,driver={driver} speed={val.get('speed',0)},rpm={val.get('rpm',0)},throttle={val.get('throttle',0)},brake={val.get('brake',0)},gear={val.get('gear',0)},drs={val.get('drs',0)} {ts}"
                points.append(point)

            elif topic == 'f1.residuals':
                driver = val.get('driver', 'unknown')
                timestamp_str = val.get('timestamp', time.time())
                try:
                    from datetime import datetime
                    dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                    ts = int(dt.timestamp() * 1e9)
                except:
                    ts = int(float(timestamp_str) * 1e9) if isinstance(timestamp_str, (int, float)) else int(time.time() * 1e9)
                res = val.get('residuals', {})
                point = f"f1_residuals,driver={driver} speed_residual={res.get('speed',0)},rpm_residual={res.get('rpm',0)} {ts}"
                points.append(point)

            elif topic == 'f1.alerts':
                driver = val.get('driver', 'unknown')
                timestamp_str = val.get('timestamp', time.time())
                try:
                    from datetime import datetime
                    dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                    ts = int(dt.timestamp() * 1e9)
                except:
                    ts = int(float(timestamp_str) * 1e9) if isinstance(timestamp_str, (int, float)) else int(time.time() * 1e9)
                point = f"f1_alerts,driver={driver},type={val.get('type','unknown')} severity={val.get('severity',0)} {ts}"
                points.append(point)

            count += 1

            # Write every 100 points
            if len(points) >= 100:
                data = '\n'.join(points)
                r = requests.post(f"{INFLUXDB_URL}/write?db={DATABASE}", data=data, timeout=5)
                if r.status_code == 204:
                    logger.info(f"✓ Wrote {len(points)} points ({count} total)")
                else:
                    logger.error(f"Write failed: {r.status_code} {r.text[:100]}")
                points = []

        except Exception as e:
            logger.error(f"Error: {e}")
            continue

except KeyboardInterrupt:
    logger.info("Shutting down...")

# Flush remaining
if points:
    data = '\n'.join(points)
    requests.post(f"{INFLUXDB_URL}/write?db={DATABASE}", data=data)
    logger.info(f"✓ Flushed {len(points)} remaining points")

consumer.close()
logger.info(f"✓ Complete! Wrote {count} total points")
