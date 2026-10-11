#!/usr/bin/env python3
"""
Mock F1 telemetry data generator for testing the dashboard.
Generates realistic F1 telemetry and writes directly to InfluxDB.
"""

import json
import time
import logging
import random
import math
from influxdb import InfluxDBClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# InfluxDB connection
# Try Docker hostname first, fall back to localhost
import os
host = os.getenv('INFLUXDB_HOST', 'influxdb')
client = InfluxDBClient(host=host, port=8086, database='pitwall', username='admin', password='admin_password')
try:
    client.create_database('pitwall')
except:
    pass

logger.info("✓ Connected to InfluxDB")

# Simulate 20 drivers
NUM_DRIVERS = 20
SIMULATION_TIME = 7200  # 2 hours in seconds
TIME_STEP = 0.1  # 10 Hz sampling

logger.info(f"Generating {NUM_DRIVERS} drivers × {SIMULATION_TIME}s of telemetry...")

base_time = int(time.time() * 1e9)  # nanoseconds

for t in range(0, SIMULATION_TIME, int(TIME_STEP * 100)):
    points = []

    for driver in range(1, NUM_DRIVERS + 1):
        # Simulate realistic F1 telemetry patterns
        # Speed: oscillates between 100-330 km/h (slow corners to straights)
        speed = 200 + 100 * math.sin(t / 100) + random.gauss(0, 5)
        speed = max(80, min(330, speed))

        # RPM: 6000-13000, correlates with speed
        rpm = 9000 + speed * 15 + random.gauss(0, 200)
        rpm = max(6000, min(13000, rpm))

        # Throttle: 0-1, random but trending
        throttle = max(0, min(1, 0.5 + random.gauss(0, 0.3)))

        # Brake: 0-1, inverse of throttle
        brake = max(0, min(1, 0.3 * (1 - throttle) + random.gauss(0, 0.1)))

        # Gear: 1-8
        gear = max(1, min(8, 3 + int(speed / 40)))

        # DRS: 0 or 1 (randomly active)
        drs = 1 if random.random() > 0.7 and speed > 250 else 0

        # Create telemetry point
        tel_point = {
            "measurement": "f1_telemetry",
            "tags": {"driver": str(driver)},
            "time": base_time + t * int(1e7),  # convert to nanoseconds
            "fields": {
                "speed": float(speed),
                "rpm": float(rpm),
                "throttle": float(throttle),
                "brake": float(brake),
                "gear": int(gear),
                "drs": int(drs),
            }
        }
        points.append(tel_point)

        # Create residual points (predicted vs actual)
        # Simulate prediction errors
        pred_speed = speed + random.gauss(0, 10)  # some error
        residual = speed - pred_speed

        res_point = {
            "measurement": "f1_residuals",
            "tags": {"driver": str(driver)},
            "time": base_time + t * int(1e7),
            "fields": {
                "speed_residual": float(residual),
                "rpm_residual": float(random.gauss(0, 100)),
                "throttle_residual": float(random.gauss(0, 0.05)),
                "brake_residual": float(random.gauss(0, 0.05)),
            }
        }
        points.append(res_point)

        # Occasionally inject anomalies for some drivers
        if driver in [5, 12] and 3600 < t < 4200:  # Driver 5 and 12, after 1 hour
            alert_point = {
                "measurement": "f1_alerts",
                "tags": {"driver": str(driver), "alert_type": "anomaly"},
                "time": base_time + t * int(1e7),
                "fields": {"severity": 0.8}
            }
            points.append(alert_point)

    # Write batch to InfluxDB
    if points:
        client.write_points(points, batch_size=500)

    # Log progress every 60 seconds
    if t % 600 == 0:
        progress = (t / SIMULATION_TIME) * 100
        logger.info(f"  {progress:.1f}% - Wrote {len(points)} telemetry points")

    time.sleep(0.01)  # Small delay to avoid flooding

logger.info(f"✓ Complete! Generated {t // 1}s of telemetry for {NUM_DRIVERS} drivers")
logger.info("✓ Data is now in InfluxDB - refresh Grafana to see it!")

client.close()
