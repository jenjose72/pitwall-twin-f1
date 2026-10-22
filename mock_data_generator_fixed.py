#!/usr/bin/env python3
import logging
from influxdb import InfluxDBClient
import random
import time
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Connect to InfluxDB
try:
    client = InfluxDBClient(host='localhost', port=8086, database='f1')
    client.ping()
    logger.info('✓ Connected to InfluxDB')
except Exception as e:
    logger.error(f'Failed to connect: {e}')
    exit(1)

# Generate telemetry data
logger.info('Generating 20 drivers × 7200s of telemetry...')

num_drivers = 20
duration_seconds = 7200
sampling_interval = 1  # 1 second per sample
base_time = datetime.utcnow() - timedelta(hours=2)  # 2 hours ago

points = []
total_written = 0

for driver_id in range(1, num_drivers + 1):
    for second in range(0, duration_seconds, sampling_interval):
        # Realistic F1 telemetry
        speed = 185 + random.gauss(15, 5)  # 185-210 km/h
        rpm = 11900 + random.gauss(100, 50)  # 11800-12000 RPM
        throttle = random.uniform(0.47, 0.52)  # 47-52%
        brake = random.uniform(0.13, 0.17)  # 13-17%
        drs = random.choice([0, 1])  # Open/closed
        gear = random.randint(1, 8)
        
        # Timestamp distributed over time
        timestamp = base_time + timedelta(seconds=second)
        
        point = {
            'measurement': 'f1_telemetry',
            'time': timestamp,
            'tags': {'driver': str(driver_id)},
            'fields': {
                'speed': speed,
                'rpm': rpm,
                'throttle': throttle,
                'brake': brake,
                'drs': drs,
                'gear': gear
            }
        }
        points.append(point)
        
        # Batch write
        if len(points) >= 100:
            client.write_points(points)
            total_written += len(points)
            percent = (total_written / (num_drivers * (duration_seconds // sampling_interval))) * 100
            logger.info(f'  {percent:.1f}% - Wrote {len(points)} telemetry points')
            points = []

# Final batch
if points:
    client.write_points(points)
    total_written += len(points)

logger.info(f'✓ Complete! Generated {duration_seconds}s of telemetry for {num_drivers} drivers')
logger.info(f'✓ Total points written: {total_written}')
logger.info(f'✓ Data is now in InfluxDB - refresh Grafana to see it!')

# Generate residuals and alerts
logger.info('Generating residuals and alerts...')

residual_points = []
alert_points = []

for driver_id in range(1, num_drivers + 1):
    for second in range(0, duration_seconds, 10):  # Every 10 seconds
        timestamp = base_time + timedelta(seconds=second)
        
        residual_points.append({
            'measurement': 'f1_residuals',
            'time': timestamp,
            'tags': {'driver': str(driver_id)},
            'fields': {'speed_residual': random.uniform(-5, 5)}
        })
        
        # Some random alerts
        if random.random() < 0.05:  # 5% chance
            alert_points.append({
                'measurement': 'f1_alerts',
                'time': timestamp,
                'tags': {'driver': str(driver_id)},
                'fields': {'severity': random.choice([1, 2, 3])}
            })

if residual_points:
    client.write_points(residual_points)
    logger.info(f'✓ Wrote {len(residual_points)} residual points')

if alert_points:
    client.write_points(alert_points)
    logger.info(f'✓ Wrote {len(alert_points)} alert points')

logger.info('✓ All data generation complete!')
