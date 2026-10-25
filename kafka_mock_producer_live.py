#!/usr/bin/env python3
import json, random, time
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['kafka:29092'],
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8') if k else None,
    acks='all',
    request_timeout_ms=10000
)

# Anchor on "now" (UTC, timezone-aware) and walk BACKWARD, so every point
# is timestamped at or before the moment the script runs - never in the future.
now = datetime.now(timezone.utc)
drivers = [f"Driver_{i+1}" for i in range(20)]
NUM_SECONDS = 300

print("Sending LIVE F1 data to Kafka...")

for second in range(NUM_SECONDS):
    # second=0 -> oldest point (now - 299s), last iteration -> newest point (now - 0s)
    timestamp = now - timedelta(seconds=(NUM_SECONDS - 1 - second))

    for driver_id, driver_name in enumerate(drivers):
        msg = {
            'timestamp': timestamp.isoformat(),
            'driver': driver_name,
            'speed': float(175 + random.gauss(20, 5)),
            'rpm': float(11900 + random.gauss(100, 50)),
            'throttle': float(0.5 + random.gauss(0, 0.05)),
            'brake': float(0.15 + random.gauss(0, 0.02)),
            'gear': random.randint(1, 8),
            'drs': random.choice([0, 1]),
        }
        try:
            producer.send('f1.telemetry', value=msg, key=driver_name)

            # Residuals (LSTM prediction error) - every point
            residual_msg = {
                'timestamp': timestamp.isoformat(),
                'driver': driver_name,
                'residuals': {
                    'speed': random.gauss(0, 2),
                    'rpm': random.gauss(0, 50),
                },
            }
            producer.send('f1.residuals', value=residual_msg, key=driver_name)

            # Alerts (confirmed drift/anomaly) - occasional, higher severity = more notable
            if random.random() < 0.03:  # ~3% of points flagged as anomalies
                alert_msg = {
                    'timestamp': timestamp.isoformat(),
                    'driver': driver_name,
                    'type': random.choice(['speed_drift', 'brake_anomaly', 'rpm_spike']),
                    'severity': round(random.uniform(0.5, 1.0), 2),
                }
                producer.send('f1.alerts', value=alert_msg, key=driver_name)
        except Exception as e:
            print(f"Error: {e}")

producer.flush()
print(f"✓ {NUM_SECONDS * len(drivers)} telemetry points + residuals + alerts sent to Kafka (timestamps ending at {now.isoformat()})")
