"""
Twin service: consumes telemetry from Kafka, runs predictions,
detects drift, writes results to InfluxDB.
"""

import json
import logging
import time
from collections import defaultdict
from datetime import datetime
from typing import Dict, List

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import numpy as np

from lstm_twin import TelemetryTwin
from drift_gate import DriftGate, FleetConsensus

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TwinService:
    """Main streaming twin service."""

    def __init__(
        self,
        kafka_servers: str = "localhost:9092",
        influxdb_url: str = "http://localhost:8086",
        mode: str = "B0",  # B0, B1, E1, E2
    ):
        self.kafka_servers = kafka_servers
        self.influxdb_url = influxdb_url
        self.mode = mode  # determines adaptation behavior

        # Twin model
        self.twin = TelemetryTwin(
            channels=["speed", "rpm", "throttle", "brake", "gear"],
            seq_len=30,
            hidden_size=128,
            device="cpu",
        )

        # Drift detection (E1, E2 modes)
        self.drift_gate = DriftGate(
            channels=self.twin.channels,
            adwin_threshold=0.05,
        )

        # Fleet consensus (E2 mode only)
        self.fleet = FleetConsensus(num_drivers=20, consensus_threshold=0.7)

        # Kafka consumers
        self.telemetry_consumer = KafkaConsumer(
            "f1.telemetry",
            bootstrap_servers=kafka_servers,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id="twin-service",
            auto_offset_reset="earliest",
        )

        # Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        )

        # State: recent telemetry history per driver
        self.driver_history = defaultdict(lambda: {ch: [] for ch in self.twin.channels})

        # Statistics
        self.predictions_made = 0
        self.anomalies_detected = 0
        self.adaptations_triggered = 0

    def run(self, duration_seconds: int = None):
        """Run the twin service."""
        logger.info(f"Starting twin service (mode={self.mode})")
        logger.info(f"Consuming from Kafka: {self.kafka_servers}")

        start_time = time.time()
        msg_count = 0

        try:
            for message in self.telemetry_consumer:
                if duration_seconds and (time.time() - start_time) > duration_seconds:
                    break

                try:
                    tel = message.value
                    driver = tel.get("driver")
                    timestamp = tel.get("timestamp")

                    # Update history
                    for channel in self.twin.channels:
                        if channel in tel and tel[channel] is not None:
                            self.driver_history[driver][channel].append(tel[channel])
                            # Keep only recent history
                            if len(self.driver_history[driver][channel]) > 100:
                                self.driver_history[driver][channel].pop(0)

                    # Make prediction when we have enough history
                    if all(
                        len(self.driver_history[driver][ch]) >= self.twin.seq_len
                        for ch in self.twin.channels
                    ):
                        predictions = self.twin.predict(self.driver_history[driver])
                        actual = {ch: tel.get(ch) for ch in self.twin.channels}

                        # Compute residuals
                        residuals = self.twin.get_residuals(actual, predictions)

                        # Publish prediction results
                        result = {
                            "timestamp": timestamp,
                            "driver": driver,
                            "actual": actual,
                            "predicted": predictions,
                            "residuals": residuals,
                        }

                        self.producer.send("f1.residuals", value=result)
                        self.predictions_made += 1

                        # Drift detection (E1, E2)
                        if self.mode in ["E1", "E2"]:
                            should_adapt, drift_events = self.drift_gate.update(
                                residuals, timestamp=float(msg_count)
                            )
                            if drift_events:
                                logger.info(
                                    f"[{driver}] Drift: {drift_events[0].description}"
                                )
                                self.producer.send(
                                    "f1.alerts",
                                    value={
                                        "timestamp": timestamp,
                                        "driver": driver,
                                        "type": "drift",
                                        "event": drift_events[0].__dict__,
                                    },
                                )

                            # Fleet consensus (E2 only)
                            if self.mode == "E2":
                                classification = self.fleet.update(int(driver) or 0, residuals)
                                result["classification"] = classification

                            # Decide adaptation
                            if should_adapt and self.mode in ["E1", "E2"]:
                                logger.info(f"[{driver}] Triggering adaptation")
                                self.adaptations_triggered += 1
                                # In real system: retrain on recent data
                                # For now: just log

                        # Publish to InfluxDB topic
                        self.producer.send("f1.alerts", value=result)

                        # Log every 100 predictions
                        if self.predictions_made % 100 == 0:
                            logger.info(
                                f"Processed {msg_count} messages, "
                                f"{self.predictions_made} predictions, "
                                f"{self.adaptations_triggered} adaptations"
                            )

                    msg_count += 1

                except Exception as e:
                    logger.error(f"Error processing message: {e}", exc_info=True)
                    continue

        except KeyboardInterrupt:
            logger.info("Shutting down...")
        finally:
            self.shutdown()

    def shutdown(self):
        """Clean shutdown."""
        logger.info(
            f"Final stats: {self.predictions_made} predictions, "
            f"{self.adaptations_triggered} adaptations"
        )
        self.telemetry_consumer.close()
        self.producer.flush()
        self.producer.close()


if __name__ == "__main__":
    import sys

    mode = sys.argv[1] if len(sys.argv) > 1 else "B0"
    service = TwinService(
        kafka_servers="localhost:9092",
        mode=mode,
    )
    service.run()
