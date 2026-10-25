import json
import logging
import time
from datetime import datetime
from pathlib import Path

import fastf1
from kafka import KafkaProducer
from kafka.errors import KafkaError

from replayer.config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

fastf1.Cache.enable_cache(settings.f1_cache_dir)


class F1Replayer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            acks='all',
            retries=3
        )
        self.session = None
        self.base_time = None
        self.replay_start_time = None

    def load_session(self, year: int, gp: str):
        """Load a race session from FastF1."""
        logger.info(f"Loading session: {year} {gp}")
        try:
            self.session = fastf1.get_session(year, gp)
            logger.info(f"Loading telemetry, weather, and messages...")
            self.session.load(telemetry=True, weather=True, messages=True, laps=True)
            logger.info(f"✓ Loaded session: {self.session.event['EventName']} ({year})")
            return True
        except Exception as e:
            logger.error(f"Failed to load session: {e}")
            logger.error(f"Retrying with basic load...")
            try:
                self.session = fastf1.get_session(year, gp)
                self.session.load()
                logger.info(f"✓ Basic load successful")
                return True
            except Exception as e2:
                logger.error(f"Basic load also failed: {e2}")
                return False

    def stream_telemetry(self):
        """Stream telemetry data from the session."""
        if not self.session:
            logger.error("No session loaded")
            return

        logger.info("Starting telemetry stream")
        self.base_time = self.session.telemetry['Time'].min()
        self.replay_start_time = time.time()

        for driver in self.session.drivers:
            driver_tel = self.session.telemetry[self.session.telemetry['Driver'] == driver]
            if len(driver_tel) == 0:
                continue

            logger.info(f"Streaming {len(driver_tel)} telemetry records for driver {driver}")

            for _, row in driver_tel.iterrows():
                time_offset = (row['Time'] - self.base_time).total_seconds()
                wait_time = time_offset / settings.replay_speed
                time.sleep(max(0, wait_time - (time.time() - self.replay_start_time)))

                msg = {
                    'timestamp': row['Time'].isoformat() if row['Time'] is not None else None,
                    'driver': str(driver),
                    'speed': float(row['Speed']) if row['Speed'] is not None else None,
                    'rpm': float(row['RPM']) if row['RPM'] is not None else None,
                    'throttle': float(row['Throttle']) if row['Throttle'] is not None else None,
                    'brake': float(row['Brake']) if row['Brake'] is not None else None,
                    'gear': int(row['Gear']) if row['Gear'] is not None else None,
                    'drs': int(row['DRS']) if row['DRS'] is not None else None,
                    'x': float(row['X']) if row['X'] is not None else None,
                    'y': float(row['Y']) if row['Y'] is not None else None,
                    'z': float(row['Z']) if row['Z'] is not None else None,
                    'session_time': time_offset,
                }

                future = self.producer.send(
                    settings.telemetry_topic,
                    value=msg,
                    partition=int(driver) % 20,
                    key=str(driver).encode('utf-8')
                )

                try:
                    record_metadata = future.get(timeout=5)
                    if time.time() % 100 < 0.1:  # Log every ~100 messages
                        logger.debug(f"Sent telemetry: {driver} @ {row['Time']}")
                except KafkaError as e:
                    logger.error(f"Failed to send message: {e}")

    def stream_laps(self):
        """Stream lap and tyre data."""
        if not self.session or not hasattr(self.session, 'laps') or self.session.laps is None:
            logger.warning("No laps data available")
            return

        logger.info("Starting laps stream")
        laps = self.session.laps

        for _, lap in laps.iterrows():
            msg = {
                'timestamp': lap['LapStartTime'].isoformat() if lap['LapStartTime'] else None,
                'driver': str(lap['Driver']),
                'lap_number': int(lap['LapNumber']) if lap['LapNumber'] else None,
                'tyre_compound': lap['Compound'] if 'Compound' in lap else None,
                'tyre_age': int(lap['TyreLife']) if 'TyreLife' in lap else None,
                'pit_in_time': lap['PitInTime'].isoformat() if 'PitInTime' in lap and lap['PitInTime'] else None,
                'pit_out_time': lap['PitOutTime'].isoformat() if 'PitOutTime' in lap and lap['PitOutTime'] else None,
                'lap_time': float(lap['LapTime'].total_seconds()) if lap['LapTime'] else None,
            }

            self.producer.send(
                settings.laps_topic,
                value=msg,
                key=str(lap['Driver']).encode('utf-8')
            )

        logger.info(f"Streamed {len(laps)} lap records")

    def stream_weather(self):
        """Stream weather data."""
        if not self.session or not hasattr(self.session, 'weather') or self.session.weather is None:
            logger.warning("No weather data available")
            return

        logger.info("Starting weather stream")
        weather = self.session.weather

        for _, row in weather.iterrows():
            msg = {
                'timestamp': row['Time'].isoformat() if row['Time'] else None,
                'air_temp': float(row['AirTemp']) if row['AirTemp'] else None,
                'track_temp': float(row['TrackTemp']) if row['TrackTemp'] else None,
                'humidity': float(row['Humidity']) if row['Humidity'] else None,
                'pressure': float(row['Pressure']) if row['Pressure'] else None,
                'wind_direction': int(row['WindDirection']) if row['WindDirection'] else None,
                'wind_speed': float(row['WindSpeed']) if row['WindSpeed'] else None,
                'rainfall': bool(row['Rainfall']) if 'Rainfall' in row else False,
            }

            self.producer.send(
                settings.weather_topic,
                value=msg
            )

        logger.info(f"Streamed {len(weather)} weather records")

    def stream_race_control(self):
        """Stream race control messages."""
        if not self.session or not hasattr(self.session, 'messages') or self.session.messages is None:
            logger.warning("No race control messages available")
            return

        logger.info("Starting race control stream")
        messages = self.session.messages

        for _, msg_row in messages.iterrows():
            msg = {
                'timestamp': msg_row['Time'].isoformat() if msg_row['Time'] else None,
                'category': msg_row['Category'],
                'message': msg_row['Message'],
                'flag': msg_row['Flag'],
            }

            self.producer.send(
                settings.race_control_topic,
                value=msg
            )

        logger.info(f"Streamed {len(messages)} race control messages")

    def run(self, year: int, gp: str):
        """Run the replayer for a session."""
        if not self.load_session(year, gp):
            return

        self.stream_laps()
        self.stream_weather()
        self.stream_race_control()
        self.stream_telemetry()

        self.producer.flush()
        logger.info("Replay complete")

    def close(self):
        self.producer.close()


if __name__ == '__main__':
    replayer = F1Replayer()
    try:
        replayer.run(2023, 'Monaco')
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    finally:
        replayer.close()
