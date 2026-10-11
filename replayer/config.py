from pathlib import Path
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    kafka_bootstrap_servers: str = "localhost:9092"
    f1_cache_dir: str = "/data/f1_cache"

    # Replay configuration
    replay_speed: float = 1.0  # 1.0, 10.0, or 50.0
    start_year: int = 2018
    start_round: int = 1

    # Kafka topics
    telemetry_topic: str = "f1.telemetry"
    laps_topic: str = "f1.laps"
    race_control_topic: str = "f1.race_control"
    weather_topic: str = "f1.weather"

    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings()
