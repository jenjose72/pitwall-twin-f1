# PitWall Twin — Drift-Aware Digital Twin for F1 Telemetry

Real-time anomaly detection in Formula 1 car telemetry using a streaming digital twin with drift detection and fleet consensus.

## Architecture

```
FastF1 Replayer → Kafka
    ↓ (20 partitions by driver)
Spark Structured Streaming (feature engineering)
    ↓
PyTorch Digital Twin (LSTM residuals)
    ↓
River Drift Detectors (ADWIN/DDM)
    ↓
InfluxDB + Grafana Dashboard
```

## Quick Start

### Prerequisites

- Docker and Docker Compose
- ~4GB RAM (Kafka + Spark + services)

### 1. Start the Stack

```bash
docker-compose up -d
```

This spins up:
- **Kafka** (9092): Message broker with 20 partitions
- **Kafka UI** (8080): Topic monitoring
- **Spark Master** (8888): Streaming job orchestration
- **InfluxDB** (8086): Time-series database
- **Grafana** (3000): Pit-wall dashboard

Wait ~30s for services to stabilize:

```bash
docker-compose ps
```

### 2. Load and Replay a Race

The replayer streams F1 telemetry from FastF1 into Kafka. Default: 2023 Monaco GP.

```bash
docker-compose run replayer python -m replayer.main
```

Or, to replay a different race:

```bash
docker exec streaming-replayer-1 python -c \
  "from replayer.main import F1Replayer; r = F1Replayer(); r.run(2023, 'Silverstone')"
```

Topics created automatically:
- `f1.telemetry` (partitioned by driver)
- `f1.laps`
- `f1.weather`
- `f1.race_control`

### 3. Monitor Kafka

Open [Kafka UI](http://localhost:8080) to inspect topics and message flow.

### 4. Inspect Data

List Kafka topics:

```bash
docker exec streaming-kafka-1 kafka-topics --list --bootstrap-server localhost:9092
```

Read messages from `f1.telemetry`:

```bash
docker exec streaming-kafka-1 kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic f1.telemetry \
  --from-beginning \
  --max-messages 10
```

## Replayer Configuration

Edit `replayer/config.py` or set environment variables:

```bash
# Replay speed: 1.0 (real-time), 10.0 (10x speed), 50.0 (50x speed)
export REPLAY_SPEED=10.0

# F1 race cache directory
export F1_CACHE_DIR=/data/f1_cache

# Kafka bootstrap servers
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

## Service Endpoints

| Service      | URL                   | Credentials |
|--------------|-------------------------|-------------|
| Kafka UI     | http://localhost:8080   | None        |
| Spark Master | http://localhost:8888   | None        |
| InfluxDB     | http://localhost:8086   | admin/admin_password |
| Grafana      | http://localhost:3000   | admin/admin |

## Project Structure

```
streaming/
├── docker/
│   └── replayer/          # FastF1 → Kafka replayer Docker image
├── replayer/              # Replayer Python module
│   ├── main.py            # F1Replayer streaming logic
│   ├── config.py          # Configuration
│   └── __init__.py
├── spark-jobs/            # Spark Structured Streaming jobs
├── twin-model/            # PyTorch LSTM digital twin
├── dashboards/            # Grafana dashboard definitions
├── data/                  # Local cache (FastF1, InfluxDB)
├── docker-compose.yml     # Full stack orchestration
└── README.md              # This file
```

## Development

### Add a new Spark job

1. Create `spark-jobs/my_job.py`
2. Submit to Spark Master:

```bash
docker exec streaming-spark-master-1 \
  spark-submit --master spark://spark-master:7077 /opt/spark-apps/my_job.py
```

### Rebuild the replayer image

```bash
docker-compose build replayer
docker-compose up -d replayer
```

### Logs

View logs from all services:

```bash
docker-compose logs -f
```

View just Kafka:

```bash
docker-compose logs -f kafka
```

## Stopping the Stack

```bash
docker-compose down
```

To also delete data volumes:

```bash
docker-compose down -v
```

## Next Steps

- [ ] Implement Spark Structured Streaming jobs for feature engineering
- [ ] Build PyTorch digital twin with LSTM
- [ ] Integrate River drift detectors (ADWIN/DDM)
- [ ] Create Grafana dashboards
- [ ] Reproduce Hundman et al. baseline on NASA MSL data
- [ ] Fault injection and evaluation

## References

- **FastF1**: https://github.com/theOehrly/Fast-F1
- **Kafka**: https://kafka.apache.org/
- **Spark Structured Streaming**: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
- **River**: https://riverml.xyz/
- **Hundman et al. (KDD 2018)**: https://arxiv.org/abs/1802.04431
