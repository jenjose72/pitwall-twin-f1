# Quick Start Guide — PitWall Twin

## 5-Minute Setup

### 1. Build and start the stack

```bash
cd streaming
./start.sh
```

This will:
- Create data directories
- Start Kafka, InfluxDB, Grafana, Spark
- Wait for Kafka to be ready
- Display endpoints

### 2. Start replaying F1 telemetry

In a new terminal:

```bash
docker-compose run replayer python -m replayer.main
```

This streams 2023 Monaco GP data into Kafka at 1x speed (real-time). To change the race:

```bash
# Edit replayer/main.py, line 176:
# replayer.run(2023, 'Monaco')  → replayer.run(2023, 'Silverstone')
```

### 3. Check data flow

Open **Kafka UI**: http://localhost:8080
- Navigate to **Topics**
- You'll see:
  - `f1.telemetry` (20 partitions, one per driver)
  - `f1.laps`
  - `f1.weather`
  - `f1.race_control`

### 4. Monitor locally (no Docker)

Test the replayer without Docker (requires FastF1 installed):

```bash
pip install fastf1 kafka-python pydantic pydantic-settings
python tests/test_replayer.py
```

This verifies FastF1 can download race data and lists available telemetry.

## Configuration

### Replay Speed

Set `REPLAY_SPEED` in `replayer/config.py`:
- `1.0` = real-time (takes ~2h for a race)
- `10.0` = 10x faster
- `50.0` = 50x faster

### Select a Different Race

Modify `replayer/main.py` line 176:

```python
replayer.run(year=2023, gp='Silverstone')  # British GP
```

Available races: any F1 race from 2018 onward via FastF1.

### Kafka Bootstrap Servers

Default is `kafka:29092` (Docker network). If running Kafka elsewhere, update:

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

## Endpoints

| Service          | URL                   |
|------------------|-----------------------|
| **Kafka UI**     | http://localhost:8080 |
| **Spark Master** | http://localhost:8888 |
| **InfluxDB**     | http://localhost:8086 |
| **Grafana**      | http://localhost:3000 |

## Logs

See what's happening:

```bash
docker-compose logs -f replayer
docker-compose logs -f kafka
docker-compose logs -f spark-master
```

## Stop the Stack

```bash
docker-compose down
```

To also clear data:

```bash
docker-compose down -v
```

## Troubleshooting

**Kafka fails to start**
- Wait 30s and check: `docker-compose ps`
- Logs: `docker-compose logs kafka`

**Replayer crashes**
- Check logs: `docker-compose logs replayer`
- Verify Kafka is running: `docker-compose ps kafka`

**Out of memory**
- Reduce Spark worker cores in `docker-compose.yml`
- Or increase Docker memory limit

## What's Next?

- [ ] Build Spark Streaming jobs for feature engineering
- [ ] Implement PyTorch LSTM digital twin
- [ ] Add River drift detectors
- [ ] Create Grafana dashboards
