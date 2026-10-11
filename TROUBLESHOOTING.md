# Troubleshooting Guide

## Problem: Docker Build Fails with FastF1 Error

**Error**: `Could not find a version that satisfies the requirement fastf1==3.3.10`

**Solution**: ✅ FIXED in v2
- Updated to `fastf1==3.8.3` (latest available)
- Files changed:
  - `docker/replayer/requirements.txt`
  - `requirements-dev.txt`

**To fix**: Rebuild with `--no-cache`:
```bash
docker-compose build --no-cache replayer
```

---

## Problem: Docker Build Fails with build-essential

**Error**: `E: Unable to locate package build-essential` or similar

**Solution**: Update Ubuntu packages in Dockerfile:
```bash
# Already fixed in Dockerfile
RUN apt-get update && apt-get install -y \
    git \
    build-essential \
    && rm -rf /var/lib/apt/lists/*
```

**To fix**: Rebuild:
```bash
docker-compose build --no-cache replayer
```

---

## Problem: "Command 'python' not found"

**Error**: `Command 'python' not found, did you mean 'python3'?`

**Cause**: Your system uses `python3` instead of `python`

**Solution**: Use the provided startup scripts (they handle this):
```bash
./run.sh                          # Recommended (auto-handles python3)
docker-compose run replayer ...   # Uses docker Python (correct version)
```

**If running manually**:
```bash
# ❌ DON'T do this:
python -m experiments.runner

# ✅ DO this instead:
python3 -m experiments.runner
# OR
docker-compose run replayer python -m replayer.main
```

---

## Problem: Kafka Takes Too Long to Start

**Symptom**: "Waiting for Kafka" hangs for >1 minute

**Cause**: Kafka cluster initialization (normal for first run)

**Solution**: Be patient, it's starting Zookeeper + Kafka
```bash
# Watch progress:
docker-compose logs -f kafka

# Should see:
# kafka_1      | [Kafka Server 1], started (kafka.server.KafkaServer)
```

**If still stuck after 2 minutes**:
```bash
# Restart Kafka
docker-compose restart kafka

# Check logs
docker-compose logs kafka | tail -30

# If still failing, try full restart
docker-compose down -v
docker-compose up -d
```

---

## Problem: Cannot Connect to Kafka

**Error**: `Failed to connect to Kafka at localhost:9092`

**Cause**: Docker networking issue or Kafka not fully started

**Solution**:
```bash
# Check if Kafka is running
docker-compose ps kafka

# Should show: "Up (healthy)" or just "Up"

# If not, restart
docker-compose restart kafka
sleep 30

# Try again
docker-compose logs kafka | grep -i "started\|ready\|listening"
```

---

## Problem: Replayer Crashes Immediately

**Error**: Docker logs show replayer exiting quickly

**Solutions**:

**1. Kafka not ready yet**
```bash
# Wait for Kafka
docker-compose logs -f kafka
# Wait for "started" message, then run replayer
docker-compose run replayer python -m replayer.main
```

**2. FastF1 download fails**
```bash
# Check cache is writable
ls -la data/f1_cache/
chmod 777 data/f1_cache/

# Retry
docker-compose run replayer python -m replayer.main
```

**3. Memory issues**
```bash
# Reduce replay speed (slower but less memory)
# Edit: replayer/config.py
replay_speed: float = 1.0  # Change from 10.0 to 1.0

# Rebuild and retry
docker-compose build replayer
docker-compose run replayer python -m replayer.main
```

---

## Problem: "No such file or directory: results/REPORT.md"

**Error**: `cat: results/REPORT.md: No such file or directory`

**Cause**: Experiments haven't been run yet

**Solution**: Run experiments first:
```bash
python -m experiments.runner
# Wait 15-20 minutes...

# Then view results
cat results/REPORT.md
```

**If you skipped this step**:
```bash
# Create results directory
mkdir -p results

# Run experiments
python -m experiments.runner

# Check progress
ls -lh results/
```

---

## Problem: InfluxDB/Grafana Not Accessible

**Symptoms**: 
- http://localhost:3000 → Connection refused
- http://localhost:8086 → Connection refused

**Solution**:
```bash
# Check if services are running
docker-compose ps grafana influxdb

# Both should show "Up"

# If not, restart
docker-compose restart grafana influxdb
sleep 10

# Try again
curl http://localhost:3000
curl http://localhost:8086
```

---

## Problem: Out of Memory (OOM Killer)

**Error**: Services killed randomly, `docker-compose logs` shows OOM

**Solutions**:

**1. Reduce Spark resources** (edit docker-compose.yml):
```yaml
spark-worker:
  environment:
    SPARK_WORKER_MEMORY: 1G    # was 2G
    SPARK_WORKER_CORES: 1      # was 2
```

**2. Stop non-essential services**:
```bash
# Keep only Kafka, replayer
docker-compose down
docker-compose up -d kafka replayer

# Skip Spark, InfluxDB, Grafana for now
```

**3. Replay fewer cars**:
```bash
# Edit replayer/main.py to sample cars
# Then rebuild
docker-compose build replayer
```

---

## Quick Health Check

```bash
# Is Docker running?
docker ps
# Should list containers

# Are all services up?
docker-compose ps
# All should show "Up"

# Is Kafka responsive?
docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092
# Should list topics (or be empty)

# Is Grafana accessible?
curl -u admin:admin http://localhost:3000/api/health
# Should return: {"status":"ok"}

# Is InfluxDB working?
curl http://localhost:8086/health
# Should return: {"status":"ok"}
```

---

## Common Commands

```bash
# View logs
docker-compose logs -f                    # All services
docker-compose logs -f replayer           # Just replayer
docker-compose logs -f kafka              # Just Kafka

# Restart a service
docker-compose restart kafka
docker-compose restart replayer

# Stop everything
docker-compose down                       # Keep volumes
docker-compose down -v                    # Delete volumes too

# Rebuild an image
docker-compose build --no-cache replayer

# Clean up
docker system prune -a                    # Remove unused images
docker volume prune                       # Remove unused volumes

# Check resource usage
docker stats
# Shows CPU, memory, network per container
```

---

## Still Stuck?

1. **Check logs first**:
   ```bash
   docker-compose logs --tail=50 <service_name>
   ```

2. **Try a clean restart**:
   ```bash
   docker-compose down -v
   docker-compose build --no-cache replayer
   docker-compose up -d
   sleep 60
   docker-compose logs kafka | grep started
   ```

3. **Verify Docker itself**:
   ```bash
   docker ps
   docker images
   docker version
   ```

4. **Check system resources**:
   ```bash
   df -h                    # Disk space
   free -h                  # RAM
   top                      # CPU usage
   ```

---

## Support

For persistent issues:
- Read `README.md` (architecture overview)
- Read `PROJECT_OVERVIEW.md` (complete guide)
- Check inline code docstrings
- Review `FINAL_REPORT.md` (technical details)

Good luck! 🚀
