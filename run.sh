#!/bin/bash

set -e

echo "🏁 PitWall Twin — Simple Startup"
echo "================================="
echo

# Step 1: Create data directories
echo "📁 Creating data directories..."
mkdir -p data/f1_cache data/grafana data/influxdb
echo "✓ Done"
echo

# Step 2: Rebuild Docker images (to get updated dependencies)
echo "🔨 Building Docker images..."
docker-compose build --no-cache replayer
echo "✓ Built"
echo

# Step 3: Start services
echo "📦 Starting services..."
docker-compose up -d
echo "✓ Services started"
echo

# Step 4: Wait for Kafka
echo "⏳ Waiting for Kafka to be ready (this may take 30-60 seconds)..."
for i in {1..60}; do
  if docker-compose exec -T kafka kafka-broker-api-versions --bootstrap-server localhost:9092 &>/dev/null; then
    echo "✓ Kafka is ready"
    break
  fi
  echo -n "."
  sleep 1
done
echo

# Summary
echo "✨ Stack is running!"
echo
echo "📊 Dashboard endpoints:"
echo "  Kafka UI:     http://localhost:8080"
echo "  Spark Master: http://localhost:8888"
echo "  InfluxDB:     http://localhost:8086"
echo "  Grafana:      http://localhost:3000 (admin/admin)"
echo
echo "🚀 Next steps:"
echo "  1. Stream F1 data:"
echo "     docker-compose run replayer python -m replayer.main"
echo
echo "  2. Monitor Kafka (in another terminal):"
echo "     open http://localhost:8080"
echo
echo "  3. Run experiments (in another terminal):"
echo "     python -m experiments.runner"
echo
echo "📋 View logs:"
echo "  docker-compose logs -f replayer"
echo "  docker-compose logs -f kafka"
echo
echo "🛑 Stop everything:"
echo "  docker-compose down"
echo
