#!/bin/bash

set -e

echo "🏁 PitWall Twin — Starting Docker stack..."
echo

# Create data directories
mkdir -p data/f1_cache data/grafana data/influxdb

# Start services
echo "📦 Starting services..."
docker-compose up -d

# Wait for Kafka to be ready
echo "⏳ Waiting for Kafka to be ready..."
for i in {1..30}; do
  if docker-compose exec -T kafka kafka-broker-api-versions --bootstrap-server localhost:9092 &>/dev/null; then
    echo "✓ Kafka is ready"
    break
  fi
  echo -n "."
  sleep 1
done

echo
echo "✓ Stack is running!"
echo
echo "📊 Dashboard endpoints:"
echo "  Kafka UI:     http://localhost:8080"
echo "  Spark Master: http://localhost:8888"
echo "  InfluxDB:     http://localhost:8086"
echo "  Grafana:      http://localhost:3000"
echo
echo "🚀 To start replaying F1 data:"
echo "  docker-compose run replayer python -m replayer.main"
echo
echo "📋 To check service status:"
echo "  docker-compose ps"
echo
echo "🔍 To view logs:"
echo "  docker-compose logs -f"
