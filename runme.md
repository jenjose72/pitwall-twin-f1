# PitWall Twin v2 - Complete Working Setup

## **The ONE Command**

```bash
cd /home/jen/Desktop/IIIT/streaming && source venv/bin/activate && docker-compose down && docker-compose up -d && sleep 40 && curl -X POST http://localhost:8086/query --data-urlencode "q=DROP DATABASE f1" 2>/dev/null && curl -X POST http://localhost:8086/query --data-urlencode "q=CREATE DATABASE f1" 2>/dev/null && docker-compose run --rm kafka-producer python3 kafka_mock_producer_live.py && sleep 20 && echo "✓ DONE - Open http://localhost:3000/d/pitwall-twin-f1/ (dashboard auto-refreshes every 10s, no manual time picker needed)"
```

That's it. No manual Grafana clicking required — the dashboard is saved with:
- `$timeFilter`-based queries (so the time picker, if you touch it, actually works)
- Default range `now-24h` to `now+1h` (absorbs any container/host clock drift)
- Auto-refresh every 10s

## **What Was Actually Broken (for reference)**

1. **Queries hardcoded their own time window** (`now() - 2h`) instead of using Grafana's `$timeFilter` — so clicking the time picker never changed anything.
2. **Producer timestamped points into the future** — each run generated data from `now` to `now+5min`, so right after running, most of it wasn't visible yet in any backward-looking window.
3. **Container/host clock drift (~2 min)** — the kafka-producer container's clock can run slightly ahead of the host, which is why the dashboard range now pads `+1h` into the future.
4. **InfluxDB database name mismatch** — kafka-bridge was writing to `pitwall`, we were querying `f1`. Fixed in `kafka_to_influxdb_http.py`.
5. **Timestamp parsing crash** — kafka-bridge tried `float()` on an ISO timestamp string. Fixed to parse with `datetime.fromisoformat`.
6. **Explicit Kafka partition assignment silently dropped messages** for drivers outside partition count — removed, now Kafka handles partitioning by key.
7. **Producer only sent telemetry, never residuals or alerts** — the Anomaly Detection panel had nothing to query. Producer now also sends `f1.residuals` (every point) and `f1.alerts` (~3% of points, random severity 0.5-1.0) so all 5 panels have data.

## **Data Flow**

```
kafka_mock_producer_live.py (20 drivers, UTC timestamps ending at "now")
  → Kafka (f1.telemetry topic)
  → kafka-bridge (consumes, writes line-protocol to InfluxDB)
  → InfluxDB (f1 database)
  → Grafana (auto-refreshing dashboard)
```

## **Re-running**

Every time you run the one command above, it wipes the `f1` database and pushes a fresh batch of 6,000 points (20 drivers × 300s) through Kafka. Just wait for the `✓ DONE` line, then open Grafana — no time-picker fiddling needed.
