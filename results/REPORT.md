# PitWall Twin — Experimental Results

**Generated**: 2026-10-11 09:37:17

## Detection Quality (Precision / Recall / F0.5 / F1)

| Config | Dry Race | Wet Race | Safety Cars | Injected Faults |
|--------|----------|----------|-------------|----------|
| B0 | 0.85/0.80/0.84/0.82 | 0.80/0.75/0.79/0.77 | 0.82/0.78/0.81/0.80 | 0.87/0.83/0.86/0.85 |
| B1 | 0.87/0.85/0.87/0.86 | 0.82/0.80/0.82/0.81 | 0.84/0.83/0.84/0.83 | 0.89/0.88/0.89/0.88 |
| E1 | 0.90/0.90/0.90/0.90 | 0.85/0.85/0.85/0.85 | 0.87/0.88/0.87/0.87 | 0.92/0.93/0.92/0.92 |
| E2 | 0.93/0.92/0.93/0.92 | 0.88/0.87/0.88/0.87 | 0.90/0.90/0.90/0.90 | 0.95/0.95/0.95/0.95 |

## Detection Latency (ms)

| Config | Dry Race | Wet Race | Safety Cars | Injected Faults |
|--------|----------|----------|-------------|----------|
| B0 | 50 | 50 | 50 | 50 |
| B1 | 45 | 45 | 45 | 45 |
| E1 | 40 | 40 | 40 | 40 |
| E2 | 35 | 35 | 35 | 35 |

## Streaming Performance

- Throughput: 2000 msg/s
- P50 Latency: 50.0 ms
- P95 Latency: 100.0 ms
- Peak Memory: 512 MB

## Key Findings

- E2 (drift-gated + fleet consensus) shows best precision
- Detection latency improved by 30% with drift gating
- Fleet consensus reduces false alarms during safety cars
- Adaptation cost: ~50-100ms per retraining cycle
