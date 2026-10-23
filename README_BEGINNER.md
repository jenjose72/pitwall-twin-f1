# PitWall Twin — The Complete Beginner's Guide

This guide assumes you know **nothing** about Formula 1, Kafka, neural networks or streaming.
Every term is explained the first time it appears. Read it top to bottom once and you will be
able to explain the whole project in a viva.

> **Honesty note (read this first).** This project is a *working skeleton* of a real-time
> anomaly detection system. The pipeline pieces (replayer, Kafka, model, drift detectors,
> dashboard) exist as code. But the **experiment results in `results/REPORT.md` are
> placeholder numbers produced by a formula, not real measurements**. Section 12 lists exactly
> what is real, what is a stub, and what is broken. Never present the placeholder numbers as
> measured results.

---

## Table of contents

1. [What is this project, in one paragraph?](#1-what-is-this-project-in-one-paragraph)
2. [Background you need (F1, telemetry, anomalies)](#2-background-you-need)
3. [The core idea: the "digital twin" and the residual](#3-the-core-idea-the-digital-twin-and-the-residual)
4. [Why simple anomaly detection fails: drift](#4-why-simple-anomaly-detection-fails-drift)
5. [The project's two safeguards](#5-the-projects-two-safeguards)
6. [The technology stack explained](#6-the-technology-stack-explained)
7. [Architecture: how data flows](#7-architecture-how-data-flows)
8. [File-by-file walkthrough](#8-file-by-file-walkthrough)
9. [The four experiment configurations (B0, B1, E1, E2)](#9-the-four-experiment-configurations)
10. [A worked example, step by step](#10-a-worked-example-step-by-step)
11. [How to run it](#11-how-to-run-it)
12. [What is real, what is a stub, what is broken](#12-what-is-real-what-is-a-stub-what-is-broken)
13. [Viva questions and model answers](#13-viva-questions-and-model-answers)
14. [Glossary](#14-glossary)
15. [Possible future work](#15-possible-future-work)

---

## 1. What is this project, in one paragraph?

A Formula 1 car has hundreds of sensors that continuously report numbers (speed, engine RPM,
throttle position, brake pressure, gear). **PitWall Twin** imitates a pit-wall engineering
system that watches this stream in real time and raises an alert when a car behaves
abnormally (for example a failing sensor or a developing mechanical fault).
It does this by training a neural network (the **"twin"**) to predict what each sensor
should read next, and flagging cases where reality differs a lot from the prediction.
Because racing conditions change constantly (rain, safety cars, tyre wear), the system also has
a **drift detector** and a **fleet consensus** check so it does not raise false alarms
every time the whole race changes character.

Full name used in the docs: *"Drift-Aware Digital Twin for F1 Telemetry"*.

---

## 2. Background you need

### 2.1 Formula 1 in 60 seconds
- 20 cars (10 teams × 2 drivers) race around a circuit for about 1.5–2 hours.
- Each car has a **driver number** (e.g. 1, 44, 16). The project uses this to tell cars apart.
- **Pit stop**: a car drives into the pit lane to change tyres. During pit laps the car
  behaves very differently (slow, speed-limited) — this is *normal*, not a fault.
- **Safety car**: after a crash, a slow-moving official car leads the field and ALL cars slow
  down. Normal, not a fault.
- **Tyre compounds**: soft/medium/hard/intermediate/wet. Different grip, different behaviour.
- **Pit wall**: where the team's engineers sit and watch live data.

### 2.2 What is telemetry?
Telemetry = data sent remotely from a machine's sensors. In this project each telemetry
message from one car looks like this (see `replayer/main.py`):

```json
{
  "timestamp": "0 days 00:42:13.512000",
  "driver": "44",
  "speed": 287.0,      // km/h
  "rpm": 11800.0,      // engine revolutions per minute
  "throttle": 100.0,   // % pedal pressed
  "brake": 0.0,        // brake on/off
  "gear": 7,
  "drs": 12,           // Drag Reduction System flap state
  "x": 123.0, "y": -456.0, "z": 12.0,   // position on track
  "session_time": 2533.5
}
```
The project focuses on 5 **channels**: `speed, rpm, throttle, brake, gear`.

### 2.3 What is an anomaly?
An **anomaly** is a reading (or a run of readings) that is *unusual compared to normal
behaviour* — e.g. RPM stuck at one value because a sensor froze, or a sudden impossible spike.
Detecting anomalies early lets engineers react before a failure.

### 2.4 Where does the data come from?
**FastF1** is a free Python library that downloads real, historical F1 race data. The project
does not have a live car; it **replays** a past race (default: 2023 Monaco Grand Prix) as if it
were happening live. This is called a *replayer*.

---

## 3. The core idea: the "digital twin" and the residual

### 3.1 Digital twin
A **digital twin** is a software copy of a real thing that mimics its behaviour. Here the
"twin" is a neural network that has learned how a car's sensors normally evolve over time.

### 3.2 How the twin works (prediction)
- Input: the last **30** readings of all 5 channels (a "window" of history).
- Output: its best guess for the **next** reading of all 5 channels.
- The model is an **LSTM** (Long Short-Term Memory network). An LSTM is a type of neural
  network built for *sequences*: it reads values in order and keeps an internal "memory" of
  what it has seen, so it can learn patterns like "after braking hard, speed drops and gear
  goes down".

### 3.3 The residual — the heart of the project
```
residual = actual value − predicted value
```
- Small residual → the car did what the twin expected → **normal**.
- Large residual → the car did something the twin did NOT expect → **possible anomaly**.

Example: twin predicts speed 280 km/h, the car reports 281 → residual 1 (fine).
Twin predicts 280, car reports 0 → residual −280 (suspicious!).

This is the approach of **Hundman et al., KDD 2018 ("telemanom")**, a paper that used LSTMs to
find anomalies in NASA spacecraft telemetry. This project reproduces that idea on F1 data and
tries to improve it.

---

## 4. Why simple anomaly detection fails: drift

Suppose we train the twin once, on dry-weather data, and then use it all race. Then:
- It starts raining. Every car's behaviour changes (lower speeds, more wheel slip).
- The twin's predictions are now consistently off → residuals grow for *every* car.
- The system screams "anomaly!" on all 20 cars. These are **false alarms** — nothing is broken.

This slow or sudden change in the statistics of the data is called **concept drift**
(or just *drift*). Other causes: tyre wear, track temperature, fuel burning off,
safety cars, track "rubbering in".

Two naive fixes both have problems:
1. **Never update the model (B0)** → false alarms whenever conditions change.
2. **Always keep retraining the model on new data (B1)** → the model also learns *real faults*
   as "normal". If a sensor slowly degrades, the model adapts to the broken reading and
   stops warning you. That is dangerous.

---

## 5. The project's two safeguards

### Safeguard 1 — Drift gate (`twin-model/drift_gate.py`, class `DriftGate`)
Only update the model when there is **confirmed drift**, and **never while an anomaly is
active**.

How drift is detected, per channel, on the *absolute residual*:
1. **ADWIN-style check:** compare the average residual of the last 50 samples with the
   average of the first 100 (the "baseline"). If it changed by more than **5 %**, flag it.
   (*Real ADWIN* keeps an adaptive window; this project uses a simplified version.)
2. **Page-Hinkley check:** keep a running sum of how far each residual is from the baseline
   (in standard deviations). If the sum's size exceeds **5.0**, flag it. This catches steady
   upward/downward shifts quickly.
3. **Confirmation:** drift is only declared if **both** detectors fire, **5 times in a row**.
   This double-check reduces false positives.
4. **Freeze rule:** even if drift is confirmed, do **not** adapt if any recent residual is
   very large (> 2.0) — that looks like an active fault, and we must not teach the model that
   a fault is normal.

### Safeguard 2 — Fleet consensus (`FleetConsensus` class)
Ask: **"Is it just me, or is everyone affected?"**
- If **most cars** (> 70 %) show elevated residuals at the same time → probably a shared cause
  (rain, safety car) → label it `benign_regime_change` → **not a fault**.
- If **only one car** shows it → label it `individual_anomaly` → **investigate!**
- **Masking:** some car-specific situations are normal and are ignored (`masked_event`):
  pit-stop laps, being lapped by a faster car, driving closer than 1 second behind another car
  (turbulent air / traffic), and recent tyre-compound changes.

Together: the drift gate decides *"should the model learn from this?"* and the fleet check
decides *"is this a real problem or just the race changing?"*

---

## 6. The technology stack explained

| Tool | What it is | Role here |
|---|---|---|
| **Python** | Programming language | All the code |
| **Docker / Docker Compose** | Runs programs in isolated "containers"; Compose starts many at once from one file | Starts Kafka, Grafana, etc. with one command |
| **Apache Kafka** | A *message broker*: producers publish messages into named **topics**; consumers read them. Think of a durable, ordered conveyor belt. | Carries the live data between components |
| **Zookeeper** | Coordination service Kafka (this version) needs | Required by Kafka |
| **Kafka UI** | Web page to look inside Kafka | Debugging (port 8080) |
| **FastF1** | Library to download F1 data | Source of real telemetry |
| **Apache Spark Structured Streaming** | Engine for processing continuous data in time **windows** | Computes rolling statistics (feature engineering) |
| **PyTorch** | Deep-learning library | Builds/trains the LSTM |
| **River** (mentioned in docs) | Library for online learning/drift detection | Named in docs, but the code uses hand-written detectors instead |
| **InfluxDB 1.8** | *Time-series database* (stores values with timestamps) | Stores telemetry/residuals/alerts |
| **Grafana** | Dashboard tool | Draws the live graphs (port 3000) |

**Key Kafka words**
- **Topic**: a named channel, e.g. `f1.telemetry`.
- **Partition**: a topic is split into parts so work can be parallel. Messages with the same
  *key* (here: driver) go to the same partition, so **one car's messages stay in order**.
- **Producer / Consumer**: writer / reader of a topic.
- **Consumer group**: a team of consumers sharing a topic's work.

**Topics used**

| Topic | Contents | Written by |
|---|---|---|
| `f1.telemetry` | sensor readings per car | replayer |
| `f1.laps` | lap numbers, tyre compound & age, pit in/out times | replayer |
| `f1.weather` | air/track temp, humidity, wind, rain flag | replayer |
| `f1.race_control` | official messages/flags (e.g. safety car) | replayer |
| `f1.features` | rolling statistics | Spark job |
| `f1.residuals` | actual, predicted, residual | twin service |
| `f1.alerts` | drift events and results | twin service |

---

## 7. Architecture: how data flows

```
 ┌───────────────┐
 │ FastF1 (past  │
 │ race data)    │
 └──────┬────────┘
        ▼
 ┌───────────────┐   writes   ┌──────────────────────────────────┐
 │   REPLAYER    │──────────► │  KAFKA  topics:                  │
 │ (replays race │            │  f1.telemetry / f1.laps /        │
 │  at 1x–50x)   │            │  f1.weather / f1.race_control    │
 └───────────────┘            └───────┬───────────────┬──────────┘
                                      │               │
                  ┌───────────────────▼──┐   ┌────────▼───────────────────────┐
                  │ SPARK feature job    │   │ TWIN SERVICE                   │
                  │ 10-sec windows,      │   │ 1. keep last 30 readings/car   │
                  │ joins laps+weather   │   │ 2. LSTM predicts next values   │
                  │ → f1.features        │   │ 3. residual = actual−predicted │
                  └──────────────────────┘   │ 4. DriftGate (E1/E2)           │
                                             │ 5. FleetConsensus (E2)         │
                                             │ → f1.residuals, f1.alerts      │
                                             └────────┬───────────────────────┘
                                                      ▼
                                             ┌─────────────────┐
                                             │ KAFKA→INFLUXDB  │
                                             │ bridge          │
                                             └────────┬────────┘
                                                      ▼
                                             InfluxDB ──► GRAFANA dashboard
```

---

## 8. File-by-file walkthrough

```
repo/
├── docker-compose.yml            # defines the services to start
├── replayer/
│   ├── main.py                   # F1Replayer: FastF1 → Kafka
│   └── config.py                 # settings (topics, speed, cache dir)
├── spark-jobs/feature_engineering.py   # Spark streaming job
├── twin-model/
│   ├── lstm_twin.py              # the LSTM twin
│   ├── drift_gate.py             # DriftGate + FleetConsensus
│   ├── fault_injection.py        # fake faults for testing
│   └── twin_service.py           # the live loop tying it together
├── experiments/runner.py         # runs B0/B1/E1/E2 × 4 scenarios
├── kafka_to_influxdb_http.py     # Kafka → InfluxDB bridge
├── mock_data_generator*.py       # fake data written straight to InfluxDB
├── dashboards/                   # Grafana dashboard JSON + provisioning
├── results/                      # results.json, REPORT.md (placeholder numbers!)
├── tests/test_replayer.py        # checks FastF1 can download a race
└── *.md                          # assorted docs/status files
```

### 8.1 `replayer/main.py` — `F1Replayer`
1. `load_session(year, gp)` downloads the race with FastF1 (cached locally in `data/f1_cache`).
2. `stream_laps / stream_weather / stream_race_control` send those tables to Kafka.
3. `stream_telemetry` loops over each driver's rows. For each row it **sleeps** so that
   timing matches the original race divided by `replay_speed` (1.0 = real time, 10.0 = ten
   times faster). It then sends a JSON message to `f1.telemetry` with
   `partition = driver_number % 20` and `key = driver`.
4. `acks='all'` means Kafka confirms the message was safely stored.

### 8.2 `replayer/config.py`
A `Settings` object (via `pydantic-settings`) holding Kafka address, cache folder,
`replay_speed`, and topic names. Values can be overridden by environment variables or `.env`.

### 8.3 `spark-jobs/feature_engineering.py`
- Reads `f1.telemetry`, `f1.laps`, `f1.weather` as **streams**.
- Defines schemas (column names/types) and parses JSON.
- Uses a **watermark** (e.g. "wait up to 10 s for late data") — a streaming concept for
  handling out-of-order messages.
- **Joins** telemetry with the latest lap info and weather so each reading knows the tyre
  compound, tyre age and rain status.
- Computes **sliding-window statistics** (window 10 s, slide 1 s) per driver: mean/std of
  speed and RPM, mean throttle/brake, track temperature, current lap, tyre info.
- Writes the result to `f1.features`.

### 8.4 `twin-model/lstm_twin.py`
- `LSTMTelemetryTwin` (the neural net):
  - **Car embedding**: each car ID (0–19) maps to a learned 16-number vector, so the single
    model can learn each car's personality.
  - The 5 channels + the embedding go into a **2-layer LSTM** (128 hidden units, 20 % dropout).
  - The last hidden state passes through a small **MLP** (128 → 64 → 5) producing predictions
    for all 5 channels.
  - *v2 design choice:* one **shared** model instead of 5 channels × 20 cars = 100 models.
- `TelemetryTwin` (the wrapper):
  - `normalize/denormalize`: z-score scaling `(x − mean)/std` so channels with huge numbers
    (RPM ≈ 12 000) don't swamp small ones (throttle).
  - `fit(...)`: slides a 30-step window over history to make (input, next-value) pairs, trains
    with **Adam** optimiser and **MSE** (mean squared error) loss.
  - `predict(history, car_id)`, `get_residuals(actual, predicted)`, `save/load`.

### 8.5 `twin-model/drift_gate.py`
`DriftGate` and `FleetConsensus` exactly as described in section 5.
`DriftEvent` is a small record (timestamp, channel, detector, severity, description).

### 8.6 `twin-model/twin_service.py` — `TwinService(mode)`
The live loop:
1. Consume messages from `f1.telemetry`.
2. Append values to that driver's rolling history (max 100 points).
3. When there are ≥ 30 points on all channels: predict → residuals → publish to
   `f1.residuals`.
4. If mode is `E1` or `E2`: run `DriftGate`; publish drift alerts; count adaptations.
5. If mode is `E2`: also run `FleetConsensus` and attach the classification.
`mode` is chosen on the command line: `B0`, `B1`, `E1`, `E2`.

### 8.7 `twin-model/fault_injection.py`
Creates **fake faults with known start/end times**, so you know the ground truth:
- `STUCK_SENSOR` — value frozen
- `SPIKE` — sudden jump
- `DROPOUT` — value missing
- `GAIN_DRIFT` — value gradually scaled up
`ScenarioBuilder` bundles presets (mechanical failure, sensor fault, transient issue, slow ramp).

### 8.8 `experiments/runner.py`
Defines result dataclasses (precision, recall, F1, F0.5, detection delay, false alarms/hour,
latency, throughput, memory) and `ExperimentRunner`, which loops 4 configs × 4 scenarios
and writes `results/results.json` and `results/REPORT.md`.
**Caution:** it currently *simulates* the numbers (see section 12).

### 8.9 `kafka_to_influxdb_http.py`
Reads `f1.telemetry`, `f1.residuals`, `f1.alerts`, converts each message into InfluxDB
"line protocol" text, and POSTs batches of 100 to InfluxDB over HTTP.

### 8.10 `dashboards/`
Grafana dashboard definitions (speed/RPM/throttle/brake traces, residual heatmap, anomaly
events) and provisioning files that auto-connect Grafana to InfluxDB.

---

## 9. The four experiment configurations

| Config | Name | Model updates? | Drift detection | Fleet consensus |
|---|---|---|---|---|
| **B0** | Static baseline | Never (trained once) | No | No |
| **B1** | Continuous adaptation | Always | No | No |
| **E1** | Drift-gated | Only on confirmed drift & no active anomaly | Yes | No |
| **E2** | Full proposal | Same as E1, plus ignores fleet-wide benign changes | Yes | Yes |

(B = baseline, E = enhancement.)

**Scenarios**
- **Dry race** – normal conditions.
- **Wet race** – rain changes behaviour for everyone.
- **Safety cars** – all cars slow together.
- **Injected faults** – synthetic faults with known ground truth.

**Metrics (plain English)**
- **Precision**: of all alerts raised, what fraction were real anomalies? (low = many false alarms)
- **Recall**: of all real anomalies, what fraction did we catch? (low = we miss problems)
- **F1**: balance of precision and recall. **F0.5**: same but weights precision more.
- **Detection delay**: time from anomaly start to alert.
- **False alarms per hour**, **throughput** (messages/s), **latency**, **memory**.

**Expected hypothesis:** B0 < B1 < E1 < E2 in precision/recall, with E2 best especially in
wet-race and safety-car scenarios. *This is the hypothesis — not yet a measured result.*

---

## 10. A worked example, step by step

Race is replayed. Driver 44's car sends readings. Rolling history for driver 44 reaches 30
points on all 5 channels.

1. **Predict.** LSTM says next speed ≈ 250, RPM ≈ 11 500, throttle ≈ 80, brake ≈ 0, gear ≈ 6.
2. **Actual** arrives: speed 252, RPM 11 480, ... → tiny residuals → publish to `f1.residuals`.
3. Later, a **safety car** deploys. All 20 cars slow. Residuals rise for everyone.
   - `DriftGate`: baseline vs. recent residual mean differs > 5 % and Page-Hinkley sum > 5, five
     times in a row → **drift confirmed**.
   - `FleetConsensus` (E2): > 70 % of cars elevated → `benign_regime_change` → *not a fault*.
4. Separately, only driver 16's RPM sensor sticks. Only that car's residual rises →
   `individual_anomaly` → **real alert**.
5. If driver 44 were in the pits, `is_masked` would be true and his residuals would be ignored.

---

## 11. How to run it

**Prerequisites:** Docker + Docker Compose, ~4 GB free RAM, Python 3.10+ (for local tests).

```bash
# 1. Start infrastructure (Kafka, Zookeeper, Kafka UI, InfluxDB, Grafana, bridge, replayer)
docker-compose up -d
docker-compose ps                    # wait ~30–60 s until all are Up

# 2. Replay a race into Kafka (default 2023 Monaco)
docker-compose run replayer python -m replayer.main
#   Faster: set REPLAY_SPEED=10 (env var or replayer/config.py)

# 3. Look around
#   Kafka UI : http://localhost:8080
#   Grafana  : http://localhost:3000   (admin / admin)
#   InfluxDB : http://localhost:8086

# 4. Run the twin service (from inside the twin-model folder)
cd twin-model && python twin_service.py E2     # modes: B0 B1 E1 E2

# 5. Run the experiment harness (writes results/; numbers are simulated!)
python -m experiments.runner
```

Test that FastF1 works without Docker:
```bash
pip install -r requirements.txt
python tests/test_replayer.py
```

Stop everything: `docker-compose down` (add `-v` to also delete stored data).

---

## 12. What is real, what is a stub, what is broken

| Area | Status |
|---|---|
| Docker stack (Kafka, InfluxDB, Grafana, Kafka UI) | ✅ Real, defined in `docker-compose.yml` |
| Replayer (FastF1 → Kafka, timed replay) | ✅ Real code |
| LSTM architecture with car-ID embedding | ✅ Real code |
| Drift gate & fleet consensus logic | ✅ Written; hand-rolled (not River) |
| Fault injector | ✅ Real code (stuck-sensor injection is incomplete: the stuck value is never applied) |
| Spark feature job | ⚠️ Written but Spark services are commented out of the compose file, so it isn't part of the running stack |
| Grafana dashboards | ⚠️ Exist; status notes admit panels may show "No data" |
| **Experiment results** | ❌ **Simulated.** `_simulate_experiment()` in `runner.py` computes numbers from fixed constants (base precision 0.85, recall 0.80, plus fixed bonuses per config). The code never runs the twin. |
| **NASA SMAP/MSL baseline reproduction** | ❌ No code loads this data; figures quoted in older docs have nothing behind them |
| **Model training in the live service** | ❌ `twin_service.py` never calls `fit()`, so predictions use random weights |
| **"Adaptation"** | ❌ Only logs a message; no retraining happens |
| "ADWIN / DDM from River" | ❌ Docs say so; code uses simplified hand-written checks |
| `drift_gate.py` | ❌ Uses `np.std` without `import numpy as np` → crashes when drift check runs |
| Imports | ⚠️ `twin_service.py` imports `lstm_twin` directly, so it must be run from inside `twin-model/` (folder name has a hyphen) |
| Real data → Grafana | ⚠️ Bridge does `float(timestamp)`, which fails on the replayer's text timestamps; dashboards were populated with mock data from `mock_data_generator*.py` |
| Replayer concurrency | ⚠️ Streams drivers one after another, not all 20 simultaneously |

**How to describe the project honestly:**
*"I built the streaming architecture and the model and drift/consensus components. The
evaluation harness is a scaffold with placeholder numbers; running real experiments is the
next step."*

---

## 13. Viva questions and model answers

**Q: What problem does the project solve?**
Detecting abnormal behaviour in F1 car telemetry in real time, without raising false alarms
when race conditions (rain, safety car, tyre wear) change.

**Q: What is a digital twin?**
A software model that mimics a real system. Here, an LSTM that predicts a car's next sensor
readings.

**Q: How do you detect an anomaly?**
Compute the residual (actual − predicted). A large residual means the car did something the
model did not expect.

**Q: Why an LSTM?**
It is designed for sequences and remembers recent history; it matches the Hundman et al.
baseline (fair comparison); and it is lightweight enough for real-time use.

**Q: Why one shared model with car embeddings?**
One model instead of 100 (5 channels × 20 cars) – cheaper to train/run – while the embedding
lets it capture each car's own characteristics.

**Q: What is drift and why does it matter?**
Drift is a change in the data's statistics over time. It makes a static model's residuals grow
for reasons that are not faults, causing false alarms.

**Q: Why not just retrain constantly?**
The model would learn real faults as normal and stop detecting them. Hence drift *gating*.

**Q: Why two drift detectors?**
ADWIN-style comparison catches distribution changes; Page-Hinkley catches steady shifts
quickly. Requiring both (5 times in a row) reduces false positives.

**Q: What does the freeze rule do?**
Blocks model adaptation while any recent residual is large, so the model does not learn a
developing fault.

**Q: What is fleet consensus?**
Checking whether many cars show the shift together. Fleet-wide shifts (safety car, rain) are
benign; a single car's shift is a likely real anomaly. Pit laps, lapped cars, traffic and tyre
changes are masked.

**Q: Why Kafka? Why partition by driver?**
Kafka decouples producers from consumers, buffers data and scales. Partitioning by driver keeps
each car's readings in order and allows parallel processing.

**Q: What does Spark do here?**
Computes rolling-window features and joins telemetry with lap and weather context in a
streaming way.

**Q: What are B0, B1, E1, E2?**
Static model; always-retrain model; drift-gated retraining; drift-gated plus fleet consensus.

**Q: What results did you get?**
Be honest: the harness exists and its output is currently simulated; real measured evaluation
is future work. (See section 12.)

**Q: What are the limitations?**
Model not trained in the live service; adaptation not implemented; simplified drift detectors;
evaluation uses placeholder numbers; slow-developing faults can resemble drift; no real labelled
F1 fault data.

**Q: How would you improve it?**
Train on real FastF1 data, implement retraining, use River's ADWIN, run the fault injector
through the live pipeline and measure true precision/recall, handle all 20 cars concurrently.

---

## 14. Glossary

- **Adam**: popular optimiser that adjusts neural-net weights during training.
- **ADWIN**: Adaptive Windowing, a drift detector comparing recent vs. older data.
- **Anomaly**: unusual behaviour compared with normal.
- **Channel**: one measured signal (speed, RPM, ...).
- **Concept drift**: change in data statistics over time.
- **Consumer / Producer**: reads / writes Kafka messages.
- **DRS**: Drag Reduction System, a movable rear-wing flap used for overtaking.
- **Embedding**: learned vector representing a category (here, a car).
- **F1 score**: harmonic mean of precision and recall.
- **False alarm / false positive**: alert when nothing is wrong.
- **Feature engineering**: turning raw data into useful summaries (rolling means, etc.).
- **Hundman et al. (2018)**: paper using LSTMs for spacecraft telemetry anomaly detection.
- **LSTM**: neural network for sequences with memory.
- **MSE**: mean squared error, the loss used for training.
- **Normalisation (z-score)**: rescale data to mean 0, standard deviation 1.
- **Page-Hinkley**: change detector based on cumulative deviation.
- **Partition**: subdivision of a Kafka topic.
- **Precision / Recall**: fraction of alerts that are correct / fraction of anomalies caught.
- **Residual**: actual − predicted.
- **Safety car**: slow lead car that bunches the field after an incident.
- **Telemetry**: remotely transmitted sensor data.
- **Topic**: named Kafka channel.
- **Watermark**: streaming setting for how long to wait for late data.
- **Window (sliding)**: fixed-length slice of recent data moved along the stream.

---

## 15. Possible future work

1. Replace the simulated `_simulate_experiment()` with real runs: train the LSTM on real
   FastF1 laps, replay races, inject faults, and measure true precision/recall per config.
2. Actually retrain the model when `should_adapt` is true.
3. Use River's real ADWIN/Page-Hinkley (or fix the imports in the current implementation).
4. Re-enable Spark in Docker and feed `f1.features` into the model.
5. Stream all 20 drivers concurrently, ordered by session time.
6. Fix the bridge's timestamp parsing so real data reaches Grafana.
7. Validate the baseline on NASA SMAP/MSL data before claiming a reproduction.
