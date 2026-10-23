# PitWall Twin v2 — Presentation Guide

Slide-by-slide speaking guide for `PitWall_Twin_v2_Presentation.pptx` (11 slides).

Each slide has three layers. Use as many as your audience needs:

- **L1 — The story:** one or two plain sentences. Anyone can follow.
- **L2 — The idea:** what is actually happening, for a CS-literate audience.
- **L3 — Under the hood:** the technical detail, taken from the code in this repo.

Each slide ends with a **Reality check**: where the code differs from what the slide says. Read these before Q&A. Section 3.14 of [PROJECT_EXPLAINED.md](PROJECT_EXPLAINED.md) has the full status table.

---

## The whole talk in 30 seconds

> An F1 car streams hundreds of readings a second, and 20 cars race at once. No human can watch all of it. We replay real race data as a live stream, teach one neural network what "normal" looks like for every car, and flag moments where reality surprises it. Two helpers stop false alarms. A **drift gate** asks whether the world changed or the car broke. **Fleet consensus** asks whether one car or all cars are acting strangely.

**Core vocabulary (say these consistently):**

| Term | Plain meaning |
|---|---|
| Telemetry | Sensor readings from the car: speed, RPM, throttle, brake, gear |
| Digital twin | A model of a healthy car that predicts what it should do next |
| Residual | Actual − predicted. The size of the twin's "surprise" |
| Drift | The world changed (rain, tyre wear, track grip), so "normal" moved |
| Benign event | Odd-looking but harmless (pit stop, traffic) |

---

## Slide 1 — Title: PitWall Twin v2

**L1 — Story.** "The pit wall is where F1 engineers watch live car data. We built an automatic assistant for that pit wall."

**L2 — Idea.** Introduce the team and the one-line pitch: *F1 telemetry anomaly detection*. "v2" signals an improvement over an earlier design. Slide 4 explains the improvement.

**L3 — Technical.** Nothing to add. Mention the stack in one breath: Kafka → LSTM → drift detection → InfluxDB → Grafana. Slide 3 shows it.

**Tip:** Say what "v1 vs v2" means now, so slide 4 lands: v1 used 100 small models, v2 uses one shared model.

---

## Slide 2 — Problem Statement & Motivation

**L1 — Story.** "Twenty cars send many signals many times a second. Engineers can't watch it all, so we need software to spot trouble early. The hard part is that most weird-looking moments are not problems."

**L2 — Idea.** The four issues on the slide are really two problems:

1. **Too much data, too fast.** Scale and latency (bullets 3 and 4).
2. **Too many false alarms.** A pit stop makes speed drop sharply, and a car in traffic is slow. Both look like faults but aren't (bullet 1). The conditions also change over a race: rain, tyre wear, grip building up (bullet 2). That change is called **distribution shift** or **concept drift**.

The dilemma to state clearly: if you retrain the model constantly to follow the changes, it also learns to accept a *slowly failing sensor* as normal. If you never retrain, it raises false alarms as conditions change. The project is about navigating that trade-off.

**L3 — Technical.**
- Anomaly detection here is **prediction-error based**: predict the next value, then threshold the residual. This follows the NASA "Telemanom" approach (Hundman et al., 2018).
- Drift detection is the field of online change detection. The project uses ADWIN and Page-Hinkley.
- The research question the code is set up to test: *does gated, fleet-aware adaptation beat simpler strategies?* The four strategies are:
  - B0: never retrain.
  - B1: always retrain.
  - E1: retrain only on confirmed drift.
  - E2: E1 plus fleet consensus.

**Reality check.** "Sub-second latency" is a design target. The pipeline end to end has not been benchmarked (see slide 8).

---

## Slide 3 — System Architecture

Flow on slide: **FastF1 → Kafka → LSTM → Drift Detection → InfluxDB → Grafana**, with "20 Partitions by Driver" and "Grafana (6 panels)".

**L1 — Story.** "A conveyor belt. Race data goes in one end, a model checks each car, and results show up as live charts at the other end."

**L2 — Idea.** Walk left to right with a real-world analogy:

| Box | Role | Analogy |
|---|---|---|
| FastF1 | Source of real recorded race data | The race |
| Replayer → Kafka | Plays the old race back "as if live" onto a message highway | Live broadcast |
| LSTM | Predicts each car's next moment | The expert who knows normal |
| Drift detection | Decides whether surprise means a fault or a world change | Referee |
| InfluxDB | Time-stamped storage | Logbook |
| Grafana | Live charts | The pit-wall screens |

Why Kafka in the middle: every stage only talks to the log. You can restart, scale or replace any stage on its own, and several consumers can read the same stream.

**L3 — Technical.**
- **FastF1** is a Python library that returns pandas DataFrames for telemetry, laps, weather and race-control messages. [replayer/main.py](replayer/main.py) loads a session, publishes laps/weather/messages first, then walks telemetry rows. It sleeps between rows so publish time follows the original timeline ÷ `REPLAY_SPEED`.
- **Kafka topics:** `f1.telemetry` (keyed by driver so each car's order is kept), `f1.laps`, `f1.weather`, `f1.race_control`, plus `f1.residuals` and `f1.alerts` from the twin service.
- **Partitioning:** the same key always goes to the same partition. That keeps per-car ordering while allowing parallel consumers. 20 partitions for 20 drivers is the intended design.
- **Storage bridge** ([kafka_to_influxdb_http.py](kafka_to_influxdb_http.py)): consumes the topics, converts JSON to InfluxDB **line protocol**, and batches 100 lines per HTTP POST to `/write?db=pitwall`.
- **Optional Spark job** ([spark-jobs/feature_engineering.py](spark-jobs/feature_engineering.py)): 10-second sliding windows advancing every 1 second, per driver. Not on the slide.

**Reality check (expect questions).**
- The **Spark stage is not deployed**, and the diagram correctly omits it.
- The topic is **not auto-created with 20 partitions**. Auto-create gives 1 partition. Either create the topic explicitly with 20, or say "designed for 20".
- The twin service ([twin-model/twin_service.py](twin-model/twin_service.py)) is **not part of the running compose stack**. It also crashes at init (`drift_threshold` argument doesn't exist).
- Real replayer timestamps fail to parse in the bridge, so the live dashboard is fed today by [mock_data_generator.py](mock_data_generator.py).

---

## Slide 4 — Core: Shared LSTM Architecture

Slide: v1 = 100 models, ~500K parameters; v2 = 1 model, ~150K parameters, with 16-dim car-ID embeddings and a joint 5-channel output.

**L1 — Story.** "Instead of hiring 100 specialists, one for each sensor on each car, we hired one expert who has seen every car and knows which car they are looking at."

**L2 — Idea.**
- v1: 5 signals × 20 cars = **100 tiny models**. Heavy to run, and none of them learn from each other.
- v2: **one model** predicts all 5 signals together. Signals are physically linked: RPM follows gear and speed, and throttle and brake are opposites. Seeing them together gives better predictions.
- Cars differ a little, so each car gets a short learned "ID card" (the **embedding**: 16 numbers). The model uses it to adjust for that car's quirks.
- Results: less memory, one file to deploy, faster inference.

**L3 — Technical.** Architecture in [twin-model/lstm_twin.py](twin-model/lstm_twin.py):

```
input x:  (batch, 30 timesteps, 5 channels)   car_id: (batch,)
car_id → Embedding(20, 16) → repeat across 30 steps → (batch, 30, 16)
concat  → (batch, 30, 21)
LSTM(21 → 128 units, 2 layers, dropout 0.2)
take last timestep → (batch, 128)
Linear 128→128 · ReLU · Dropout
Linear 128→64  · ReLU · Dropout
Linear 64→5     → next [speed, rpm, throttle, brake, gear] (normalised)
```

- **LSTM** = a recurrent network with gates (forget, input, output) that keep or discard memory across the 30-step window. That suits time series.
- **Inputs are z-scored** per channel and de-normalised after prediction, so residuals come out in real units (km/h, rpm…).
- **Training:** sliding windows (30 in → 1 out), MSE loss, Adam at lr 1e-3, batch 32.
- **Residual:** `actual − predicted`.

**Reality check.**
- **Parameter count.** My own arithmetic for this exact architecture gives about **235K** parameters, not 150K (LSTM layer 1 ≈ 77K, layer 2 ≈ 132K, heads ≈ 25K, embedding 320). Check with `sum(p.numel() for p in model.parameters())` before quoting "150K" or "3.3×". The "~500K for v1" is also not backed by v1 code in the repo.
- The model is only **trained on toy data** (`__main__`). The service never calls `fit()` or `load()`, so it would predict from random weights.
- The service does not pass `car_id`, so every car gets embedding 0. The "per-car identity" benefit is not exercised end to end.
- `gear` is categorical but treated as a regression target.

---

## Slide 5 — Dual Drift Detection System

Slide: ADWIN (adaptive window) + Page-Hinkley (cumulative sum). Both must trigger for ≥5 consecutive windows.

**L1 — Story.** "Two independent smoke detectors. We only raise the alarm when both go off, and keep going off."

**L2 — Idea.**
- Question being answered: *has the pattern of the model's errors really changed, or is this just noise?*
- **ADWIN** compares recent errors to older errors. If the averages differ meaningfully, something shifted. It reacts well to sudden changes.
- **Page-Hinkley** adds up small deviations from the baseline. Many small nudges in the same direction eventually cross a line. It catches gradual shifts.
- **Confirmation logic:** require agreement from both *and* persistence (≥5 in a row). That trades a little delay for far fewer false alarms.
- **The safety rule (say this, it's the key idea):** when drift is confirmed, the system recommends retraining **unless recent errors are huge**. Huge errors suggest a real fault, and retraining on a fault would teach the model to accept it.

**L3 — Technical.** From `DriftGate` in [twin-model/drift_gate.py](twin-model/drift_gate.py). Parameters: `adwin_threshold=0.05`, `page_hinkley_threshold=5.0`, `consecutive_drifts=5`, `min_samples=100`.

Per channel, per update:

```
r = |residual|
after 100 samples:
  baseline = mean(first 100)      recent = mean(last 50)
  adwin_flag = |recent − baseline| / baseline > 0.05
  PH += (r − baseline) / std(first 100);   ph_flag = |PH| > 5
  both flags → confirm += 1  else  confirm = max(0, confirm − 1)
  confirm ≥ 5 → DriftEvent(severity 0..1), reset
adaptation allowed only if no channel's last 10 |residuals| > 2.0
```

- Formal ADWIN uses a variable-length window and a Hoeffding bound. Formal Page-Hinkley uses a tolerance δ and `m_t − min(m_t)`.

**Reality check.**
- These are **simplified "ADWIN-like" / "Page-Hinkley-like" versions** with a fixed baseline and a fixed 5% threshold, not the `river` library implementations (River is listed as a dependency but never imported). Say "ADWIN-inspired". For a faithful version, swap in `river.drift.ADWIN` and `river.drift.PageHinkley`.
- `drift_gate.py` uses `np` without importing numpy, so it raises `NameError` at the 100th sample.
- The `> 2.0` freeze check is in raw units. A 2 km/h residual is tiny, so adaptation would nearly always be blocked. It should use normalised residuals.
- Retraining itself is a TODO. The service only counts "adaptations".

---

## Slide 6 — Fleet Consensus Masking

Slide: pit stops, traffic, lapped cars, tyre changes are masked using a 20-car comparison.

**L1 — Story.** "If one student fails a test, check the student. If the whole class fails, check the test. And ignore moments when we already know why someone looks odd."

**L2 — Idea.** Two separate mechanisms share the slide:

1. **Masking (known-benign events).** Skip a car's surprise when it's explained: it's entering or leaving the pits, being lapped, stuck within 1 second of the car ahead, or just changed tyres.
2. **Consensus (fleet vs individual).** Compare each car to the fleet:
   - Most of the fleet is surprising the model at once → **benign regime change** (safety car, rain). Don't alarm, don't retrain on it.
   - Only this car is off → **individual anomaly**. That is the real alert.

The slide's four bullets are the masking rules. The "20-car consensus" is the second mechanism. Name both when you present.

**L3 — Technical.** `FleetConsensus` in [twin-model/drift_gate.py](twin-model/drift_gate.py):

```
masked if: pit_in/out present | is_lapped | gap_to_car_ahead < 1.0 s | tyre_compound changed
magnitude = mean(|residuals|);  stored as 0 if masked (keep last 100 per driver)
fleet_mean = mean of non-zero recent values
elevated   = # drivers with last-10 mean > 1.2 × fleet_mean
ratio      = elevated / (n_drivers − 1)
ratio > 0.7 → benign_regime_change; else individual_anomaly
```

The 70% threshold means "more than 70% of the other cars are also elevated".

**Reality check.**
- **Masking never actually fires today.** The service calls `fleet.update(driver, residuals)` without lap or gap info, so the masking inputs are empty.
- `residual_shifts` is keyed 0–19, but real car numbers go up to 99 (44, 63, 81…), which causes a `KeyError`. Fix: map driver numbers to 0–19.
- Residuals of different units (rpm vs km/h) are averaged without normalising, so RPM dominates.
- "Pit stop: sharp speed drop + pit lane detection" and "Tyre changes: temperature spikes" on the slide are more detailed than the code, which keys on lap-table fields (pit in/out time, compound change), not on speed drops or temperature.

---

## Slide 7 — Technology Stack & Deployment

**L1 — Story.** "Everything runs on a laptop with one command, because each piece lives in its own container."

**L2 — Idea.** One technology per job:

| Job | Tool | Why |
|---|---|---|
| Data source | FastF1 | Free, real F1 data |
| Streaming | Kafka 7.5.0 | Durable, ordered, replayable message log |
| Model | PyTorch LSTM | Standard for sequence prediction |
| Storage | InfluxDB 1.8 | Built for time-stamped data |
| Dashboard | Grafana 9.5.3 | Live charts on top of InfluxDB |
| Packaging | Docker Compose | "Works on my machine" for everyone |

**L3 — Technical.**
- [docker-compose.yml](docker-compose.yml) starts: ZooKeeper, Kafka (Confluent cp-kafka), Kafka UI (port 8080), InfluxDB (8086), Grafana (3000), the kafka-bridge, and the replayer image.
- Start with [start.sh](start.sh) / [run.sh](run.sh) (`docker-compose up -d`, then it polls Kafka until ready). Grafana login admin/admin.
- Why Kafka: partitioned by key, each consumer group tracks its own offset, and retention allows replay.
- Why InfluxDB: tags (driver) index the series, and range queries over time are cheap.
- Dashboards are provisioned from [dashboards/](dashboards/) JSON, so the panels appear automatically.

**Reality check.**
- "All services containerized" is true for infrastructure and the bridge. The **twin service and Spark job are not in compose**.
- Docs disagree on the Grafana version (compose pins 9.5.3; one doc says 10.2.0). Use 9.5.3.
- The provisioned datasource uses Flux mode while InfluxDB 1.8 speaks InfluxQL, so panels can show "No data" until that is fixed.

---

## Slide 8 — Results: Real F1 Telemetry Data

Slide: 14,400 points (20 cars × 2 h), 6 panels, <100 ms latency, 150K parameters (3.3× reduction). Dashboard ranges: speed 185–210, RPM 11.8–12K, throttle 0.47–0.52.

**L1 — Story.** "This is the proof it runs: all 20 cars on live charts, updating in real time."

**L2 — Idea.** Present it as a **working demonstration of the pipeline's plumbing**: data flows through Kafka into InfluxDB and appears in Grafana. Keep claims about *anomaly detection accuracy* out of this slide unless you have measured them.

**L3 — Technical.**
- 14,400 = 20 cars × 720 points. That is a sensible size for a demo dataset.
- Latency would be measured as time from Kafka publish to a point being queryable in InfluxDB (p50/p95). The experiment design lists latency, throughput, memory, precision/recall/F1/F0.5, false alarms per hour and detection delay as its metrics. F0.5 is used because false alarms cost more than a missed one on a pit wall.

**Reality check (most important slide to be careful on).**
- The values shown (speed 185–210, RPM 11.8–12K, throttle 0.47–0.52) are narrow bands typical of the **synthetic generator** [mock_data_generator.py](mock_data_generator.py) (sine waves plus noise for 20 drivers). Real F1 speed spans roughly 70–330 km/h. The dashboard was populated by this script, which writes straight to InfluxDB and bypasses Kafka.
- The `<100 ms` latency and any precision/recall figure are not measured by the repo. [experiments/runner.py](experiments/runner.py) generates results from a formula (`_simulate_experiment`, marked TODO), so `results/results.json` contains constants, not measurements.
- Recommended wording: *"Demonstrates the end-to-end data path and dashboard; detection-accuracy evaluation is future work,"* and call the latency a design target.
- If asked "is that real F1 data?": the replayer does load real FastF1 sessions (e.g. Monaco 2023 in tests). The charts on this slide came from the mock generator. Be ready to say so.

---

## Slide 9 — Dashboard Visualization

Six panels: Speed, RPM, Residuals heatmap, Throttle, Brake, Anomaly events.

**L1 — Story.** "This is what the engineer sees. Red flags on the screen tell them which car to look at."

**L2 — Idea.** Read the panels in two groups:
- **Raw signals** (what the car is doing): Speed, RPM, Throttle, Brake.
- **Model outputs** (what the system thinks): the **residual heatmap** (surprise per car over time, 20 rows) and **anomaly events** (count of alerts).

Demo order: point at a normal-looking trace → point at the heatmap where one car lights up → show the alert panel.

**L3 — Technical.**
- Measurements in InfluxDB: `f1_telemetry` (tag `driver`; fields speed, rpm, throttle, brake, gear, drs), `f1_residuals` (speed and RPM residuals only), `f1_alerts` (tags `driver`, `type`; field `severity`).
- Grafana queries those measurements by time range. Dashboard JSON is in [dashboards/](dashboards/), loaded by `dashboard-provisioning.yml`.

**Reality check.**
- The bridge stores only **speed and RPM residuals**, not all five channels.
- Alert `severity` is read from the wrong field, so it is always 0.
- The "Residuals heatmap" depends on residual data that the (currently undeployed) twin service would produce. With mock data it shows generated residuals.
- The slide text gives Speed 175–300 km/h but slide 8 says 185–210. State that one is the panel's allowed range and the other is the data's range, or align them.

---

## Slide 10 — Key Achievements

**L1 — Story.** "What we built, in seven checkmarks."

**L2 — Idea.** Group the seven bullets into three themes so you don't read them one by one:
1. **Efficiency:** one shared model, 3.3× smaller, real-time inference.
2. **Smart alerting:** dual drift detection, benign-event masking.
3. **Engineering:** scales to 20 cars, Docker one-command deploy, live dashboard.

**L3 — Technical (what stands behind each claim).**

| Claim | What backs it |
|---|---|
| 3.3× parameter reduction | Architecture in `lstm_twin.py` (verify the exact count, see slide 4) |
| <100 ms latency | Not measured yet |
| Dual drift + confirmation | Implemented in `DriftGate` (simplified detectors) |
| Benign-event masking | Implemented in `FleetConsensus` (not yet fed context) |
| 20 cars | Embedding table and fleet logic sized for 20 |
| Docker deployment | `docker-compose.yml` for infrastructure |
| Live dashboard with real F1 data | Dashboard works; data in the demo is mock |

**Reality check.** Verified-by-code claims are the design and the components. "Real-time <100 ms", "real F1 data on the dashboard" and any accuracy claim need to be reworded or measured first. Suggested honest version: *"Designed and built the components and infrastructure; end-to-end integration and evaluation are the next step."*

---

## Slide 11 — Future Work & Conclusions

Slide: multi-session learning, explainability (SHAP), live race integration.

**L1 — Story.** "Next steps: learn from more races, explain *why* something was flagged, and run on a live race."

**L2 — Idea.**
- **Multi-session learning:** train across many races and tracks so "normal" is broader than one session.
- **Explainability (SHAP):** tell the engineer *which signal* drove an alert ("RPM is off for the given speed and gear"), which builds trust.
- **Live race integration:** swap the replayer for a real-time feed.

**L3 — Technical.** Fold the project's own known gaps into "future work". That shows you understand the system well:
1. Train the twin on real F1 laps (per car, with `car_id`, one fixed scaler), `save()` it and `load()` it in the service.
2. Add the missing **anomaly decision step**: normalised residuals → EWMA smoothing → dynamic threshold (Telemanom style).
3. Replace the approximations with `river.drift.ADWIN` / `PageHinkley`.
4. Wire in the **fault injector** ([twin-model/fault_injection.py](twin-model/fault_injection.py): spike, dropout, gain drift; stuck-sensor is a stub) so detections can be scored against ground truth.
5. Replace the simulated experiment harness with real runs of B0/B1/E1/E2 across dry, wet, safety-car and injected-fault scenarios.
6. Fix the bridge timestamp/null handling so real replayer data reaches the dashboard.

**Closing line:** *"PitWall Twin v2 sets up an efficient, drift-aware architecture for F1 anomaly detection. The next step is to train it on real data and measure it."*

---

## Likely questions and safe answers

| Question | Safe answer |
|---|---|
| Why an LSTM and not a simpler model? | Next-step prediction on sequences with cross-channel dependencies; it is the standard in Telemanom-style detection. A simpler baseline is a good comparison to add. |
| How do you know a flagged moment is a real fault? | Today we only compute surprise (residuals). A formal anomaly threshold and fault-injection scoring are the next steps. |
| Why both ADWIN and Page-Hinkley? | They catch different shapes of change (sudden vs gradual). Requiring both for 5 windows cuts false alarms at the price of some delay. |
| What stops the model from learning a fault? | The adaptation freeze: no retraining while recent residuals are large. (Threshold needs normalising, see slide 5.) |
| Is the data real? | The replayer reads real FastF1 sessions. The dashboard demo was populated by a synthetic generator. |
| What are the accuracy numbers? | Not measured yet. The experiment harness currently simulates them, so do not quote them. |
| Why partition Kafka by driver? | Same key → same partition → each car's readings stay in order, and consumers can parallelise. |
| What does "shared model" cost you? | Per-car quirks rely on the 16-dim embedding being trained with real car IDs. |

## Suggested timing (about 10 minutes)

| Slides | Time |
|---|---|
| 1–2 (hook + problem) | 1.5 min |
| 3 (architecture) | 1.5 min |
| 4 (shared LSTM) | 1.5 min |
| 5–6 (drift + consensus; the key ideas) | 2.5 min |
| 7 (stack) | 0.5 min |
| 8–9 (results + dashboard demo) | 1.5 min |
| 10–11 (achievements + future) | 1 min |
