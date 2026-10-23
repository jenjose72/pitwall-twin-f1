# PitWall Twin — Explained Simply

This guide is for someone who knows **nothing** about F1, Kafka or AI.
Every hard word is explained in plain language, with everyday examples.

> **Important honesty note.** The project's code is a *working skeleton*. But the **result
> numbers** (like "93% precision") are **placeholders made by a formula, not real
> measurements**. See [Part 9](#9-what-is-real-and-what-is-not) before you talk about results.

---

## Contents

1. [The project in 30 seconds](#1-the-project-in-30-seconds)
2. [Basic words you need](#2-basic-words-you-need)
3. [The big idea: guess, then compare](#3-the-big-idea-guess-then-compare)
4. [The problem: conditions keep changing](#4-the-problem-conditions-keep-changing)
5. [The two fixes](#5-the-two-fixes)
6. [How the pieces connect](#6-how-the-pieces-connect)
7. [What each file does](#7-what-each-file-does)
8. [The four experiments: B0, B1, E1, E2](#8-the-four-experiments-b0-b1-e1-e2)
9. [What is real and what is not](#9-what-is-real-and-what-is-not)
10. [How to run it](#10-how-to-run-it)
11. [Viva questions with easy answers](#11-viva-questions-with-easy-answers)
12. [Dictionary](#12-dictionary)

---

## 1. The project in 30 seconds

An F1 car has many sensors that constantly report numbers: speed, engine revolutions (RPM),
throttle, brake and gear.

This project builds a system that **watches those numbers live and warns when something looks
wrong** (for example, a broken sensor).

**How?**
1. A computer model learns what "normal" looks like and **guesses the next number**.
2. If the real number is **very different** from the guess, something may be wrong.
3. Races change all the time (rain, safety car), which can fool the system. So the project adds
   two extra checks to avoid **false alarms**.

That is the whole project. The rest of this document explains each part slowly.

---

## 2. Basic words you need

**Formula 1 (F1):** a car race. 20 cars race for about 2 hours.

**Pit stop:** a car stops in the pit lane for new tyres. It drives differently then, but
that is normal.

**Safety car:** after a crash, a slow official car leads the race, so **all cars slow
down**. Normal too.

**Telemetry:** numbers sent from the car's sensors, like a fitness watch sending your heart
rate to your phone. Each message looks like this:

```
driver: 44   speed: 287   rpm: 11800   throttle: 100   brake: 0   gear: 7
```

**Channel:** one kind of number (speed is one channel, rpm is another). This project uses 5:
speed, rpm, throttle, brake, gear.

**Anomaly:** something unusual. Example: RPM stays frozen at one value because a sensor broke.

**Where does the data come from?** From **FastF1**, a free tool that downloads data from
*past real races*. The project **replays** an old race (default: Monaco 2023) as if it were
live. The replayer is like pressing play on a recorded match.

---

## 3. The big idea: guess, then compare

### The "digital twin"
A **digital twin** is a computer copy of a real thing. Here it is a **small AI model** that has
learned how a car's sensors normally behave.

### How it works (weather-forecast style)
Imagine a weather forecaster who guesses tomorrow's temperature.
- Forecast: 25°C. Real: 26°C → close. All good.
- Forecast: 25°C. Real: 5°C → something strange happened!

The twin does the same for car sensors:
1. Look at the **last 30 readings**.
2. **Guess the next reading.**
3. Compare the guess with the real reading.

### The residual (the most important word)
```
residual = real value − guessed value
```
- **Small residual** → the car did what we expected → normal.
- **Big residual** → surprise → maybe a problem.

Example: guess speed 280, real speed 281 → residual 1 (fine).
Guess 280, real 0 → residual −280 (very suspicious).

### What AI model is it?
An **LSTM**. You can think of it as a model that **reads numbers in order and remembers what
came before**, like how you remember the start of a sentence while reading the end. That makes
it good at predicting "what comes next" in a sequence.

This idea comes from a research paper (**Hundman et al., 2018**) that used it on NASA
spacecraft data. The project copies that idea and applies it to F1.

### One model for all cars
Instead of 100 models (5 channels × 20 cars), the project uses **one shared model**. To let it
know which car it is looking at, each car gets a small learned "ID card" called an
**embedding**.

---

## 4. The problem: conditions keep changing

Suppose we train the model on a **dry** race. Then it **starts raining**.

- All cars now behave differently.
- The model's guesses become wrong for **every car**.
- Residuals get big everywhere → the system screams "problem!" 20 times.
- But **nothing is broken**. These are **false alarms**.

This slow or sudden change in how the data behaves is called **drift**.
Other causes: worn tyres, hot track, safety car, less fuel.

### Two bad solutions
- **Never update the model.** It becomes outdated and gives false alarms when conditions
  change.
- **Always keep updating the model.** Dangerous! If a sensor slowly breaks, the model *learns
  the broken values as normal* and stops warning you. Like a smoke alarm that gets used to
  smoke.

---

## 5. The two fixes

### Fix 1: The Drift Gate ("update only when it's safe")
Code: `twin-model/drift_gate.py`, class `DriftGate`.

It decides **when the model may be updated**:

1. **Two detectors watch the size of the residuals.**
   - **Detector A (ADWIN-style):** compares recent residuals with older ones. If they changed
     by more than 5%, it says "drift?"
   - **Detector B (Page-Hinkley):** keeps a running total of how far residuals have wandered.
     If the total gets big, it says "drift?"
2. **Both must agree, 5 times in a row.** This double-check avoids mistakes.
3. **Freeze rule:** even if drift is confirmed, **do not update if a big residual happened
   recently**. That could be a real fault, and we must not teach the model that faults are normal.

### Fix 2: Fleet Consensus ("is it just me or everyone?")
Code: same file, class `FleetConsensus`.

Think of a classroom: if **one** student says it's cold, maybe that student is ill. If
**everyone** says it's cold, the heating is off. So:

- **Most cars (more than 70%) affected** → shared cause (rain, safety car) → `benign_regime_change`
  = **not a fault**.
- **Only one car affected** → `individual_anomaly` = **investigate!**
- **Ignored on purpose ("masked"):** a car in the pits, a car being lapped, a car driving very
  close behind another, a car that just changed tyres. These are normal for that car.

**Summary:** the Drift Gate answers *"should the model learn from this?"* and Fleet Consensus
answers *"is this a real problem or just the race changing?"*

---

## 6. How the pieces connect

Think of a **factory conveyor belt**:

```
  FastF1 (old race data)
        │
        ▼
   REPLAYER  ── plays the race like a video, sends messages ──►  KAFKA
                                                                  │
                          ┌───────────────────────────────────────┤
                          ▼                                       ▼
                    SPARK JOB                              TWIN SERVICE
              (makes 10-second averages)          (guess → compare → drift checks)
                                                              │
                                                              ▼
                                                    KAFKA → INFLUXDB BRIDGE
                                                              │
                                                              ▼
                                                  INFLUXDB (storage) → GRAFANA (charts)
```

**What each tool is, simply:**

| Tool | Simple meaning | Job here |
|---|---|---|
| **Docker** | Runs programs in separate boxes | Starts everything with one command |
| **Kafka** | A post office / conveyor belt for messages | Carries data between parts |
| **Spark** | Tool for crunching a never-ending stream of data | Makes rolling averages |
| **PyTorch** | Library for building AI models | Builds the LSTM |
| **InfluxDB** | A database for numbers with time stamps | Stores results |
| **Grafana** | A tool that draws live charts | The dashboard |

**Kafka words:**
- **Topic** = a named mailbox, like `f1.telemetry`.
- **Producer** = writes into a topic. **Consumer** = reads from it.
- **Partition** = a topic split into lanes. All messages of one driver use the same lane, so
  **one car's messages stay in order**.

**Topics in this project:** `f1.telemetry` (sensor numbers), `f1.laps` (tyres, pit stops),
`f1.weather`, `f1.race_control` (flags), `f1.features` (averages), `f1.residuals` (guess vs
real), `f1.alerts` (warnings).

---

## 7. What each file does

| File | What it does (simple) |
|---|---|
| `docker-compose.yml` | List of programs Docker should start (Kafka, InfluxDB, Grafana…) |
| `replayer/main.py` | Downloads a race and replays it into Kafka at a chosen speed (1× = real time, 10× = ten times faster) |
| `replayer/config.py` | Settings: speed, topic names, folders |
| `spark-jobs/feature_engineering.py` | Joins telemetry with tyre and weather info, makes 10-second averages |
| `twin-model/lstm_twin.py` | The AI model: reads 30 readings, guesses the next one |
| `twin-model/drift_gate.py` | Drift Gate + Fleet Consensus (Part 5) |
| `twin-model/twin_service.py` | The live loop: read data → guess → compute residual → run checks → publish |
| `twin-model/fault_injection.py` | Creates **fake faults** (stuck sensor, spike, missing value, slow drift) with known times, so we can test if we catch them |
| `experiments/runner.py` | Runs the 4 experiments on 4 scenarios and writes a report |
| `kafka_to_influxdb_http.py` | Copies messages from Kafka into InfluxDB |
| `dashboards/` | Grafana chart definitions |
| `mock_data_generator*.py` | Writes **fake** data into InfluxDB to fill the dashboard |
| `tests/test_replayer.py` | Checks that FastF1 can download a race |

### Small details that help in a viva
- The model's input is the last **30** readings; it has **2 LSTM layers** with **128** units.
- Numbers are **normalised** (rescaled to a similar size) so big numbers like RPM (12000) don't
  drown small ones like throttle.
- Training uses the **Adam** optimiser and **MSE** (average squared error) as the score to
  reduce.
- The replayer sends each car to Kafka lane `driver_number % 20`.

---

## 8. The four experiments: B0, B1, E1, E2

**B = baseline** (simple versions to compare against). **E = enhancement** (the project's
ideas).

| Name | Plain meaning |
|---|---|
| **B0** | Train once, never update. |
| **B1** | Update constantly, no safety checks. |
| **E1** | Update only when drift is confirmed and no fault is active (Drift Gate). |
| **E2** | E1 + Fleet Consensus (ignores race-wide changes). **The full idea.** |

**Four test situations (scenarios):**
1. **Dry race** – normal.
2. **Wet race** – rain affects everyone.
3. **Safety cars** – everyone slows.
4. **Injected faults** – fake faults with known answers.

**Scores explained with a smoke alarm:**
- **Precision:** when the alarm rings, how often is there really a fire? (Low = lots of false
  alarms.)
- **Recall:** of all real fires, how many did the alarm catch? (Low = missed fires.)
- **F1:** one score mixing both.
- **Detection delay:** how long after the fire starts does the alarm ring?

**The hope (hypothesis):** B0 is worst and E2 is best, especially in rain and safety cars.
This is a *hope*, not a proven result yet.

---

## 9. What is real and what is not

Be honest in your viva. This is exactly where the project stands:

**✅ Real (code exists):**
- Docker setup for Kafka, InfluxDB, Grafana, Kafka UI
- Replayer (FastF1 → Kafka)
- The LSTM model design
- Drift Gate and Fleet Consensus logic
- Fault injector

**⚠️ Partly done or fragile:**
- Spark job is written but **switched off** in Docker (commented out).
- Grafana dashboards exist, but panels may show "No data".
- Dashboard data came from **fake generated data**, and the bridge probably can't read the
  replayer's timestamp format.
- Replayer sends cars one after another, not all 20 at once.

**❌ Not real / broken:**
- **The result numbers are fake.** `experiments/runner.py` uses a formula (start at 85%
  precision, add fixed bonuses for each config). It never runs the model.
- **The NASA baseline reproduction does not exist.** No code loads that data, even though some
  older docs quote numbers for it.
- **The model is never trained in the live service.** It would guess with random weights.
- **"Adaptation" only prints a message.** No retraining happens.
- **The docs say River's ADWIN is used. It isn't.** The code uses simple hand-written checks.
- **`drift_gate.py` is missing `import numpy as np`**, so it would crash when the drift check
  runs.

**What to say if asked:**
> "I built the streaming pipeline, the model, and the drift and consensus components. The
> evaluation part is a scaffold with placeholder numbers, and running real experiments is the
> next step."

---

## 10. How to run it

You need Docker and Docker Compose, about 4 GB of free memory, and Python 3.10+.

```bash
# 1. Start Kafka, InfluxDB, Grafana, etc.
docker-compose up -d
docker-compose ps            # wait until everything says "Up" (30-60 seconds)

# 2. Replay a race into Kafka (default: Monaco 2023)
docker-compose run replayer python -m replayer.main
#    For faster replay, set REPLAY_SPEED=10

# 3. Open the web pages
#    Kafka UI : http://localhost:8080
#    Grafana  : http://localhost:3000  (login: admin / admin)

# 4. Run the twin service (must be run from inside the twin-model folder)
cd twin-model
python twin_service.py E2       # modes: B0, B1, E1, E2

# 5. Run the experiment harness (numbers are simulated!)
cd ..
python -m experiments.runner
```

Stop everything: `docker-compose down`

---

## 11. Viva questions with easy answers

**What does the project do?**
It watches live F1 sensor data and warns about abnormal behaviour, without false alarms when
race conditions change.

**What is a digital twin?**
A computer copy of a real system. Here: an AI model that guesses a car's next sensor readings.

**How do you find an anomaly?**
Residual = real − guess. A big residual means a surprise.

**Why an LSTM?**
It reads sequences and remembers recent history. It also matches the paper we copy
(Hundman et al.), so the comparison is fair.

**Why one shared model?**
One model instead of 100 is cheaper. A small learned "ID card" per car (embedding) lets it
handle each car's differences.

**What is drift?**
The data's behaviour changes over time (rain, tyre wear). It makes a fixed model wrong and
causes false alarms.

**Why not retrain all the time?**
The model would learn real faults as normal and stop catching them.

**Why two drift detectors?**
They catch different kinds of change. Requiring both to agree, 5 times in a row, reduces
mistakes.

**What is the freeze rule?**
No model updates while a big residual happened recently, so we never learn a fault as normal.

**What is fleet consensus?**
If most cars shift together, it's a race-wide cause (safety car, rain), not a fault. If only
one car shifts, it's suspicious.

**Why Kafka? Why partition by driver?**
Kafka passes messages reliably between parts and can scale. Partitioning by driver keeps each
car's data in order.

**What does Spark do?**
Makes rolling 10-second averages and joins sensor data with tyre and weather info.

**What are B0, B1, E1, E2?**
Never update; always update; update only when safe (drift gate); update when safe + fleet check.

**What were your results?**
Be honest: the harness exists but its numbers are placeholders. Real experiments are future
work.

**What are the limitations?**
Model not trained in the live service, no real retraining, simplified drift detectors,
placeholder evaluation, and slow faults can look like drift.

**How would you improve it?**
Train on real FastF1 data, implement real retraining, use River's ADWIN, test with injected
faults and measure real precision and recall.

---

## 12. Dictionary

- **Adam:** a method that adjusts an AI model's settings while it learns.
- **ADWIN:** a drift detector that compares recent data with older data.
- **Anomaly:** unusual behaviour.
- **Channel:** one type of sensor number.
- **Consumer / Producer:** reads / writes Kafka messages.
- **Drift:** data behaviour changes over time.
- **DRS:** a movable wing flap that helps a car overtake.
- **Embedding:** a small learned "ID card" for a category (here, a car).
- **False alarm:** a warning when nothing is wrong.
- **Feature engineering:** turning raw numbers into useful summaries, like averages.
- **LSTM:** an AI model for sequences that remembers recent history.
- **MSE:** average squared error, the score the model tries to lower.
- **Normalise:** rescale numbers so they are similar in size.
- **Page-Hinkley:** a detector that adds up deviations and fires when the total is big.
- **Partition:** one lane of a Kafka topic.
- **Precision / Recall:** how many alerts were right / how many real problems were caught.
- **Residual:** real value − guessed value.
- **Telemetry:** sensor data sent from a machine.
- **Topic:** a named Kafka mailbox.
- **Window:** a short slice of recent data.
