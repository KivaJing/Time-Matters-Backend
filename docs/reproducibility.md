# Reproducibility Guide

This document describes the software configuration and experiment entry points associated with:

**Simulation-Based Evaluation of an Event-Driven Hybrid Reasoning Backend for Just-in-Time Adaptive Interventions**

## 1. Fixed Repository Version

The submitted manuscript is associated with a fixed repository state.

```text
Repository:
https://github.com/KivaJing/Time-Matters-Backend

Release/tag: ieee-access-submission-v1
Snapshot date: 2026-10-23
```

The exact release tag and commit hash should be used instead of the moving `main` branch.

## 2. Experimental Environment

The system is deployed using Docker Compose.

Core infrastructure:

- Apache Kafka: Confluent Platform 7.5.0
- ZooKeeper: Confluent Platform 7.5.0
- ksqlDB: 7.5.9
- Ollama: 0.18.1

The evaluated Ollama container image was subsequently pinned for the reproducibility release to:

```text
ollama/ollama@sha256:5949ec041e62b28f92d5ff1d7c34080ec67efd73ad24a523e2ff558a24f33072
```

The image ID observed on the experimental host was:

```text
sha256:ba1264eecd706fa1c2a616c1d8313b48eae59fed6749384625e3cddc13240ca2
```

## 3. Python Environment

Create and activate a virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

The release requirements file should contain the exact versions observed in the experimental environment for:

```text
Python: 3.12.3

Python dependencies:
kafka-python 2.3.0
confluent-kafka 2.13.0
requests 2.32.5
websockets 16.0
```

## 4. Model Configuration

### fast LLM

```text
Model: granite3-moe:1b
API: /api/generate
MAX_WORKERS=3
MAX_INFLIGHT=6
MAX_REQUEST_AGE_MS=4000
OLLAMA_NUM_PREDICT=32
OLLAMA_NUM_CTX=256
OLLAMA_TEMPERATURE=0.0
```

### high-capacity LLM

```text
Model: llama3.2:3b
API: /api/chat
MAX_WORKERS=2
MAX_INFLIGHT=4
MAX_REQUEST_AGE_MS=5000
OLLAMA_NUM_PREDICT=100
OLLAMA_TEMPERATURE=0.0
```

## 5. Scheduler Configuration

The primary hybrid configuration uses:

```text
COALESCE_SEC=0.1
MAX_PENDING=256
MAX_PENDING_PER_USER=5

ENABLE_USER_COOLDOWN=true
USER_COOLDOWN_SEC=1

ENABLE_PRIORITY_FILTER=true
DROP_MEDIUM_WHEN_PENDING_GE=64
DROP_LOW_ALWAYS=true

MAX_LLM_REQUESTS_PER_SEC=3
MAX_FAST_LLM_REQUESTS_PER_SEC=4

ENABLE_DROPPED_TOPIC=true
SCHEDULER_VERSION=v4_scored
```

The selected no-load-control ablation disables the scheduler load-control mechanisms through the archived experiment configuration.

The semantic-state-suppression ablation enables emission of all matched windows through the corresponding archived configuration.

## 6. Starting the System

Start infrastructure:

```bash
docker-compose up -d zookeeper kafka ksqldb ollama_full ollama_fast
```

Pull model weights:

```bash
docker exec "$(docker-compose ps -q ollama_full)" ollama pull llama3.2:3b
docker exec "$(docker-compose ps -q ollama_fast)" ollama pull granite3-moe:1b
```

Deploy ksqlDB queries:

```bash
docker-compose up ksql_deployer
```

Start application services:

```bash
docker-compose up -d \
  rule_engine \
  reasoning_scheduler \
  llm_reasoner \
  fast_llm_reasoner \
  intervention_engine
```

## 7. RQ1: Load Scaling

Entry point:

```bash
bash experiments/run_scaling_one.sh
```

Primary workload:

```text
MODE=mixed_risk
BASE_SEED=42
```

Evaluated user loads:

```text
10
50
100
150
200
```

Each load level was executed in five independent runtime repetitions.

Each formal run used:

```text
workload warm-up:       10 s
measurement interval:  200 s
simulator duration:    210 s
```

Application services were restarted between runs. Kafka, ksqlDB, and Ollama remained running across repetitions.

The individual runtime repetition is the statistical unit.

## 8. RQ2: Semantic Reduction and Scheduler Suppression

RQ2 uses the same primary scaling runs.

Two distinct reductions are evaluated:

1. CEP output to semantic event construction.
2. Semantic events to admitted reasoning requests.

These quantities must not be conflated.

Scheduler suppression contains both:

- explicit drops; and
- coalesced replacements.

## 9. RQ3: Controlled Reasoning Baselines

Entry point:

```bash
bash experiments/run_baseline_smoke.sh
```

The reported controlled configurations at 100 simulated users include:

```text
rule-only
fast LLM only
high-capacity LLM only
full hybrid
hybrid without load control
hybrid without semantic-state suppression
```

The evaluation compares latency, throughput, and operational behaviour. It does not evaluate clinical output quality.

## 10. Direct Ollama Token-Throughput Benchmark

Entry point:

```bash
python3 experiments/benchmark_ollama_tokens.py
```

This benchmark measures direct model-server decoding throughput separately from end-to-end application latency.

The high-capacity model runs used in this benchmark reached the configured generation cap; therefore, the reported values should be interpreted as decoding throughput rather than natural-response latency.

## 11. Scheduler Pending-Buffer Telemetry

Queue telemetry was collected in a separate confirmatory experiment at:

```text
100 users
200 users
```

with three repetitions per load.

The measured quantity is scheduler pending-buffer occupancy, not Kafka backlog.

## 12. RQ4: Semantic Functional Correctness

Entry point:

```bash
python3 experiments/correctness_v2.py
```

Correctness-v2 operates on finalized logical windows and is intentionally independent from intermediate ksqlDB hopping-window changelog emissions.

The experiment uses:

```text
seeds:                  101, 202, 303
cases per class/seed:   50
classes:                4
cases per condition:    600
bootstrap repetitions:  2000
bootstrap seed:          20261019
```

Evaluated conditions:

```text
clean
noise_sd_0.35
missing_one_of_five_windows
delayed_adjacent_window
threshold_0.90
threshold_1.00
threshold_1.10
```

Expected output files:

```text
case_results.csv
ambiguous_results.csv
confusion_clean.csv
confusion_noise_sd_0.35.csv
confusion_missing_one_of_five_windows.csv
confusion_delayed_adjacent_window.csv
confusion_threshold_0.90.csv
confusion_threshold_1.00.csv
confusion_threshold_1.10.csv
summary.json
```

The archived case-level result file used for the supplementary confusion matrices is:

```text
correctness_v2_20261019_122516/case_results.csv
```

The functional-verification results assess agreement with simulator-defined expectations, not clinical correctness.

## 13. RQ5: Repeated-Replay Consistency

Frozen consistency input construction:

```bash
python3 experiments/build_consistency_frozen_raw.py
```

The frozen semantic event dataset is replayed repeatedly through the reasoning pipeline.

The analysis distinguishes:

- reasoning-output presence consistency; and
- exact agreement of fields when an output is present across all runs.

Admission differences near scheduler rate-limit boundaries are retained as part of the measured system behaviour rather than removed from the analysis.

## 14. Provenance and Trace Reconstruction

Entry point:

```bash
python3 experiments/trace_reconstruction_benchmark.py
```

The analysis reconstructs the admitted path from semantic event to reasoning request, reasoning output, and intervention event using run, user, window, event, and processing-time metadata.

The benchmarked trace lookup is an offline indexed reconstruction operation and should not be interpreted as runtime request latency.

## 15. Experimental Data Archive

Large raw runtime outputs are maintained outside the Git repository.

The archive includes:

```text
scaling runs
controlled baselines
ablation runs
scheduler-buffer runs
token-throughput benchmark
correctness-v2
repeated-replay consistency
trace reconstruction
Kafka topic dumps
JSONL event streams
service logs
resource telemetry
```

Where applicable, individual experiment archives contain:

```text
meta/git_commit.txt
source snapshots
configuration snapshots
run logs
result summaries
SHA256SUMS.txt
```

## 16. Reproducibility Scope

Reproduction can be considered at two levels.

### Software reproduction

The fixed GitHub release contains the implementation, container configuration, experiment scripts, and model-serving configuration needed to recreate the evaluated backend.

### Numerical-result verification

The separate experimental archive preserves the raw and intermediate outputs used to derive the manuscript tables and supplementary analyses.

The reported evaluation uses synthetic behavioural data only. No human participants or personal health records are included.