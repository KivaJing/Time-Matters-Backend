# Reproducibility Guide

This document describes the software configuration, fixed repository version, archived experimental data, and experiment entry points associated with:

**Simulation-Based Evaluation of an Event-Driven Hybrid Reasoning Backend for Just-in-Time Adaptive Interventions**

## 1. Fixed Repository Version

The submitted manuscript is associated with a fixed public repository release rather than the moving `main` branch.

```text
Repository:
https://github.com/KivaJing/Time-Matters-Backend

Release/tag:
ieee-access-submission-v1.1

Repository snapshot date:
2026-10-23

Original experiment commit:
fdfd306828b4bed0e76266ab7b43cca3167746b3
```

The original experiment commit identifies the source-code state recorded in the formal experiment archives. The public reproducibility release additionally contains documentation, dependency pinning, runtime-output exclusions, the exact Ollama image digest observed on the experimental host, and the finalized reproduction scripts.

The exact commit referenced by the annotated release tag can be verified after cloning with:

```bash
git rev-list -n 1 ieee-access-submission-v1.1
```

## 2. Experimental Environment

The system is deployed using Docker Compose.

Core infrastructure:

- Apache Kafka: Confluent Platform 7.5.0
- ZooKeeper: Confluent Platform 7.5.0
- ksqlDB: 7.5.9
- Ollama: 0.18.1

The evaluated Ollama image was originally run from the `latest` tag. For the reproducibility release, the exact digest observed on the experimental host is pinned to:

```text
ollama/ollama@sha256:5949ec041e62b28f92d5ff1d7c34080ec67efd73ad24a523e2ff558a24f33072
```

The corresponding image ID observed on the experimental host was:

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

The release requirements reproduce the Python environment used for the evaluation:

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

The archived controlled baselines confirm the following routing settings:

```text
rule-only:                 FORCE_ROUTE=rule_based
fast LLM-only:             FORCE_ROUTE=fast_llm
high-capacity LLM-only:    FORCE_ROUTE=llm
hybrid without load control:
                            DISABLE_LOAD_CONTROL=true
```

The semantic-state-suppression ablation uses:

```text
EMIT_ALL_MATCHED_WINDOWS=true
```

The primary hybrid and scheduler-buffer runs use:

```text
DISABLE_LOAD_CONTROL=false
FORCE_ROUTE=""
EMIT_ALL_MATCHED_WINDOWS=false
MAX_LLM_REQUESTS_PER_SEC=3
MAX_FAST_LLM_REQUESTS_PER_SEC=4
MAX_PENDING=256
MAX_PENDING_PER_USER=5
```

## 6. Starting the System

Start infrastructure:

```bash
docker compose up -d zookeeper kafka ksqldb ollama_full ollama_fast
```

Pull model weights:

```bash
docker exec "$(docker compose ps -q ollama_full)" ollama pull llama3.2:3b
docker exec "$(docker compose ps -q ollama_fast)" ollama pull granite3-moe:1b
```

Deploy ksqlDB queries:

```bash
docker compose up ksql_deployer
```

Start application services:

```bash
docker compose up -d \
  rule_engine \
  reasoning_scheduler \
  llm_reasoner \
  fast_llm_reasoner \
  intervention_engine
```

## 7. RQ1: Load Scaling

The scaling runner requires two positional arguments:

```bash
bash experiments/run_scaling_one.sh <num_users> <run_number>
```

The formal scaling experiment used:

```text
MODE=mixed_risk
EMIT_INTERVAL_SEC=10
BASE_SEED=42

workload warm-up:       10 s
measurement interval:  200 s
simulator duration:    210 s
cooldown:               20 s
```

Evaluated user loads:

```text
10
50
100
150
200
```

Each load level was executed in five runtime repetitions:

```bash
for users in 10 50 100 150 200; do
  for r in 1 2 3 4 5; do
    bash experiments/run_scaling_one.sh "$users" "$r"
  done
done
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

### Semantic-state-suppression ablation

The ablation disables semantic-state suppression by emitting all matched windows:

```bash
for r in 1 2 3 4 5; do
  RUN_PREFIX=baseline_nosemantic \
  EMIT_ALL_MATCHED_WINDOWS=true \
  bash experiments/run_scaling_one.sh 100 "$r"
done
```

The formal archived runs are:

```text
baseline_nosemantic_u100_r1
baseline_nosemantic_u100_r2
baseline_nosemantic_u100_r3
baseline_nosemantic_u100_r4
baseline_nosemantic_u100_r5
```

## 9. RQ3: Controlled Reasoning Baselines

The baseline runner requires:

```bash
bash experiments/run_baseline_smoke.sh <num_users> <run_number>
```

Formal controlled baseline runs used:

```text
users:                  100
workload warm-up:        10 s
measurement interval:   120 s
simulator duration:     130 s
cooldown:                 5 s
repetitions:               5
```

### Rule-only

```bash
for r in 1 2 3 4 5; do
  RUN_PREFIX=baseline_rule \
  FORCE_ROUTE=rule_based \
  bash experiments/run_baseline_smoke.sh 100 "$r"
done
```

### fast LLM-only

```bash
for r in 1 2 3 4 5; do
  RUN_PREFIX=baseline_fast \
  FORCE_ROUTE=fast_llm \
  bash experiments/run_baseline_smoke.sh 100 "$r"
done
```

### high-capacity LLM-only

```bash
for r in 1 2 3 4 5; do
  RUN_PREFIX=baseline_high \
  FORCE_ROUTE=llm \
  bash experiments/run_baseline_smoke.sh 100 "$r"
done
```

### Hybrid without load control

```bash
for r in 1 2 3 4 5; do
  RUN_PREFIX=baseline_noload \
  DISABLE_LOAD_CONTROL=true \
  bash experiments/run_baseline_smoke.sh 100 "$r"
done
```

### Full hybrid comparator

The full-hybrid 100-user comparator is the corresponding 100-user condition from the primary scaling experiment.

The evaluation compares latency, throughput, and operational behaviour. It does not evaluate clinical output quality.

## 10. Direct Ollama Token-Throughput Benchmark

Entry point:

```bash
python3 experiments/benchmark_ollama_tokens.py <output_directory>
```

The formal benchmark uses:

```text
fast LLM:
  model: granite3-moe:1b
  warm-up requests: 5
  sequential requests: 20
  parallel requests: 20
  configured parallelism: 3

high-capacity LLM:
  model: llama3.2:3b
  warm-up requests: 5
  sequential requests: 20
  parallel requests: 20
  configured parallelism: 2
```

The exact benchmark script preserved in the archived experiment is byte-identical to `experiments/benchmark_ollama_tokens.py` in the public reproducibility release.

SHA256 of the benchmark script:

```text
30e89b9df816240f6796b35a7ed2fbaa8abf4364782edf0d25e2577864a81669
```

This benchmark measures direct model-server decoding throughput separately from end-to-end application latency.

The high-capacity model runs reached the configured generation cap; therefore, the reported values should be interpreted as decoding throughput rather than natural-response latency.

## 11. Scheduler Pending-Buffer Telemetry

Queue telemetry was collected in a separate confirmatory experiment at:

```text
100 users
200 users
```

with three repetitions per load.

The archived configuration uses the normal hybrid scheduler configuration:

```text
DISABLE_LOAD_CONTROL=false
FORCE_ROUTE=""
EMIT_ALL_MATCHED_WINDOWS=false
MAX_FAST_LLM_REQUESTS_PER_SEC=4
MAX_LLM_REQUESTS_PER_SEC=3
MAX_PENDING=256
MAX_PENDING_PER_USER=5
```

Reproduction command:

```bash
for users in 100 200; do
  for r in 1 2 3; do
    RUN_PREFIX=queue \
    bash experiments/run_scaling_one.sh "$users" "$r"
  done
done
```

The formal run identifiers are:

```text
queue_u100_r1
queue_u100_r2
queue_u100_r3
queue_u200_r1
queue_u200_r2
queue_u200_r3
```

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

The case-level records used to construct the supplementary confusion matrices and bootstrap confidence intervals are preserved in the experimental-data archive under:

```text
correctness_v2_20261019_122516/case_results.csv
```

The functional-verification results assess agreement with simulator-defined expectations, not clinical correctness.

## 13. RQ5: Repeated-Replay Consistency

The consistency archive preserves both the frozen semantic input and the exact replay script used for the reported downstream consistency analysis.

The reported consistency analysis uses a frozen set of 185 semantic events replayed five times through the downstream reasoning pipeline.

The archived replay script accepts:

```text
--input
--run-id
--bootstrap
```

After extracting the consistency archive, replay one repetition with:

```bash
python3 semantic_replay/replay_semantic.py \
  --input semantic_replay/frozen_semantic.jsonl \
  --run-id reason_consistency_r1 \
  --bootstrap localhost:9092
```

Repeat with run identifiers:

```text
reason_consistency_r1
reason_consistency_r2
reason_consistency_r3
reason_consistency_r4
reason_consistency_r5
```

The replay script:

- orders the frozen semantic events by the archived `engine_ts_ms`;
- preserves the original inter-event timing;
- rewrites the run identifier;
- updates temporal provenance immediately before publication; and
- republishes the events to the `semantic_events` topic.

The analysis distinguishes:

- reasoning-output presence consistency; and
- exact agreement of fields when an output is present across all runs.

Admission differences near scheduler rate-limit boundaries are retained as part of the measured system behaviour rather than removed from the analysis.

The consistency archive also contains the earlier raw-state replay material. The reported downstream repeated-replay analysis described above uses the frozen semantic dataset rather than the 2,000-event raw-state replay.

## 14. Provenance and Trace Reconstruction

Entry point:

```bash
python3 experiments/trace_reconstruction_benchmark.py
```

The analysis reconstructs the admitted path from semantic event to reasoning request, reasoning output, and intervention event using run, user, window, event, and processing-time metadata.

The benchmarked trace lookup is an offline indexed reconstruction operation and should not be interpreted as runtime request latency.

## 15. Experimental Data Archive

Large raw runtime outputs are maintained outside the Git repository.

Experimental-data archive:

```text
https://drive.google.com/drive/folders/1r9vvi5RApJTozvN7FXBSHUX0BZkzXins?usp=sharing
```

The archive contains the formal evaluation outputs for:

```text
scaling_20261008_144334
baselines_20261019_103042
nosemantic_20261019_112053
queue_20261019_121743
token_benchmark_20261019_113054
correctness_v2_20261019_122516
consistency_20261008_160051
trace_reconstruction_v3_20261019_123938
```

The archived material includes, where applicable:

```text
meta/git_commit.txt
source snapshots
configuration snapshots
run logs
Kafka topic dumps
JSONL event streams
result summaries
resource telemetry
SHA256SUMS.txt
```

All formal experiment archives record the original experiment commit:

```text
fdfd306828b4bed0e76266ab7b43cca3167746b3
```

The data archive is separate from the source-code repository so that large raw and intermediate runtime outputs do not need to be stored in Git.

## 16. Reproducibility Scope

Reproduction can be considered at two levels.

### Software reproduction

The fixed GitHub release contains the implementation, container configuration, experiment scripts, and model-serving configuration needed to recreate the evaluated backend.

### Numerical-result verification

The separate experimental archive preserves the raw and intermediate outputs used to derive the manuscript tables and supplementary analyses.

The reported evaluation uses synthetic behavioural data only. No human participants or personal health records are included.
