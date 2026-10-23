# Time Matters Backend

Backend implementation and reproducibility code for the manuscript:

**Simulation-Based Evaluation of an Event-Driven Hybrid Reasoning Backend for Just-in-Time Adaptive Interventions**

Authors: Jing Zhan and Rahim Rahmani
Department of Computer and Systems Sciences, Stockholm University

Repository: https://github.com/KivaJing/Time-Matters-Backend

## Overview

This repository contains the prototype backend evaluated in the accompanying IEEE Access manuscript. The system implements an event-driven reasoning pipeline for processing simulated behavioural state observations, constructing higher-level semantic events, controlling downstream reasoning demand, routing selected events to heterogeneous reasoning paths, and producing intervention events.

The evaluation is a simulation-based technical systems study. The repository does not contain clinical data, and the reported results should not be interpreted as clinical validation of an intervention system.

## Architecture

```text
Behavioural Simulator
        |
        v
      Kafka
        |
        v
     ksqlDB
  hopping-window CEP
        |
        v
   Rule Engine
 semantic-event construction
        |
        v
Reasoning Scheduler
  |       |        |
  |       |        |
 rule   fast LLM   high-capacity LLM
  |       |        |
  +-------+--------+
          |
          v
   reasoning_events
          |
          v
 Intervention Engine
```

Main components:

- `simulator/` — deterministic synthetic behavioural-state generator.
- `ksqldb/` — stream-processing deployment and CEP configuration.
- `rule_engine/` — semantic rule evaluation and state handling.
- `llm_reasoner/reasoning_scheduler.py` — load-aware reasoning scheduler.
- `llm_reasoner/fast_llm_reasoner.py` — fast LLM reasoning path.
- `llm_reasoner/llm_reasoner.py` — high-capacity LLM reasoning path.
- `intervention/` — intervention-event processing and feedback-state support.
- `observer/` — measurement, replay, and evaluation utilities.
- `experiments/` — experiment runners and analysis scripts used for the reported evaluation.

## Evaluated Software Environment

The reproducibility release reflects the evaluated software environment:

- Python 3.12.3
- Apache Kafka: Confluent Platform 7.5.0
- ZooKeeper: Confluent Platform 7.5.0
- ksqlDB: 7.5.9
- Ollama: 0.18.1

The Ollama image observed on the experimental host was subsequently pinned by repository digest in `docker-compose.yml`:

```text
ollama/ollama@sha256:5949ec041e62b28f92d5ff1d7c34080ec67efd73ad24a523e2ff558a24f33072
```

## Python Dependencies

Create and activate a virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

The fixed Python dependencies are:

```text
kafka-python==2.3.0
confluent-kafka==2.13.0
requests==2.32.5
websockets==16.0
```

## Model Configuration

### fast LLM

- Model: `granite3-moe:1b`
- API: `/api/generate`
- Maximum generated tokens: 32
- Context size: 256
- Temperature: 0.0
- Worker count: 3
- Maximum in-flight requests: 6
- Maximum request age: 4000 ms

### high-capacity LLM

- Model: `llama3.2:3b`
- API: `/api/chat`
- Maximum generated tokens: 100
- Temperature: 0.0
- Worker count: 2
- Maximum in-flight requests: 4
- Maximum request age: 5000 ms

## Scheduler Configuration

The evaluated hybrid scheduler uses semantic-event coalescing, bounded pending capacity, per-user pending limits, user cooldown, priority filtering, explicit drops, and separate rate limits for the two LLM paths.

Primary configuration:

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

## Setup

Clone the repository:

```bash
git clone https://github.com/KivaJing/Time-Matters-Backend.git
cd Time-Matters-Backend
```

For exact manuscript reproduction, check out the fixed submission release once it has been created:

```bash
git checkout ieee-access-submission-v1
```

Start the infrastructure:

```bash
docker-compose up -d zookeeper kafka ksqldb ollama_full ollama_fast
```

Pull the evaluated models:

```bash
docker exec "$(docker-compose ps -q ollama_full)" ollama pull llama3.2:3b
docker exec "$(docker-compose ps -q ollama_fast)" ollama pull granite3-moe:1b
```

Deploy the ksqlDB processing logic:

```bash
docker-compose up ksql_deployer
```

Start the application services:

```bash
docker-compose up -d \
  rule_engine \
  reasoning_scheduler \
  llm_reasoner \
  fast_llm_reasoner \
  intervention_engine
```

Check service status:

```bash
docker-compose ps
```

## Simulator

Example mixed-risk workload:

```bash
cd simulator

RUN_ID=example_run \
NUM_USERS=5 \
MODE=mixed_risk \
EMIT_INTERVAL_SEC=10 \
DURATION_SEC=200 \
python3 event_generator.py
```

Available simulator modes include:

```text
baseline_only
stress_rising
craving_rising
low_motivation
relapse_risk
mixed_risk
```

The primary scaling evaluation uses `mixed_risk`.

## Paper Experiment Entry Points

Detailed parameters and experiment-to-research-question mappings are documented in [`docs/reproducibility.md`](docs/reproducibility.md).

### Scaling evaluation

```bash
bash experiments/run_scaling_one.sh
```

The reported evaluation uses 10, 50, 100, 150, and 200 simulated users with five runtime repetitions per load.

### Controlled reasoning baselines and selected ablations

```bash
bash experiments/run_baseline_smoke.sh
```

Reported configurations include rule-only, fast LLM only, high-capacity LLM only, the full hybrid system, hybrid without load control, and hybrid without semantic-state suppression.

### Semantic functional correctness

```bash
python3 experiments/correctness_v2.py
```

Expected outputs include:

```text
case_results.csv
ambiguous_results.csv
confusion_*.csv
summary.json
```

### Direct Ollama token-throughput benchmark

```bash
python3 experiments/benchmark_ollama_tokens.py
```

### Trace reconstruction

```bash
python3 experiments/trace_reconstruction_benchmark.py
```

### Consistency dataset construction

```bash
python3 experiments/build_consistency_frozen_raw.py
```

## Experimental Data

Large runtime archives and raw experimental outputs are intentionally excluded from the Git repository.

This includes per-run Kafka topic dumps, JSONL event streams, service logs, resource-monitoring output, complete scaling-run directories, consistency replay archives, scheduler pending-buffer telemetry archives, and trace-reconstruction archives.

## Repository Structure

```text
.
├── README.md
├── LICENSE
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── simulator/
├── ksqldb/
├── rule_engine/
├── llm_reasoner/
├── intervention/
├── observer/
├── experiments/
└── docs/
```

Runtime outputs, local experiment archives, virtual environments, and large result datasets are excluded through `.gitignore`.

## Scope and Limitations

The prototype is evaluated with synthetic behavioural data under controlled simulation scenarios.

The reported semantic functional correctness results measure agreement with predefined simulator expectations rather than clinical or real-world behavioural validity.

The LLM experiments evaluate system latency, throughput, routing, and operational behaviour. They do not establish clinical quality, therapeutic effectiveness, or intervention safety.

## License

See `LICENSE`.

## Citation

Citation information will be added after publication.

For pre-publication reproducibility, cite the fixed GitHub release together with the accompanying manuscript.
