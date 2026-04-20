# Thesis-Project-Time-Matters-Backend

This repository contains the backend implementation and reproducible scripts for the thesis experiments on event-driven reasoning and intervention systems.

The code in this repository corresponds to the experimental setup used in the thesis and is intended to support reproducibility.

---

## 1. Overview

The system is built around a Kafka-based event processing pipeline with multiple components:

* Kafka & Zookeeper (event streaming)
* ksqlDB (CEP rules)
* Rule Engine
* LLM-based Reasoner
* Intervention Engine
* Simulator and Observers for experiments

---

## 2. Requirements

* Docker & Docker Compose
* Python 3.x
* Virtual environment (venv recommended)

---

## 3. Setup and Initialization

### 3.1 Clone the Repository

```bash
git clone https://github.com/KivaJing/Thesis-Project-Time-Matters-Backend.git
cd Thesis-Project-Time-Matters-Backend
```

### 3.2 Stop Existing Containers

```bash
docker-compose down
```

### 3.3 Start Core Infrastructure

```bash
docker-compose up -d zookeeper kafka ksqldb ollama_full ollama_fast
docker-compose up ksql_deployer
```

### 3.4 Start Long-Running Services

```bash
docker-compose up -d rule_engine reasoning_scheduler llm_reasoner fast_llm_reasoner intervention_engine
```

### 3.5 Check Running Containers

```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

---

## 4. Access ksqlDB

```bash
docker exec -it kafka-cep-prototype_ksqldb_1 ksql http://localhost:8088
```

---

## 5. Python Environment

```bash
cd ~/kafka-cep-prototype
source venv/bin/activate
```

---

## 6. Deploy CEP Rules

```bash
cd ksqldb
KSQLDB_URL=http://localhost:8088 python deploy_ksql.py ksql_config.json
```

---

## 7. Running Components

### 7.1 Demo Dashboard

```bash
cd demo
BOOTSTRAP=localhost:9092 streamlit run demo_dashboard.py
```

### 7.2 Rule Engine

```bash
cd rule_engine
BOOTSTRAP_SERVERS=localhost:9092 python rule_engine.py
```

### 7.3 LLM Reasoner

```bash
cd llm_reasoner
BOOTSTRAP_SERVERS=localhost:9092 python llm_reasoner.py
```

### 7.4 Intervention Engine

```bash
cd intervention
BOOTSTRAP_SERVERS=localhost:9092 python intervention_engine.py
```

---

## 8. Observers (Experiment Monitoring)

```bash
cd observer

RUN_ID=exp_scaling_10 WARMUP_SEC=10 MEASURE_SEC=200 python3 cep_observer.py
RUN_ID=exp_scaling_10 WARMUP_SEC=10 MEASURE_SEC=200 python3 rule_observer.py
RUN_ID=exp_scaling_10 WARMUP_SEC=10 MEASURE_SEC=200 python3 llm_observer.py
RUN_ID=exp_scaling_10 WARMUP_SEC=10 MEASURE_SEC=200 python3 reasoning_requests_observer.py
RUN_ID=exp_scaling_10 WARMUP_SEC=10 MEASURE_SEC=200 python3 fast_llm_reasoner_observer.py
RUN_ID=exp_scaling_10 WARMUP_SEC=10 MEASURE_SEC=200 python3 intervention_observer.py
```

---

## 9. Simulator (Generate Events)

```bash
cd simulator

RUN_ID=unity_test \
NUM_USERS=5 \
MODE=mixed_risk \
EMIT_INTERVAL_SEC=10 \
DURATION_SEC=200 \
python3 event_generator.py
```

---

## 10. WebSocket Bridge

```bash
cd intervention

BOOTSTRAP=localhost:9092 \
EVENTS_TOPIC=intervention_events \
FEEDBACK_TOPIC=intervention_feedback \
WS_HOST=0.0.0.0 \
WS_PORT=8765 \
python3 intervention_ws_bridge.py
```

---

## 11. Feedback State Manager

```bash
cd intervention

BOOTSTRAP=localhost:9092 \
FEEDBACK_TOPIC=intervention_feedback \
STATE_FILE=./intervention_state.json \
python3 feedback_state_manager.py
```

---

## 12. Kafka Topic Example

Create a compacted topic:

```bash
docker exec -it kafka-cep-prototype_kafka_1 \
kafka-topics --bootstrap-server kafka:29092 \
--create --topic reasoning_requests_latest \
--partitions 1 --replication-factor 1 \
--config cleanup.policy=compact \
--config min.cleanable.dirty.ratio=0.01
```

---

## 13. Cleanup / Utility Commands

```bash
docker rm -f $(docker ps -aq --filter "name=")
```

---

## 14. Git Workflow (for Development)

```bash
git status
git add .
git commit -m "your message"
git push
```

---

## 15. Reproducibility Note

This release (**v1.0.0**) corresponds to the version used in the thesis experiments.

To reproduce results:

1. Start infrastructure with Docker Compose
2. Deploy CEP rules
3. Start reasoning and intervention services
4. Run simulator
5. Collect metrics using observer scripts

---

## 16. Disclaimer

This repository is intended for research and reproducibility purposes.
Configuration and paths may require minor adjustments depending on the local environment.
