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

### 3.1 Start Core Infrastructure

```bash
docker-compose up -d zookeeper kafka ksqldb ollama_full ollama_fast
docker-compose up ksql_deployer
```

### 3.2 Start Long-Running Services

```bash
docker-compose up -d rule_engine reasoning_scheduler llm_reasoner fast_llm_reasoner intervention_engine
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

## 6. Observers (Experiment Monitoring)

```bash
cd observer
RUN_ID=exampleID WARMUP_SEC=10 MEASURE_SEC=200 python3 cep_observer.py
RUN_ID=exampleID WARMUP_SEC=10 MEASURE_SEC=200 python3 rule_observer.py
RUN_ID=exampleID WARMUP_SEC=10 MEASURE_SEC=200 python3 llm_observer.py
RUN_ID=exampleID WARMUP_SEC=10 MEASURE_SEC=200 python3 reasoning_requests_observer.py
RUN_ID=exampleID WARMUP_SEC=10 MEASURE_SEC=200 python3 fast_llm_reasoner_observer.py
RUN_ID=exampleID WARMUP_SEC=10 MEASURE_SEC=200 python3 intervention_observer.py
```

---

## 7. Simulator (Generate Events)

```bash
cd simulator
RUN_ID=exampleID NUM_USERS=5 MODE=mixed_risk EMIT_INTERVAL_SEC=10 DURATION_SEC=200 python3 event_generator.py
```

---

## 8. Reproducibility Note

This release (**v1.0.0**) corresponds to the version used in the thesis experiments.

To reproduce results:

1. Start infrastructure with Docker Compose
2. Deploy CEP rules
3. Start reasoning and intervention services
4. Run simulator
5. Collect metrics using observer scripts

---

## 9. Disclaimer

This repository is intended for research and reproducibility purposes.
Configuration and paths may require minor adjustments depending on the local environment.
