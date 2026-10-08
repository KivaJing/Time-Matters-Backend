#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./experiments/run_scaling_one.sh 10 1
#   ./experiments/run_scaling_one.sh 100 3

N_USERS="${1:?Usage: $0 <num_users> <run_number>}"
RUN_NO="${2:?Usage: $0 <num_users> <run_number>}"

ROOT="$HOME/kafka-cep-prototype"

WARMUP_SEC=10
MEASURE_SEC=200
SIM_DURATION_SEC=$((WARMUP_SEC + MEASURE_SEC))
COOLDOWN_SEC=20

RUN_ID="scaling_u${N_USERS}_r${RUN_NO}"
OUT_DIR="$ROOT/runs/$RUN_ID"

mkdir -p "$OUT_DIR"

cd "$ROOT"
source venv/bin/activate

echo "=================================================="
echo "RUN_ID=$RUN_ID"
echo "NUM_USERS=$N_USERS"
echo "WARMUP_SEC=$WARMUP_SEC"
echo "MEASURE_SEC=$MEASURE_SEC"
echo "SIM_DURATION_SEC=$SIM_DURATION_SEC"
echo "OUTPUT=$OUT_DIR"
echo "=================================================="

date -Is > "$OUT_DIR/start_time.txt"
git rev-parse HEAD > "$OUT_DIR/git_commit.txt"
git status --short > "$OUT_DIR/git_status.txt"
docker compose config > "$OUT_DIR/docker-compose-resolved.yml"
docker compose ps > "$OUT_DIR/docker-compose-ps-before.txt"

# --------------------------------------------------
# Reset application-level in-memory state between runs
# Kafka, ksqlDB and Ollama remain running.
# --------------------------------------------------

docker compose restart \
  rule_engine \
  reasoning_scheduler \
  llm_reasoner \
  fast_llm_reasoner \
  intervention_engine

sleep 5

# --------------------------------------------------
# Resource monitoring
# --------------------------------------------------

nvidia-smi \
  --query-gpu=timestamp,utilization.gpu,utilization.memory,memory.used,memory.total,power.draw \
  --format=csv \
  -l 1 \
  > "$OUT_DIR/gpu.csv" 2>&1 &

GPU_PID=$!

(
  while true; do
    echo "TIMESTAMP=$(date -Is)"
    docker stats --no-stream \
      --format "{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.NetIO}},{{.BlockIO}},{{.PIDs}}"
    sleep 1
  done
) > "$OUT_DIR/docker_stats.log" 2>&1 &

STATS_PID=$!

# --------------------------------------------------
# Start observers
# --------------------------------------------------

RUN_ID="$RUN_ID" \
WARMUP_SEC="$WARMUP_SEC" \
MEASURE_SEC="$MEASURE_SEC" \
BOOTSTRAP_SERVERS=localhost:9092 \
PYTHONUNBUFFERED=1 \
python3 observer/cep_observer.py \
> "$OUT_DIR/cep_observer.log" 2>&1 &
PID_CEP=$!

RUN_ID="$RUN_ID" \
WARMUP_SEC="$WARMUP_SEC" \
MEASURE_SEC="$MEASURE_SEC" \
BOOTSTRAP_SERVERS=localhost:9092 \
PYTHONUNBUFFERED=1 \
python3 observer/rule_observer.py \
> "$OUT_DIR/rule_observer.log" 2>&1 &
PID_RULE=$!

RUN_ID="$RUN_ID" \
WARMUP_SEC="$WARMUP_SEC" \
MEASURE_SEC="$MEASURE_SEC" \
BOOTSTRAP_SERVERS=localhost:9092 \
PYTHONUNBUFFERED=1 \
python3 observer/reasoning_requests_observer.py \
> "$OUT_DIR/reasoning_requests_observer.log" 2>&1 &
PID_REQ=$!

RUN_ID="$RUN_ID" \
WARMUP_SEC="$WARMUP_SEC" \
MEASURE_SEC="$MEASURE_SEC" \
BOOTSTRAP_SERVERS=localhost:9092 \
PYTHONUNBUFFERED=1 \
python3 observer/llm_observer.py \
> "$OUT_DIR/llm_observer.log" 2>&1 &
PID_LLM=$!

RUN_ID="$RUN_ID" \
WARMUP_SEC="$WARMUP_SEC" \
MEASURE_SEC="$MEASURE_SEC" \
BOOTSTRAP=localhost:9092 \
PYTHONUNBUFFERED=1 \
python3 observer/fast_llm_reasoner_observer.py \
> "$OUT_DIR/fast_llm_observer.log" 2>&1 &
PID_FAST=$!

RUN_ID="$RUN_ID" \
WARMUP_SEC="$WARMUP_SEC" \
MEASURE_SEC="$MEASURE_SEC" \
BOOTSTRAP_SERVERS=localhost:9092 \
PYTHONUNBUFFERED=1 \
python3 observer/intervention_observer.py \
> "$OUT_DIR/intervention_observer.log" 2>&1 &
PID_INT=$!

# Give all Kafka consumers a moment to subscribe.
sleep 1

# --------------------------------------------------
# Start simulator
#
# 210 seconds total:
# first 10 s = warm-up
# next 200 s = formal measurement
# --------------------------------------------------

RUN_ID="$RUN_ID" \
NUM_USERS="$N_USERS" \
MODE=mixed_risk \
EMIT_INTERVAL_SEC=10 \
DURATION_SEC="$SIM_DURATION_SEC" \
BASE_SEED=42 \
PYTHONUNBUFFERED=1 \
python3 simulator/event_generator.py \
> "$OUT_DIR/simulator.log" 2>&1 &

PID_SIM=$!

echo "Simulator PID: $PID_SIM"

# --------------------------------------------------
# Wait for simulator and observers
# --------------------------------------------------

wait "$PID_SIM" || true

wait "$PID_CEP" || true
wait "$PID_RULE" || true
wait "$PID_REQ" || true
wait "$PID_LLM" || true
wait "$PID_FAST" || true
wait "$PID_INT" || true

# --------------------------------------------------
# Stop resource monitors
# --------------------------------------------------

kill "$GPU_PID" 2>/dev/null || true
kill "$STATS_PID" 2>/dev/null || true

wait "$GPU_PID" 2>/dev/null || true
wait "$STATS_PID" 2>/dev/null || true

# --------------------------------------------------
# Save service logs for this run
# --------------------------------------------------

START_TIME="$(cat "$OUT_DIR/start_time.txt")"

docker compose logs \
  --no-color \
  --since "$START_TIME" \
  reasoning_scheduler \
  llm_reasoner \
  fast_llm_reasoner \
  rule_engine \
  intervention_engine \
  > "$OUT_DIR/services.log" 2>&1 || true

# Kafka consumer lag snapshot
docker compose exec -T kafka \
  kafka-consumer-groups \
  --bootstrap-server kafka:29092 \
  --all-groups \
  --describe \
  > "$OUT_DIR/kafka_lag_after.txt" 2>&1 || true

docker compose ps > "$OUT_DIR/docker-compose-ps-after.txt"

date -Is > "$OUT_DIR/end_time.txt"

echo
echo "RUN FINISHED: $RUN_ID"
echo "Results saved in: $OUT_DIR"
echo

echo "Cooldown ${COOLDOWN_SEC}s..."
sleep "$COOLDOWN_SEC"
