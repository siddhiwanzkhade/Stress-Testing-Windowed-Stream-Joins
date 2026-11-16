#!/usr/bin/env bash
# Runs ONE full condition end-to-end: generate -> live replay -> Spark join -> score.
# This exists because manually running 4 terminals per condition (as in the
# Quickstart) doesn't scale to the dozens of condition/watermark/seed
# combinations Week 5-6 needs.
#
# Usage example:
#   ./run_sweep.sh \
#     --condition lateness_only --watermark "10 minutes" --join-window "24 hours" \
#     --seed 1 --lateness-fraction 0.15 --burst-fraction 0 --skew-amplify-factor 1 \
#     --num-trips 200000 --speed-multiplier 60
#
# Kafka instead of watched folders (start a broker first: BOOT=$(scripts/kafka_start.sh ./kafka_work | tail -1)):
#   ./run_sweep.sh ... --source kafka --kafka-bootstrap "$BOOT"
#
# All output lands under <run-root>/<condition>/seed<seed>/wm_<watermark>/   (run-root defaults to ./runs)
set -euo pipefail

TLC_PARQUET="./data/raw/yellow_tripdata_2026-01.parquet"
NUM_TRIPS=200000
SPAN_HOURS=48
MAX_TRIP_MIN=60
SPEED_MULT=60
LATENESS_FRACTION=0
LATENESS_SCALE=1800
BURST_FRACTION=0
BURST_WINDOW=120
SKEW_TOPK=5
SKEW_FACTOR=1
SKEW_DELAY_SHARE=0
BURST_SCATTER=""
DISJOINT=""
OVERLAY_DAYS=1
JOIN_KEY="trip_id"
JOIN_TYPE="inner"
TAIL_HOURS=0
SKEW_DELAY_SCALE=90
RESULTS_LOG="./output/results.csv"
RUN_ROOT="./runs"
TAGS=""
# ingestion: file = watched folders (original setup, the control); kafka = topics on a running broker
SOURCE="file"
KAFKA_BOOTSTRAP=""
KAFKA_PARTITIONS=8
MAX_OFFSETS=""
# Spark
TRIGGER_SECONDS=3
SHUFFLE_PARTITIONS=8
STATE_STORE=default
WATERMARK_POLICY=min
DRIVER_MEMORY=""
SPARK_MASTER=""
SPARK_CONFS=()
# timing
WARMUP=20           # seconds for Spark to start before the replay begins (raise for multi-node clusters)
REPLAY_LEAD=10      # seconds both replays get to load their files before the shared start
DRAIN_TIMEOUT=600   # max seconds to wait for Spark to finish after the replay ends

while [[ $# -gt 0 ]]; do
  case "$1" in
    --condition) CONDITION="$2"; shift 2;;
    --watermark) WATERMARK="$2"; shift 2;;
    --join-window) JOIN_WINDOW="$2"; shift 2;;
    --seed) SEED="$2"; shift 2;;
    --lateness-fraction) LATENESS_FRACTION="$2"; shift 2;;
    --lateness-scale|--lateness-scale-seconds) LATENESS_SCALE="$2"; shift 2;;
    --burst-fraction) BURST_FRACTION="$2"; shift 2;;
    --burst-window|--burst-window-seconds) BURST_WINDOW="$2"; shift 2;;
    --skew-topk|--skew-top-k) SKEW_TOPK="$2"; shift 2;;
    --skew-amplify-factor) SKEW_FACTOR="$2"; shift 2;;
    --skew-delay-hot-share) SKEW_DELAY_SHARE="$2"; shift 2;;
    --skew-delay-scale|--skew-delay-scale-seconds) SKEW_DELAY_SCALE="$2"; shift 2;;
    --burst-scatter) BURST_SCATTER="--burst-scatter"; shift;;
    --disjoint-stressors) DISJOINT="--disjoint-stressors"; shift;;
    --overlay-days) OVERLAY_DAYS="$2"; shift 2;;
    --join-key) JOIN_KEY="$2"; shift 2;;
    --join-type) JOIN_TYPE="$2"; shift 2;;
    --tail-hours) TAIL_HOURS="$2"; shift 2;;
    --num-trips) NUM_TRIPS="$2"; shift 2;;
    --span-hours) SPAN_HOURS="$2"; shift 2;;
    --max-trip-minutes) MAX_TRIP_MIN="$2"; shift 2;;
    --tlc-parquet) TLC_PARQUET="$2"; shift 2;;
    --speed-multiplier) SPEED_MULT="$2"; shift 2;;
    --results-log) RESULTS_LOG="$2"; shift 2;;
    --run-root) RUN_ROOT="$2"; shift 2;;
    --tags) TAGS="$2"; shift 2;;
    --source) SOURCE="$2"; shift 2;;
    --kafka-bootstrap) KAFKA_BOOTSTRAP="$2"; shift 2;;
    --kafka-partitions) KAFKA_PARTITIONS="$2"; shift 2;;
    --max-offsets-per-trigger) MAX_OFFSETS="$2"; shift 2;;
    --trigger-seconds) TRIGGER_SECONDS="$2"; shift 2;;
    --shuffle-partitions) SHUFFLE_PARTITIONS="$2"; shift 2;;
    --state-store) STATE_STORE="$2"; shift 2;;
    --watermark-policy) WATERMARK_POLICY="$2"; shift 2;;
    --driver-memory) DRIVER_MEMORY="$2"; shift 2;;
    --spark-master) SPARK_MASTER="$2"; shift 2;;
    --spark-conf) SPARK_CONFS+=("$2"); shift 2;;
    --warmup) WARMUP="$2"; shift 2;;
    --replay-lead) REPLAY_LEAD="$2"; shift 2;;
    --drain-timeout) DRAIN_TIMEOUT="$2"; shift 2;;
    *) echo "Unknown arg: $1"; exit 1;;
  esac
done

for req in CONDITION WATERMARK JOIN_WINDOW SEED; do
  if [[ -z "${!req:-}" ]]; then echo "Missing required --$(echo "$req" | tr '[:upper:]_' '[:lower:]-')"; exit 1; fi
done
if [[ "$SOURCE" == "kafka" && -z "$KAFKA_BOOTSTRAP" ]]; then echo "--source kafka needs --kafka-bootstrap host:port"; exit 1; fi

RUN_DIR="${RUN_ROOT}/${CONDITION}/seed${SEED}/wm_${WATERMARK// /_}"
rm -rf "$RUN_DIR"
mkdir -p "$RUN_DIR"/{data,stream/pickups,stream/fares,output,chk}
RUN_DIR="$(cd "$RUN_DIR" && pwd)"   # absolute: executors on other nodes must find output and checkpoints

# stop this run's background processes (and only these) however the script exits
SPARK_PID=""; PICKUP_PID=""; FARE_PID=""; TOPICS=()
cleanup() {
  for pid in $PICKUP_PID $FARE_PID $SPARK_PID; do kill "$pid" 2>/dev/null || true; done
  if [[ ${#TOPICS[@]} -gt 0 ]]; then
    python data/kafka_topics.py --bootstrap "$KAFKA_BOOTSTRAP" delete "${TOPICS[@]}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo "=== [$CONDITION seed=$SEED wm=$WATERMARK win=$JOIN_WINDOW source=$SOURCE] generating data ==="
python data/generate_taxi_streams.py \
  --tlc-parquet "$TLC_PARQUET" --num-trips "$NUM_TRIPS" --span-hours "$SPAN_HOURS" --max-trip-minutes "$MAX_TRIP_MIN" --seed "$SEED" --tail-hours "$TAIL_HOURS" \
  --lateness-fraction "$LATENESS_FRACTION" --lateness-scale-seconds "$LATENESS_SCALE" \
  --burst-fraction "$BURST_FRACTION" --burst-window-seconds "$BURST_WINDOW" \
  --skew-top-k "$SKEW_TOPK" --skew-amplify-factor "$SKEW_FACTOR" \
  --skew-delay-hot-share "$SKEW_DELAY_SHARE" --skew-delay-scale-seconds "$SKEW_DELAY_SCALE" $BURST_SCATTER $DISJOINT \
  --overlay-days "$OVERLAY_DAYS" --out-dir "$RUN_DIR/data"

BASE_TIME=$(python -c "import json,sys; print(min(json.loads(l)['event_time'] for l in open('$RUN_DIR/data/pickups.jsonl')))")
echo "shared replay base time: $BASE_TIME"

SPARK_EXTRA=(--source "$SOURCE" --trigger-seconds "$TRIGGER_SECONDS" --shuffle-partitions "$SHUFFLE_PARTITIONS"
             --state-store "$STATE_STORE" --watermark-policy "$WATERMARK_POLICY")
if [[ -n "$DRIVER_MEMORY" ]]; then SPARK_EXTRA+=(--driver-memory "$DRIVER_MEMORY"); fi
if [[ -n "$SPARK_MASTER" ]]; then SPARK_EXTRA+=(--master "$SPARK_MASTER"); fi
for kv in ${SPARK_CONFS[@]+"${SPARK_CONFS[@]}"}; do SPARK_EXTRA+=(--spark-conf "$kv"); done

if [[ "$SOURCE" == "kafka" ]]; then
  # fresh topics per run, so runs sharing a broker can never read each other's records
  TOPIC_BASE=$(echo "${CONDITION}-s${SEED}-${WATERMARK}-$$-$(date +%s)" | tr -c 'A-Za-z0-9-\n' '-')
  TOPICS=("${TOPIC_BASE}-pickups" "${TOPIC_BASE}-fares")
  python data/kafka_topics.py --bootstrap "$KAFKA_BOOTSTRAP" create "${TOPICS[@]}" --partitions "$KAFKA_PARTITIONS"
  SPARK_EXTRA+=(--kafka-bootstrap "$KAFKA_BOOTSTRAP" --pickups-topic "${TOPICS[0]}" --fares-topic "${TOPICS[1]}")
  if [[ -n "$MAX_OFFSETS" ]]; then SPARK_EXTRA+=(--max-offsets-per-trigger "$MAX_OFFSETS"); fi
fi

echo "=== starting Spark join ==="
python spark/streaming_join.py \
  --pickups-dir "$RUN_DIR/stream/pickups" --fares-dir "$RUN_DIR/stream/fares" \
  --watermark "$WATERMARK" --join-window "$JOIN_WINDOW" --join-key "$JOIN_KEY" --join-type "$JOIN_TYPE" \
  --output-path "$RUN_DIR/output/matches" --checkpoint-dir "$RUN_DIR/chk" \
  --metrics-out "$RUN_DIR/output/metrics.jsonl" "${SPARK_EXTRA[@]}" \
  > "$RUN_DIR/spark.log" 2>&1 &
SPARK_PID=$!
sleep "$WARMUP"  # JVM warmup before replay starts, so early records aren't missed by a cold query
if ! kill -0 "$SPARK_PID" 2>/dev/null; then echo "Spark exited during warmup; see $RUN_DIR/spark.log"; tail -20 "$RUN_DIR/spark.log"; exit 1; fi

START_AT=$(python -c "import time; print(time.time() + $REPLAY_LEAD)")
echo "=== replaying pickups + fares via $SOURCE (shared start in ${REPLAY_LEAD}s) ==="
if [[ "$SOURCE" == "kafka" ]]; then
  python data/kafka_replay.py --input "$RUN_DIR/data/pickups.jsonl" --topic "${TOPICS[0]}" \
    --bootstrap "$KAFKA_BOOTSTRAP" --speed-multiplier "$SPEED_MULT" --base-event-time "$BASE_TIME" \
    --start-at "$START_AT" > "$RUN_DIR/replay_pickups.log" 2>&1 &
  PICKUP_PID=$!
  python data/kafka_replay.py --input "$RUN_DIR/data/fares.jsonl" --topic "${TOPICS[1]}" \
    --bootstrap "$KAFKA_BOOTSTRAP" --delay-field arrival_delay_seconds \
    --speed-multiplier "$SPEED_MULT" --base-event-time "$BASE_TIME" \
    --start-at "$START_AT" > "$RUN_DIR/replay_fares.log" 2>&1 &
  FARE_PID=$!
else
  python data/replay_stream.py --input "$RUN_DIR/data/pickups.jsonl" \
    --out-dir "$RUN_DIR/stream/pickups" --speed-multiplier "$SPEED_MULT" --base-event-time "$BASE_TIME" \
    --start-at "$START_AT" > "$RUN_DIR/replay_pickups.log" 2>&1 &
  PICKUP_PID=$!
  python data/replay_stream.py --input "$RUN_DIR/data/fares.jsonl" \
    --out-dir "$RUN_DIR/stream/fares" --delay-field arrival_delay_seconds \
    --speed-multiplier "$SPEED_MULT" --base-event-time "$BASE_TIME" \
    --start-at "$START_AT" > "$RUN_DIR/replay_fares.log" 2>&1 &
  FARE_PID=$!
fi

wait $PICKUP_PID $FARE_PID
PICKUP_PID=""; FARE_PID=""
tail -n 2 "$RUN_DIR/replay_pickups.log" "$RUN_DIR/replay_fares.log"
echo "=== replay finished, waiting for Spark to drain ==="
# under heavy load Spark can still be behind when the replay ends; stopping on a fixed timer would count
# those unprocessed trips as lost. Wait until Spark has read every replayed record (sum of numInputRows
# = pickups + fares sent) and its input count has stopped changing.
EXPECTED=$(( $(wc -l < "$RUN_DIR/data/pickups.jsonl") + $(wc -l < "$RUN_DIR/data/fares.jsonl") ))
python - "$RUN_DIR/output/metrics.jsonl" "$EXPECTED" "$DRAIN_TIMEOUT" "$TRIGGER_SECONDS" <<'EOF'
import json, sys, time
path, expected, timeout, trigger = sys.argv[1], int(sys.argv[2]), float(sys.argv[3]), float(sys.argv[4])
def rows_read():
    total = 0
    try:
        for line in open(path):
            try: total += json.loads(line).get("numInputRows", 0)
            except ValueError: pass
    except OSError: pass
    return total
deadline, last, stable_since = time.time() + timeout, -1, time.time()
while time.time() < deadline:
    n = rows_read()
    if n != last:
        last, stable_since = n, time.time()
    # all input read, and no new batch for a few triggers (lets the final batch's output be written)
    if n >= expected and time.time() - stable_since >= 3 * trigger + 5:
        print(f"Spark read all {n} records"); sys.exit(0)
    time.sleep(2)
print(f"[warn] after {timeout:.0f}s Spark had read {last} of {expected} records; the rest count as lost")
EOF

kill $SPARK_PID 2>/dev/null || true
wait $SPARK_PID 2>/dev/null || true
SPARK_PID=""

echo "=== scoring ==="
AUTO_TAGS="source=$SOURCE,overlay_days=$OVERLAY_DAYS,disjoint=$([[ -n "$DISJOINT" ]] && echo 1 || echo 0),join_type=$JOIN_TYPE,join_key=$JOIN_KEY,speed=$SPEED_MULT,trigger_s=$TRIGGER_SECONDS,shuffle_partitions=$SHUFFLE_PARTITIONS,state_store=$STATE_STORE,watermark_policy=$WATERMARK_POLICY"
python analysis/score_run.py \
  --ground-truth "$RUN_DIR/data/ground_truth.jsonl" \
  --matches "$RUN_DIR/output/matches" \
  --condition "$CONDITION" --watermark "$WATERMARK" --join-window "$JOIN_WINDOW" \
  --seed "$SEED" --results-log "$RESULTS_LOG" --speed-multiplier "$SPEED_MULT" \
  --metrics "$RUN_DIR/output/metrics.jsonl" --tags "${AUTO_TAGS}${TAGS:+,$TAGS}"

echo "=== done: $RUN_DIR ==="
