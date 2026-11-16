#!/bin/bash
#SBATCH --job-name=swj-scale
#SBATCH --partition=short-40core          # CHANGE: a queue with whole (non-shared) nodes; see `sinfo -s`
#SBATCH --nodes=3                         # 1 infrastructure node + 2 executor nodes; override with sbatch --nodes
#SBATCH --ntasks-per-node=1
#SBATCH --exclusive
#SBATCH --time=04:00:00
#SBATCH --output=logs/slurm/%x_%j.out
#
# Scale study on a real multi-node Spark cluster. Node 0 runs Kafka, the replay, the Spark master and the
# driver; every other node runs one Spark worker. So --nodes=N+1 gives N executor nodes:
#   sbatch --nodes=2 slurm/run_scale.slurm experiments/scale.txt      # 1 executor node
#   sbatch --nodes=3 slurm/run_scale.slurm experiments/scale.txt      # 2 executor nodes
#   sbatch --nodes=5 slurm/run_scale.slurm experiments/scale.txt      # 4 executor nodes
# Every line of the list file runs once on this cluster. Results: results/scale/<nodes>nodes_<jobid>.csv
# Env overrides: WORKER_CORES (default: all cores of a node), WORKER_MEM (default 96g),
#                EXECUTOR_MEM (default 80g), DRIVER_MEM (default 32g)
set -euo pipefail
cd "$SLURM_SUBMIT_DIR"
source slurm/env.sh
mkdir -p logs/slurm results/scale

LIST="${1:?usage: sbatch --nodes=N+1 slurm/run_scale.slurm experiments/scale.txt}"
mapfile -t NODES < <(scontrol show hostnames "$SLURM_JOB_NODELIST")
HEAD="${NODES[0]}"
WORKERS=("${NODES[@]:1}")
(( ${#WORKERS[@]} >= 1 )) || { echo "need --nodes 2 or more (1 head + executors)"; exit 1; }
CORES="${WORKER_CORES:-$(nproc)}"       # nodes in one queue are identical, so the head's count applies
TOTAL_CORES=$(( CORES * ${#WORKERS[@]} ))
SPARK_HOME="$(python -c 'import os, pyspark; print(os.path.dirname(pyspark.__file__))')"
export SPARK_HOME
LOGS="logs/slurm/scale_${SLURM_JOB_ID}"; mkdir -p "$LOGS"

KWORK="${TMPDIR:-/tmp}/swj_kafka_${SLURM_JOB_ID}"
BOOT="$(KAFKA_HOST="$HEAD" KAFKA_HEAP=8G scripts/kafka_start.sh "$KWORK" | tail -1)"

"$SPARK_HOME/bin/spark-class" org.apache.spark.deploy.master.Master \
  --host "$HEAD" --port 7077 --webui-port 8080 > "$LOGS/master.log" 2>&1 &
MASTER_PID=$!
trap 'kill $MASTER_PID 2>/dev/null; scripts/kafka_stop.sh "$KWORK"; rm -rf "$KWORK"' EXIT
sleep 5
for w in "${WORKERS[@]}"; do
  srun --nodes=1 --ntasks=1 --nodelist="$w" --overlap bash -c "
    source slurm/env.sh
    WD=\${TMPDIR:-/tmp}/swj_worker_${SLURM_JOB_ID}; mkdir -p \$WD
    exec \"$SPARK_HOME/bin/spark-class\" org.apache.spark.deploy.worker.Worker spark://$HEAD:7077 \
      --cores $CORES --memory ${WORKER_MEM:-96g} --work-dir \$WD" > "$LOGS/worker_$w.log" 2>&1 &
done

# wait until every worker has registered with the master (read from the master log, not the web UI,
# because the UI port moves when 8080 is taken on the node)
REG=0
for _ in $(seq 60); do
  REG=$(grep -c "Registering worker" "$LOGS/master.log" || true)
  (( REG >= ${#WORKERS[@]} )) && break
  sleep 3
done
(( REG >= ${#WORKERS[@]} )) || { echo "Spark workers did not register ($REG of ${#WORKERS[@]}); see the worker logs"; exit 1; }
echo "$REG Spark workers registered"


echo "cluster: head=$HEAD workers=${WORKERS[*]} cores/worker=$CORES total=$TOTAL_CORES kafka=$BOOT"
while IFS= read -r LINE || [[ -n "$LINE" ]]; do
  [[ -z "$LINE" || "$LINE" == \#* ]] && continue
  eval "ARGS=($LINE)"
  echo "##### $(date +%T) $LINE"
  ./run_sweep.sh --source kafka --kafka-bootstrap "$BOOT" \
    --spark-master "spark://$HEAD:7077" --driver-memory "${DRIVER_MEM:-32g}" \
    --spark-conf "spark.executor.memory=${EXECUTOR_MEM:-80g}" \
    --shuffle-partitions $(( TOTAL_CORES * 2 )) --kafka-partitions "$TOTAL_CORES" \
    --state-store rocksdb --warmup 45 \
    "${ARGS[@]}" \
    --run-root "runs_scale/${SLURM_JOB_ID}" \
    --results-log "results/scale/${#WORKERS[@]}nodes_${SLURM_JOB_ID}.csv" \
    --tags "experiment=scale,executor_nodes=${#WORKERS[@]},total_cores=$TOTAL_CORES,job=$SLURM_JOB_ID" \
    < /dev/null || echo "FAILED: $LINE"      # </dev/null: the run must not eat the list file being read
done < "$LIST"
echo "scale job done $(date +%T)"
