#!/bin/bash
#SBATCH --job-name=swj
#SBATCH --partition=short-40core-shared   # CHANGE: see `sinfo -s`; a -shared queue lets small runs share a node
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --time=00:45:00
#SBATCH --output=logs/slurm/%x_%A_%a.out
#
# One experiment per array task. Each task starts its own Kafka broker, runs one line of the list file
# through run_sweep.sh, and writes its own result CSV, so a failed task can be resubmitted alone.
#
#   python slurm/make_experiments.py          # writes experiments/*.txt
#   mkdir -p logs/slurm
#   sbatch --array=1-$(wc -l < experiments/main.txt)%20 slurm/run_array.slurm experiments/main.txt
#   (%20 = at most 20 tasks at once.) Then: python analysis/merge_results.py results/array/main -o ...
#
# Latency runs (experiments/outer.txt) must not share CPUs; give them whole nodes:
#   sbatch --array=1-N%10 --partition=<non-shared queue> --exclusive slurm/run_array.slurm experiments/outer.txt
set -euo pipefail
cd "$SLURM_SUBMIT_DIR"
source slurm/env.sh

LIST="${1:?usage: sbatch --array=1-N slurm/run_array.slurm experiments/<name>.txt}"
NAME="$(basename "$LIST" .txt)"
TASK="${SLURM_ARRAY_TASK_ID:?submit with sbatch --array=...}"
LINE="$(sed -n "${TASK}p" "$LIST")"
[[ -n "$LINE" ]] || { echo "no line $TASK in $LIST"; exit 1; }
eval "ARGS=($LINE)"

KWORK="${TMPDIR:-/tmp}/swj_kafka_${SLURM_JOB_ID}"
BOOT="$(scripts/kafka_start.sh "$KWORK" | tail -1)"
RUN_ROOT="runs_array/$NAME/task$TASK"
trap 'scripts/kafka_stop.sh "$KWORK"; rm -rf "$KWORK" "$RUN_ROOT"/*/seed*/wm_*/stream "$RUN_ROOT"/*/seed*/wm_*/chk' EXIT

echo "task $TASK of $NAME on $(hostname), Kafka at $BOOT: $LINE"
# defaults first, so anything on the experiment line overrides them
./run_sweep.sh --source kafka --kafka-bootstrap "$BOOT" --driver-memory 8g \
  "${ARGS[@]}" \
  --run-root "$RUN_ROOT" \
  --results-log "results/array/$NAME/task_$(printf %04d "$TASK").csv" \
  --tags "experiment=$NAME,task=$TASK,job=${SLURM_ARRAY_JOB_ID:-$SLURM_JOB_ID},host=$(hostname)"
