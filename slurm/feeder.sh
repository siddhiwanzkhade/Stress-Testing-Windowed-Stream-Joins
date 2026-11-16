#!/bin/bash
#SBATCH --job-name=swj-feeder
#SBATCH --partition=long-40core-shared
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --time=2-00:00:00
#SBATCH --output=logs/slurm/feeder_%j.out
#
# Submits every experiment list in chunks, because SeaWulf allows at most 100 queued + running jobs per
# user per queue (every array task counts as one job). Every 5 minutes it tops each queue back up.
#   sbatch slurm/feeder.sh                                     # runs as a tiny 1-core job, survives logout
#   tail logs/slurm/feeder_*.out                               # progress
# Safe to cancel and resubmit: progress is saved in logs/feeder_state/ (delete that folder to start over).
set -uo pipefail
cd "${SLURM_SUBMIT_DIR:-$(dirname "$0")/..}"
SHARED="${SHARED:-short-40core-shared}"   # small runs (share nodes)
EXCL="${EXCL:-short-40core}"              # outer + scale (whole nodes)
LIMIT="${LIMIT:-95}"                      # SeaWulf limit is 100 per queue; keep a margin
STATE=logs/feeder_state
mkdir -p "$STATE" logs/slurm

queued() { squeue -h -r -u "$USER" -p "$1" | wc -l; }      # -r: count every array task

# feed <list> <queue> [extra sbatch args]: submit the next chunk of a list if that queue has room
feed() {
  local list="$1" part="$2"; shift 2
  local total next room end
  total=$(wc -l < "experiments/$list.txt")
  next=$(cat "$STATE/$list" 2>/dev/null || echo 1)
  (( next > total )) && return
  room=$(( LIMIT - $(queued "$part") ))
  (( room > 0 )) || return
  end=$(( next + room - 1 )); (( end > total )) && end=$total
  if sbatch --partition="$part" --array="${next}-${end}" "$@" slurm/run_array.slurm "experiments/$list.txt"; then
    echo $(( end + 1 )) > "$STATE/$list"
    echo "$(date '+%F %T') $list: submitted $next-$end of $total"
  fi
}

all_submitted() {
  local list
  for list in main disjoint policy window outer; do
    (( $(cat "$STATE/$list" 2>/dev/null || echo 1) > $(wc -l < "experiments/$list.txt") )) || return 1
  done
}

# scale: 6 multi-node jobs (1 infrastructure node + 1, 2, 4 workers; list split in 2), submitted once
if [[ ! -f "$STATE/scale_submitted" ]]; then
  for nodes in 2 3 5; do
    for part in scale_a scale_b; do
      sbatch --partition="$EXCL" --nodes=$nodes slurm/run_scale.slurm "experiments/$part.txt"
    done
  done
  touch "$STATE/scale_submitted"
fi

until all_submitted; do
  for list in main disjoint policy window; do feed "$list" "$SHARED"; done
  feed outer "$EXCL" --exclusive
  echo "$(date '+%F %T') queued: $SHARED=$(queued "$SHARED") $EXCL=$(queued "$EXCL")"
  sleep 300
done
echo "$(date '+%F %T') everything submitted; feeder done"
