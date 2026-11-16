#!/usr/bin/env bash
# Stops the broker started by kafka_start.sh with the same <work-dir>. Its data stays in <work-dir>/data;
# delete the work dir yourself when you no longer need it.
set -uo pipefail
WORK="${1:?usage: kafka_stop.sh <work-dir>}"
[[ -f "$WORK/kafka.pid" ]] || exit 0
PID=$(cat "$WORK/kafka.pid")
kill "$PID" 2>/dev/null || exit 0
for _ in $(seq 30); do kill -0 "$PID" 2>/dev/null || break; sleep 1; done
kill -9 "$PID" 2>/dev/null || true
rm -f "$WORK/kafka.pid"
echo "Kafka stopped"
