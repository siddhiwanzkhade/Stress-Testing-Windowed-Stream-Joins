#!/usr/bin/env bash
# Starts a single-node Kafka broker (KRaft mode, no ZooKeeper) and prints its bootstrap address
# (host:port) as the LAST line of output. Free ports are picked automatically, so several brokers can
# share a node (e.g. job-array tasks on a shared partition).
#   BOOT=$(scripts/kafka_start.sh <work-dir> | tail -1)      ...      scripts/kafka_stop.sh <work-dir>
# Env: KAFKA_HOST (default: hostname under SLURM, else localhost), KAFKA_HEAP (default 2G),
#      KAFKA_HOME (default ./tools/kafka_2.13-4.1.0, installed by scripts/setup_kafka.sh)
set -euo pipefail
REPO="$(cd "$(dirname "$0")/.." && pwd)"
WORK="${1:?usage: kafka_start.sh <work-dir>}"
KAFKA_HOME="${KAFKA_HOME:-$REPO/tools/kafka_2.13-${KAFKA_VERSION:-4.1.0}}"
if [[ ! -x "$KAFKA_HOME/bin/kafka-server-start.sh" ]]; then
  echo "Kafka not found at $KAFKA_HOME -- run scripts/setup_kafka.sh first" >&2; exit 1
fi
if [[ -n "${SLURM_JOB_ID:-}" ]]; then HOST="${KAFKA_HOST:-$(hostname)}"; else HOST="${KAFKA_HOST:-localhost}"; fi

mkdir -p "$WORK"
WORK="$(cd "$WORK" && pwd)"
read -r PORT CPORT < <(python -c "
import socket
s = [socket.socket() for _ in range(2)]
for x in s: x.bind(('', 0))
print(*[x.getsockname()[1] for x in s])")

cat > "$WORK/server.properties" <<PROPS
process.roles=broker,controller
node.id=1
controller.quorum.bootstrap.servers=localhost:${CPORT}
listeners=PLAINTEXT://:${PORT},CONTROLLER://:${CPORT}
advertised.listeners=PLAINTEXT://${HOST}:${PORT},CONTROLLER://localhost:${CPORT}
inter.broker.listener.name=PLAINTEXT
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs=${WORK}/data
num.partitions=1
auto.create.topics.enable=false
offsets.topic.replication.factor=1
share.coordinator.state.topic.replication.factor=1
share.coordinator.state.topic.min.isr=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
log.retention.hours=24
group.initial.rebalance.delay.ms=0
PROPS

"$KAFKA_HOME/bin/kafka-storage.sh" format --standalone -t "$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)" \
  -c "$WORK/server.properties" > "$WORK/format.log" 2>&1
LOG_DIR="$WORK/logs" KAFKA_HEAP_OPTS="-Xms${KAFKA_HEAP:-2G} -Xmx${KAFKA_HEAP:-2G}" \
  nohup "$KAFKA_HOME/bin/kafka-server-start.sh" "$WORK/server.properties" > "$WORK/kafka.out" 2>&1 &
echo $! > "$WORK/kafka.pid"
python "$REPO/data/kafka_topics.py" --bootstrap "${HOST}:${PORT}" ready --timeout 90 >&2 \
  || { echo "Kafka failed to start; see $WORK/kafka.out" >&2; exit 1; }
echo "${HOST}:${PORT}" > "$WORK/bootstrap"
echo "${HOST}:${PORT}"
