#!/usr/bin/env bash
# One-time setup, run where there is internet (your laptop, or the SeaWulf LOGIN node; compute nodes are
# usually offline):
#   1. downloads Kafka into ./tools/
#   2. downloads Spark's Kafka connector jars into ./jars/, so Spark never needs the internet at run time
# Needs: Java 17 or 21 on PATH, and the project's Python venv active (pyspark installed).
set -euo pipefail
cd "$(dirname "$0")/.."
KAFKA_VERSION="${KAFKA_VERSION:-4.1.0}"
KAFKA_DIR="tools/kafka_2.13-${KAFKA_VERSION}"

if [[ ! -x "$KAFKA_DIR/bin/kafka-server-start.sh" ]]; then
  mkdir -p tools
  echo "=== downloading Kafka ${KAFKA_VERSION} ==="
  curl -fSL -o "tools/kafka.tgz" "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_2.13-${KAFKA_VERSION}.tgz"
  tar -xzf tools/kafka.tgz -C tools && rm tools/kafka.tgz
fi
echo "Kafka: $KAFKA_DIR"

echo "=== fetching Spark's Kafka connector jars ==="
# --watermark/--join-window are required flags; their values are unused in this mode
python spark/streaming_join.py --fetch-kafka-jars --watermark "1 minute" --join-window "1 minute"
ls jars
