"""
Replays a generated stream (pickups.jsonl or fares.jsonl) into a Kafka topic, respecting each record's
scheduled arrival time. Same timing logic as replay_stream.py (the file-based replay, kept as the control);
only the transport differs: records are produced to Kafka instead of written into a watched folder.

arrival_ts in each message is the wall-clock moment the record was handed to Kafka, so match latency
(emit_ts - arrival_ts) is measured the same way as in file mode.
"""
import argparse
import bisect
import json
import sys
import time
from datetime import datetime
from pathlib import Path

from confluent_kafka import Producer

from replay_stream import load_records


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="pickups.jsonl or fares.jsonl from generate_taxi_streams.py")
    ap.add_argument("--topic", required=True, help="Kafka topic to send to, e.g. pickups or fares")
    ap.add_argument("--bootstrap", default="localhost:9092", help="Kafka broker address, host:port")
    ap.add_argument("--delay-field", default=None, help="e.g. arrival_delay_seconds for the fares stream; omit for pickups")
    ap.add_argument("--speed-multiplier", type=float, default=60.0, help="time compression factor")
    ap.add_argument("--base-event-time", default=None,
                    help="ISO time that maps to replay offset 0. MUST be identical for the pickups and fares replays")
    ap.add_argument("--flush-interval-seconds", type=float, default=0.5, help="how often due records are sent")
    ap.add_argument("--start-at", type=float, default=None,
                    help="wall-clock epoch seconds at which replay offset 0 begins; pass the SAME value to both replays")
    args = ap.parse_args()

    records = load_records(Path(args.input), args.delay_field)
    base_event_time = (datetime.fromisoformat(args.base_event_time) if args.base_event_time
                       else min(datetime.fromisoformat(r["event_time"]) for r in records))

    # scheduled send time for each record, and each record turned into JSON once up front
    # (closing brace removed; arrival_ts is appended at send time as the last field)
    offsets, lines = [], []
    for r in records:
        et = datetime.fromisoformat(r["event_time"])
        offsets.append(((et - base_event_time).total_seconds() + r["_delay_s"]) / args.speed_multiplier)
        clean = {k: v for k, v in r.items() if not k.startswith("_") and k != "arrival_ts"}
        lines.append(json.dumps(clean)[:-1] + ', "arrival_ts": ')
    order = sorted(range(len(records)), key=offsets.__getitem__)
    offsets = [offsets[j] for j in order]
    lines = [lines[j] for j in order]
    del records

    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "linger.ms": 5,
        "batch.num.messages": 10000,
        "queue.buffering.max.messages": 2_000_000,
        "compression.type": "lz4",
        "acks": "1",
    })
    errors = []

    def on_delivery(err, _msg):
        if err is not None:
            errors.append(err)

    print(f"Replaying {len(lines)} records from {args.input} -> Kafka topic {args.topic} "
          f"(speed x{args.speed_multiplier}, send every {args.flush_interval_seconds}s)")

    if args.start_at is not None:
        wait = args.start_at - time.time()
        if wait > 0:
            time.sleep(wait)
        replay_start = args.start_at
    else:
        replay_start = time.time()

    i, n = 0, len(offsets)
    max_lag = 0.0
    while i < n:
        now_offset = time.time() - replay_start
        j = bisect.bisect_right(offsets, now_offset, lo=i)   # records i..j-1 are due (offsets are sorted)
        if j > i:
            max_lag = max(max_lag, now_offset - offsets[i])
            suffix = f"{time.time()}}}"                      # arrival_ts = moment of sending
            for line in lines[i:j]:
                while True:
                    try:
                        producer.produce(args.topic, value=line + suffix, on_delivery=on_delivery)
                        break
                    except BufferError:                      # local queue full: let it drain, then retry
                        producer.poll(0.05)
            producer.poll(0)
            i = j
        time.sleep(args.flush_interval_seconds)

    remaining = producer.flush(60)
    print(f"Done: {n} records sent in {time.time() - replay_start:.1f}s; max replay lag {max_lag:.2f}s")
    if errors or remaining:
        print(f"[error] {len(errors)} delivery errors, {remaining} messages never delivered")
        sys.exit(1)


if __name__ == "__main__":
    main()
