"""
Replays a generated stream (pickups.jsonl or fares.jsonl) into a directory
that a Spark file-streaming source polls, respecting each record's actual
scheduled arrival time -- not just writing everything at once.

This is what makes "lateness" and "burstiness" real for local testing: if we
just wrote all records into one static file, Spark's file source would read
them all in one micro-batch with no meaningful notion of "late arrival" at
all. Here, a record with arrival_delay_seconds=1800 genuinely doesn't land
in the watched directory until 1800 (simulated) seconds after its event
time -- so the watermark either has or hasn't advanced past it for real.

--speed-multiplier compresses wall-clock time so a --span-hours run from the
generator doesn't take literal hours to replay locally (e.g. 60x makes an
hour of simulated span replay in one minute).
"""
import argparse
import bisect
import json
import os
import time
from datetime import datetime
from pathlib import Path


def load_records(path: Path, delay_field: str | None):
    records = []
    with path.open() as f:
        for line in f:
            r = json.loads(line)
            r["_delay_s"] = float(r.get(delay_field, 0.0)) if delay_field else 0.0
            records.append(r)
    return records


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="pickups.jsonl or fares.jsonl from generate_taxi_streams.py")
    ap.add_argument("--out-dir", required=True, help="directory Spark's file-streaming source polls")
    ap.add_argument("--delay-field", default=None, help="e.g. arrival_delay_seconds for the fares stream; omit for pickups")
    ap.add_argument("--speed-multiplier", type=float, default=60.0, help="time compression factor")
    ap.add_argument("--base-event-time", default=None,
                    help="ISO time that maps to replay offset 0. MUST be identical for the pickups and fares replays; "
                         "otherwise each file uses its own min event time and the streams are misaligned.")
    ap.add_argument("--flush-interval-seconds", type=float, default=2.0, help="how often to write a new batch file")
    ap.add_argument("--start-at", type=float, default=None,
                    help="wall-clock epoch seconds at which replay offset 0 begins. Pass the SAME value to the pickups "
                         "and fares replays: large files take seconds to load, and without a shared start each replay "
                         "would start its clock whenever its own loading finished, misaligning the streams.")
    args = ap.parse_args()

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    records = load_records(Path(args.input), args.delay_field)
    base_event_time = (datetime.fromisoformat(args.base_event_time) if args.base_event_time
                       else min(datetime.fromisoformat(r["event_time"]) for r in records))

    # scheduled replay offset (in compressed wall-clock seconds) for each record, and each record serialized
    # once up front with its closing brace removed; arrival_ts is appended at write time as the last field
    offsets, lines = [], []
    for r in records:
        et = datetime.fromisoformat(r["event_time"])
        true_offset_s = (et - base_event_time).total_seconds()
        offsets.append((true_offset_s + r["_delay_s"]) / args.speed_multiplier)
        clean = {k: v for k, v in r.items() if not k.startswith("_") and k != "arrival_ts"}
        lines.append(json.dumps(clean)[:-1] + ', "arrival_ts": ')
    order = sorted(range(len(records)), key=offsets.__getitem__)
    offsets = [offsets[j] for j in order]
    lines = [lines[j] for j in order]
    del records

    print(f"Replaying {len(lines)} records from {args.input} -> {args.out_dir} "
          f"(speed x{args.speed_multiplier}, flush every {args.flush_interval_seconds}s)")

    if args.start_at is not None:
        wait = args.start_at - time.time()
        if wait > 0:
            time.sleep(wait)
        else:
            print(f"[warn] loading finished {-wait:.1f}s after --start-at; raise the lead time")
        replay_start = args.start_at
    else:
        replay_start = time.time()

    batch_idx = 0
    i = 0
    n = len(offsets)
    while i < n:
        now_offset = time.time() - replay_start
        j = bisect.bisect_right(offsets, now_offset, lo=i)   # records i..j-1 are due (offsets are sorted)
        if j > i:
            write_ts = time.time()   # wall-clock time these records land in the watched dir
            suffix = f"{write_ts}}}\n"
            # write under a hidden name, then rename: Spark skips files starting with '.', and the rename is
            # atomic, so Spark never reads a half-written batch file
            tmp = out / f".part-{batch_idx:06d}.json.tmp"
            with tmp.open("w") as f:
                f.writelines(line + suffix for line in lines[i:j])
            os.replace(tmp, out / f"part-{batch_idx:06d}.json")
            batch_idx += 1
            if i // 5000 != j // 5000:
                print(f"  ... {j}/{n} records replayed at t={now_offset:.1f}s")
            i = j
        time.sleep(args.flush_interval_seconds)

    print(f"Done: {n} records replayed in {time.time() - replay_start:.1f}s wall-clock")


if __name__ == "__main__":
    main()
