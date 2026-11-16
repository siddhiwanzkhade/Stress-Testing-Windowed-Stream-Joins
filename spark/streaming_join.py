"""
Windowed join of the pickups and fares streams, with watermark delay and
join window treated as two INDEPENDENT parameters -- this is a direct
response to reviewer feedback: earlier drafts described a single "window
tightening" knob, which conflated two different things.

  --watermark    : how long Structured Streaming waits, past the latest
                   event-time it has seen, before evicting buffered state.
                   Set via .withWatermark() on EACH stream separately.
                   This is what determines whether a late-arriving fare
                   event still has a chance to match.

  --join-window  : the business-logic bound on the match itself -- the
                   maximum allowed pickup-to-fare-confirmation interval.
                   Set via the join condition's time-range predicate.
                   This has nothing to do with lateness tolerance; it's
                   "how long can this trip legitimately take."

They are swept independently in the experiment matrix (E1 varies watermark
holding join_window fixed; a separate sweep varies join_window holding
watermark fixed) specifically so any observed completeness loss can be
attributed to the parameter that actually caused it.

IMPORTANT INTERACTION (documented, not conflated): Spark still needs
retained state for at least join_window + watermark to guarantee a match
isn't evicted before it's even eligible -- so join_window indirectly
affects how much state-store memory a given watermark costs. That's a real
interaction between the two, not a merging of them into one variable; the
experiment log below records state-store size precisely so this
interaction is visible in the results, not hidden by it.
"""
import argparse
import json
import re
import shutil
from pathlib import Path

import pyspark
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.streaming import StreamingQueryListener

PICKUP_SCHEMA = "trip_id STRING, event_time TIMESTAMP, PULocationID INT, arrival_ts DOUBLE"
FARE_SCHEMA = (
    "trip_id STRING, event_time TIMESTAMP, arrival_delay_seconds DOUBLE, "
    "fare_amount DOUBLE, payment_type INT, PULocationID INT, arrival_ts DOUBLE"
)

REPO = Path(__file__).resolve().parent.parent
JARS_DIR = REPO / "jars"          # Kafka connector jars, filled once by --fetch-kafka-jars


def kafka_package() -> str:
    # pip's pyspark 4.x is built for Scala 2.13, 3.x for Scala 2.12; the connector must match both
    scala = "2.13" if int(pyspark.__version__.split(".")[0]) >= 4 else "2.12"
    return f"org.apache.spark:spark-sql-kafka-0-10_{scala}:{pyspark.__version__}"


def read_stream(spark, args, schema: str, folder: str, topic: str):
    if args.source == "file":
        return spark.readStream.schema(schema).json(folder)
    reader = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", args.kafka_bootstrap)
              .option("subscribe", topic)
              .option("startingOffsets", "earliest"))
    if args.max_offsets_per_trigger:
        reader = reader.option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
    # same JSON payload as file mode, so everything downstream of this line is identical in both modes
    return reader.load().select(F.from_json(F.col("value").cast("string"), schema).alias("r")).select("r.*")


class MetricsListener(StreamingQueryListener):
    def __init__(self, out_path: str):
        self.out_path = out_path

    def onQueryStarted(self, event):
        pass

    def onQueryProgress(self, event):
        p = event.progress
        record = {
            "timestamp": p.timestamp,
            "batchId": p.batchId,
            "numInputRows": p.numInputRows,
            "eventTime": dict(p.eventTime) if p.eventTime else {},
            "durationMs": dict(p.durationMs),
            "stateOperators": [
                {"numRowsTotal": s.numRowsTotal, "numRowsUpdated": s.numRowsUpdated,
                 "memoryUsedBytes": s.memoryUsedBytes}
                for s in p.stateOperators
            ],
        }
        with open(self.out_path, "a") as f:
            f.write(json.dumps(record) + "\n")

    def onQueryTerminated(self, event):
        pass


def build_join(pickups, fares, watermark: str, join_window: str, join_key: str = "trip_id", join_type: str = "inner"):
    pickups_wm = pickups.withWatermark("event_time", watermark)
    fares_wm = fares.withWatermark("event_time", watermark)

    if join_key == "zone":
        # EXPERIMENT: partition the join by pickup zone (a NON-unique key) so busy zones build big buckets.
        # The real trip pairing is still enforced, but written as (hash(a) - hash(b)) == 0, an expression that
        # mixes both sides, so Spark cannot promote it to a second partitioning key. (Tried and rejected:
        # trip_id >= / <= pairs break Spark's state-cleanup analysis; a post-join filter gets pushed back
        # into the join as a key.) xxhash64 collisions are negligible at this scale.
        key_cond = ((pickups_wm["PULocationID"] == fares_wm["PULocationID"]) &
                    ((F.xxhash64(pickups_wm["trip_id"]).cast("decimal(21,0)") - F.xxhash64(fares_wm["trip_id"]).cast("decimal(21,0)")) == 0))
    else:
        key_cond = (pickups_wm["trip_id"] == fares_wm["trip_id"])

    joined = pickups_wm.join(
        fares_wm,
        key_cond &
        (fares_wm["event_time"] >= pickups_wm["event_time"]) &
        (fares_wm["event_time"] <= pickups_wm["event_time"] + F.expr(f"INTERVAL {join_window}")),
        join_type,   # "inner" or "leftOuter": leftOuter also emits pickups with no fare, once the watermark rules one out
    )

    return joined.select(
        pickups_wm["trip_id"],
        pickups_wm["event_time"].alias("pickup_time"),
        fares_wm["event_time"].alias("fare_time"),
        fares_wm["fare_amount"],
        (F.unix_timestamp(fares_wm["event_time"]) - F.unix_timestamp(pickups_wm["event_time"])).alias("trip_seconds"),
        pickups_wm["arrival_ts"].alias("pickup_arrival_ts"),
        fares_wm["arrival_ts"].alias("fare_arrival_ts"),
        # wall-clock moment Spark emitted this match (evaluated per micro-batch)
        (F.unix_millis(F.current_timestamp()) / 1000.0).alias("emit_ts"),
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pickups-dir", default="./stream/pickups")
    ap.add_argument("--fares-dir", default="./stream/fares")
    ap.add_argument("--watermark", required=True, help="e.g. '10 minutes' -- independent of --join-window")
    ap.add_argument("--join-window", required=True, help="e.g. '40 minutes' -- max valid pickup-to-fare interval")
    ap.add_argument("--join-key", choices=["trip_id", "zone"], default="trip_id")
    ap.add_argument("--join-type", choices=["inner", "leftOuter"], default="inner")
    ap.add_argument("--static-test", action="store_true",
                    help="process all files already present, print the physical plan, then exit")
    ap.add_argument("--output-path", default="./output/matches")
    ap.add_argument("--checkpoint-dir", default="./chk")
    ap.add_argument("--metrics-out", default="./output/metrics.jsonl")
    ap.add_argument("--trigger-seconds", type=int, default=3)
    ap.add_argument("--source", choices=["file", "kafka"], default="file",
                    help="file = watched folders (the original setup, kept as the control); kafka = topics")
    ap.add_argument("--kafka-bootstrap", default="localhost:9092", help="host:port of the Kafka broker")
    ap.add_argument("--pickups-topic", default="pickups")
    ap.add_argument("--fares-topic", default="fares")
    ap.add_argument("--max-offsets-per-trigger", type=int, default=None,
                    help="cap on records read per micro-batch (Kafka only); default: no cap")
    ap.add_argument("--master", default=None, help="e.g. spark://<node>:7077 for a multi-node run; omit for local[*]")
    ap.add_argument("--shuffle-partitions", type=int, default=8, help="about 2-3x total cores; 8 matches the original runs")
    ap.add_argument("--driver-memory", default=None, help="e.g. 16g; the default (1g) is too small for large join state")
    ap.add_argument("--state-store", choices=["default", "rocksdb"], default="default")
    ap.add_argument("--watermark-policy", choices=["min", "max"], default="min",
                    help="how the two streams' watermarks combine into the one that drives eviction")
    ap.add_argument("--spark-conf", action="append", default=[], metavar="KEY=VALUE",
                    help="any extra Spark setting, repeatable (e.g. spark.executor.memory=32g)")
    ap.add_argument("--fetch-kafka-jars", action="store_true",
                    help="one-time setup (needs internet): download the Kafka connector jars into ./jars and exit")
    args = ap.parse_args()

    builder = (
        SparkSession.builder.appName("taxi-pickup-fare-join")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.sql.streaming.multipleWatermarkPolicy", args.watermark_policy)
    )
    if args.master:
        builder = builder.master(args.master)
    if args.driver_memory:
        builder = builder.config("spark.driver.memory", args.driver_memory)
    if args.state_store == "rocksdb":
        builder = builder.config("spark.sql.streaming.stateStore.providerClass",
                                 "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
    local_jars = sorted(JARS_DIR.glob("*.jar"))
    if args.fetch_kafka_jars:
        builder = (builder.config("spark.jars.packages", kafka_package())
                   .config("spark.jars.ivy", str(REPO / ".ivy")))
    elif args.source == "kafka":
        # local jars need no internet (SeaWulf compute nodes); fall back to a download if they are missing
        builder = (builder.config("spark.jars", ",".join(str(j) for j in local_jars)) if local_jars
                   else builder.config("spark.jars.packages", kafka_package()))
    for kv in args.spark_conf:
        key, _, value = kv.partition("=")
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    if args.fetch_kafka_jars:
        # keep only jars Spark does not already ship (hadoop, lz4, snappy, slf4j would be duplicates)
        def artifact(name: str) -> str:
            return re.sub(r"-\d[\w.\-]*\.jar$", "", name)
        shipped = {artifact(j.name) for j in (Path(pyspark.__file__).parent / "jars").glob("*.jar")}
        JARS_DIR.mkdir(exist_ok=True)
        for jar in (REPO / ".ivy" / "jars").glob("*.jar"):
            if artifact(jar.name.split("_", 1)[-1]) not in shipped:      # ivy names are <group>_<artifact>-<ver>
                shutil.copy2(jar, JARS_DIR / jar.name)
        print(f"Kafka connector jars for Spark {pyspark.__version__} -> {JARS_DIR}: "
              f"{sorted(j.name for j in JARS_DIR.glob('*.jar'))}")
        spark.stop()
        return

    spark.streams.addListener(MetricsListener(args.metrics_out))

    pickups = read_stream(spark, args, PICKUP_SCHEMA, args.pickups_dir, args.pickups_topic)
    fares = read_stream(spark, args, FARE_SCHEMA, args.fares_dir, args.fares_topic)

    result = build_join(pickups, fares, args.watermark, args.join_window, args.join_key, args.join_type)

    query = (
        result.writeStream
        .format("json")
        .option("path", args.output_path)
        .option("checkpointLocation", args.checkpoint_dir)
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .outputMode("append")
        .start()
    )
    if args.static_test:
        query.processAllAvailable()
        query.explain()
        query.stop()
        import time; time.sleep(3)   # let the metrics listener flush its last events
        return
    query.awaitTermination()

    


if __name__ == "__main__":
    main()
