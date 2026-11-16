"""
Small Kafka admin helper used by run_sweep.sh and the SLURM scripts.

  python data/kafka_topics.py --bootstrap host:port ready              # wait until the broker answers
  python data/kafka_topics.py --bootstrap host:port create T1 T2 --partitions 8
  python data/kafka_topics.py --bootstrap host:port delete T1 T2
"""
import argparse
import sys
import time

from confluent_kafka.admin import AdminClient, NewTopic


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", required=True)
    ap.add_argument("action", choices=["ready", "create", "delete"])
    ap.add_argument("topics", nargs="*")
    ap.add_argument("--partitions", type=int, default=8)
    ap.add_argument("--timeout", type=float, default=60.0)
    args = ap.parse_args()

    # log_level 0 silences librdkafka's "connection refused" lines while 'ready' polls a broker still booting
    admin = AdminClient({"bootstrap.servers": args.bootstrap, "log_level": 0 if args.action == "ready" else 3})

    if args.action == "ready":
        deadline = time.time() + args.timeout
        while time.time() < deadline:
            try:
                admin.list_topics(timeout=5)
                print(f"Kafka at {args.bootstrap} is ready")
                return
            except Exception:
                time.sleep(1)
        sys.exit(f"Kafka at {args.bootstrap} did not answer within {args.timeout:.0f}s")

    if args.action == "create":
        futures = admin.create_topics([NewTopic(t, num_partitions=args.partitions, replication_factor=1)
                                       for t in args.topics])
    else:
        futures = admin.delete_topics(args.topics)
    for topic, f in futures.items():
        f.result(timeout=args.timeout)      # raises on failure
        print(f"{args.action}d topic {topic}")


if __name__ == "__main__":
    main()
