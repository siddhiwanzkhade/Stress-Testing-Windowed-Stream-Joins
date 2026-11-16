"""
Merges per-task result CSVs (results/array/<experiment>/task_*.csv) into one CSV with a union header, and
lists tasks that produced no result (failed runs) together with the command to resubmit only those.

  python analysis/merge_results.py results/array/main -o results/seawulf_main.csv --expected 350
  # compounding check with the control: single-stressor runs from main, the combination from disjoint
  python analysis/merge_results.py results/array/main results/array/disjoint -o results/seawulf_disjoint.csv \
      --exclude experiment=main,condition=lateness+burst
"""
import argparse
import csv
import re
from pathlib import Path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("inputs", nargs="+", help="CSV files or folders of CSVs")
    ap.add_argument("-o", "--output", required=True)
    ap.add_argument("--exclude", action="append", default=[], metavar="K=V,K=V",
                    help="drop rows matching ALL of these column values (repeatable)")
    ap.add_argument("--expected", type=int, default=None,
                    help="number of lines in the experiment list; reports which task numbers are missing")
    args = ap.parse_args()

    files = []
    for x in map(Path, args.inputs):
        files += sorted(x.glob("*.csv")) if x.is_dir() else [x]
    excludes = [dict(kv.split("=", 1) for kv in e.split(",")) for e in args.exclude]

    fields, rows = [], []
    for f in files:
        with f.open(newline="") as fh:
            r = csv.DictReader(fh)
            fields += [k for k in (r.fieldnames or []) if k not in fields]
            for row in r:
                if not any(all(row.get(k) == v for k, v in ex.items()) for ex in excludes):
                    rows.append(row)

    out = Path(args.output); out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)
    print(f"{len(rows)} runs from {len(files)} files -> {out}")

    if args.expected:
        done = {int(m.group(1)) for f in files if (m := re.match(r"task_(\d+)\.csv$", f.name))}
        missing = sorted(set(range(1, args.expected + 1)) - done)
        if missing:
            print(f"{len(missing)} tasks have no result: {missing}")
            print(f"resubmit: sbatch --array={','.join(map(str, missing))} slurm/run_array.slurm experiments/<name>.txt")
        else:
            print("all tasks present")


if __name__ == "__main__":
    main()
