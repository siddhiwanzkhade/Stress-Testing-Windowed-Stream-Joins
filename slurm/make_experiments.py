"""
Writes the experiment lists for the SLURM scripts: one line of run_sweep.sh arguments per run.
Edit the settings below, then run:  python slurm/make_experiments.py

  experiments/main.txt      inner join, 7 conditions x 5 watermarks x SEEDS        accuracy + compounding
  experiments/disjoint.txt  lateness+burst with no fare hit by both stressors       is the compounding real?
  experiments/policy.txt    watermark policy max instead of min                     whose clock drives eviction
  experiments/window.txt    join-window sweep at a wide watermark                   window vs watermark loss
  experiments/outer.txt     left outer join                                         latency (exclusive nodes!)
  experiments/scale.txt     overlay load levels, for slurm/run_scale.slurm          scale / multi-node
"""
import shlex
from pathlib import Path

# fresh seeds: 1-3 were used in the laptop runs and in choosing the analysis, so they are not reused
SEEDS = range(101, 111)
SCALE_SEEDS = range(201, 204)
WATERMARKS = ["5 minutes", "8 minutes", "10 minutes", "14 minutes", "20 minutes"]

COMMON = ["--join-window", "2 hours", "--num-trips", "3000", "--span-hours", "1", "--speed-multiplier", "60"]
L = ["--lateness-fraction", "0.5", "--lateness-scale", "600"]
B = ["--burst-fraction", "0.3", "--burst-window", "300"]
SK = ["--skew-amplify-factor", "4", "--skew-top-k", "5"]
CONDITIONS = {
    "baseline": [], "lateness_only": L, "burst_only": B, "skew_only": SK,
    "lateness+burst": L + B, "lateness+skew": L + SK, "skew+burst": SK + B,
}


def run(condition, watermark, seed, *extra, common=COMMON):
    return shlex.join(["--condition", condition, "--watermark", watermark, "--seed", str(seed),
                       *common, *CONDITIONS[condition], *extra])


def main():
    lists = {
        "main": [run(c, wm, s) for c in CONDITIONS for wm in WATERMARKS for s in SEEDS],
        # the matching single-stressor runs come from main.txt (the flag changes nothing without both stressors)
        "disjoint": [run("lateness+burst", wm, s, "--disjoint-stressors") for wm in WATERMARKS for s in SEEDS],
        "policy": [run(c, wm, s, "--watermark-policy", "max")
                   for c in ["lateness_only", "burst_only", "lateness+burst"]
                   for wm in ["5 minutes", "10 minutes", "20 minutes"] for s in SEEDS],
        # windows shorter than the longest trip (--max-trip-minutes 60) cut real matches; that loss is the point
        # (the later --join-window overrides the one in COMMON)
        "window": [run(c, "20 minutes", s, "--join-window", jw)
                   for c in ["baseline", "lateness_only", "lateness+burst"]
                   for jw in ["15 minutes", "30 minutes", "45 minutes", "1 hour", "2 hours"] for s in SEEDS],
        "outer": [run(c, wm, s, "--join-type", "leftOuter", "--join-window", "1 hour", "--tail-hours", "2.5")
                  for c in ["baseline", "lateness_only", "burst_only", "lateness+burst"]
                  for wm in ["5 minutes", "10 minutes", "20 minutes"] for s in SEEDS],
    }
    # scale: 2h of event time, all trips kept (huge --num-trips), load raised by overlaying days
    scale_common = ["--join-window", "2 hours", "--num-trips", "100000000", "--span-hours", "2",
                    "--speed-multiplier", "60"]
    lists["scale"] = (
        [run(c, "10 minutes", s, "--overlay-days", str(k), common=scale_common)
         for k in [1, 5, 10, 30] for c in ["lateness_only", "lateness+burst"] for s in SCALE_SEEDS]
        # hot keys across executors: zone-keyed join, where one busy zone can become a straggler partition
        + [run("skew_only", "10 minutes", s, "--overlay-days", str(k), "--join-key", "zone",
               "--skew-amplify-factor", "5", common=scale_common)
           for k in [1, 10, 30] for s in SCALE_SEEDS]
    )

    out = Path(__file__).resolve().parent.parent / "experiments"
    out.mkdir(exist_ok=True)
    for name, lines in lists.items():
        (out / f"{name}.txt").write_text("\n".join(lines) + "\n")
        print(f"experiments/{name}.txt: {len(lines)} runs")


if __name__ == "__main__":
    main()
