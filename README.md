# Stress-Testing-Windowed-Stream-Joins

Measuring how join completeness and latency degrade in Apache Spark Structured Streaming when late arrivals, traffic bursts, and key skew occur together, using Apache Kafka and real NYC taxi data.

**Stream Processing | Apache Kafka | Spark Structured Streaming | SLURM / HPC**

*Accuracy-versus-Latency Trade-offs in Windowed Stream Joins Under Combined Real-World Imperfections*

## Overview

A streaming system that joins two live event streams, here **taxi pickups** and their **fare records**, must decide how long to wait for a late match before giving up. That waiting time is the **watermark**. Waiting longer catches more matches but increases latency and memory use. Waiting less is faster but silently drops late matches.

Real streams rarely suffer one problem at a time. Late arrivals, traffic bursts, and skewed keys usually occur together, yet most prior work tests them separately. This project measures how join completeness and latency degrade under each problem alone and in combination, and checks whether the combined damage adds up or compounds.

## Research questions

1. How much data is lost under each problem as the watermark tightens?
2. Do two problems together cause more loss than each alone predicts?
3. Which problem hurts most per second of watermark tightening?

## Approach

- Real NYC taxi trips (January 2026) are split into a **pickup** and a **fare** event, matched on trip ID.
- Since every trip has both events, we know exactly which pairs should match (the **ground truth**) and can count what Spark missed.
- Each condition runs at **5 watermarks (5, 8, 10, 14, 20 min)** with **10 seeds**.
- A **disjoint control** makes sure no fare is hit by both problems, to test *why* compounding happens.

## Stressors

| Stressor | What happens | Setting |
|---|---|---|
| **Lateness** | Some fares arrive late by a random delay | 50% of fares, average delay 10 min |
| **Burst** | An outage holds fares back, then releases them all at once | Outage covering 30% of the time, released within 5 min |
| **Skew** | The busiest pickup zones get extra traffic | Top 5 zones, 4× traffic |

**7 conditions:** baseline, each stressor alone, and each pair (lateness+burst, lateness+skew, skew+burst).
**Compounding** = loss(both) − [loss(A) + loss(B)]. A positive value means the two make each other worse.

## Joins

| Join | Key | Join window | Measures |
|---|---|---|---|
| **Inner** | trip ID | 2 hours | Accuracy and compounding (main experiment) |
| **Left outer** | trip ID | 1 hour | Latency, including how long "no fare" answers take |
| **Zone-keyed inner** | pickup zone | 2 hours | Cost of skew: busy zones become hot keys |

All joins: a fare must come after its pickup and within the join window; watermark policy `min` (the slower stream's clock); a new micro-batch every 3 seconds.

## Architecture

Taxi data → stream generator (adds the problems) → **Kafka** (pickups and fares topics) → **Spark Structured Streaming** join with a watermark → scorer (compares with ground truth).

Runs on Stony Brook's **SeaWulf** cluster with SLURM: **859 runs, ~18 million trips**, including multi-node Spark clusters (1, 2, 4 workers) at up to 30× normal traffic.

## Key design decisions

- **Separate watermark and join window:** shows which one actually causes the loss.
- **Ground truth:** every trip has a known pickup–fare pair, so missed matches are counted exactly.
- **Independent stressors:** each one can be switched on alone or in pairs.
- **Disjoint control:**  normally a fare can be both late and caught in a burst. In this control, no fare gets both. If the extra loss disappears, compounding is caused by fares hit by both problems.
- **10 seeds per setting:** every result is repeated on 10 different random datasets.
- **Seeded data per run:** each run builds its own data, so runs are independent and exactly repeatable.
- **Fresh Kafka topics per run:** no data leaks between runs.
- **Outer join for latency:** an inner join can't time trips that never match.
- **Overlay days:** raises traffic up to 30× while keeping it real taxi data.
- **Multi-node Spark on SLURM:** tests scaling on 1, 2 and 4 worker nodes.
- **Feeder job:** keeps the queue under SeaWulf's 100-job limit.


## Metrics

| Metric | Meaning |
|---|---|
| Completeness | Matches found ÷ true matches |
| Latency | Time from a match being possible to being output |
| State size | Data Spark holds while waiting for matches |

## Findings

**Loss (% of true matches missed), inner join, mean of 10 seeds**

| Condition | 5 min | 8 min | 10 min | 14 min | 20 min |
|---|---|---|---|---|---|
| Lateness only | 16.1 | 11.1 | 8.6 | 5.2 | 2.3 |
| Burst only | 2.9 | 1.6 | 1.9 | 1.1 | 0 |
| Lateness + burst | 22.7 | 17.3 | 14.7 | 9.6 | 4.4 |
| **Compounding** (points) | **+3.8** | **+4.6** | **+4.3** | **+3.3** | **+2.1** |

Baseline and skew only: 0% at every watermark.

- **Compounding is real.** Late fares and bursts together lose **3.6 points more** than adding them up. It shows up at every watermark, across 10 seeds, with **p < 0.0001**. 
- **We know why (Disjoint Control).** When no fare is hit by both problems, the extra loss disappears. Fares that are both late *and* stuck in a burst get both delays, which pushes them past the watermark.
- **Lateness hurts most:** about **5× more** than bursts per second of watermark tightening. Skew loses nothing on a unique trip ID.
- **Waiting is a trade-off, and accuracy wins.** A longer watermark cuts loss from **16% to 2%**, while matches still come out in about **1.5 s**. The only cost: "no fare" answers come about 15 minutes later (**70 → 85 min**).
- **Policy matters.** Following the faster stream (`max`) makes bursts **6× worse** than the default (`min`).
- **Scaling works, except for hot keys.** The trip-ID join handled **30× traffic** even on one node. A skewed zone join fell behind on every cluster size, because one busy key always lands on one task, so more machines don't help.

## Repository structure

| File | What it does |
|---|---|
| `data/generate_taxi_streams.py` | Turns real trips into pickup and fare streams, adds the stressors, and writes the ground truth |
| `data/kafka_replay.py` | Sends the events into Kafka at their real arrival times |
| `data/kafka_topics.py` | Creates and deletes each run's Kafka topics |
| `spark/streaming_join.py` | The Spark Structured Streaming join (inner, left outer or zone-keyed) with the watermark |
| `run_sweep.sh` | Runs one full experiment: generate → stream → join → score |
| `analysis/score_run.py` | Compares Spark's output with the ground truth: loss and latency |
| `analysis/merge_results.py` | Combines per-run results into one file and reports missing runs |
| `analysis/compounding_report.py` | Compounding table and which stressor hurts most |
| `analysis/pooled_interaction.py` | Statistical test (p-value) for lateness + burst compounding |
| `scripts/kafka_start.sh`, `kafka_stop.sh` | Start and stop a Kafka broker for a run |
| `slurm/make_experiments.py` | Writes the list of all 859 experiments |
| `slurm/run_array.slurm` | Runs the small experiments as a SLURM job array |
| `slurm/run_scale.slurm` | Builds a multi-node Spark cluster and runs the scale tests |
| `slurm/feeder.sh` | Submits everything while staying under SeaWulf's 100-job limit |


## Running it

```bash
git clone https://github.com/siddhiwanzkhade/Stress-Testing-Windowed-Stream-Joins.git
cd Stress-Testing-Windowed-Stream-Joins
pip install -r requirements.txt
```

**See our results:** they're already in `results/` (`new_*_results.csv`). To recompute the compounding analysis from them:
```bash
python analysis/compounding_report.py --results-log results/new_main_results.csv
python analysis/pooled_interaction.py results/new_main_results.csv
```

**Rerun all 859 experiments** (needs a SLURM cluster, Java 17/21, Python 3.11, and the January 2026 Yellow Taxi parquet from NYC TLC in `data/raw/`): follow the setup at the top of `requirements.txt`, then:
```bash
python slurm/make_experiments.py
sbatch slurm/feeder.sh
```

*Big Data Systems, Algorithms and Networks (Fall 2026), Stony Brook University*


