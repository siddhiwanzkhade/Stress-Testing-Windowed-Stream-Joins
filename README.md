# Stress-Testing-Windowed-Stream-Joins

Measuring how join completeness and latency degrade in Apache Spark Structured Streaming when late arrivals, traffic bursts, and key skew occur together, using Apache Kafka and real order and payment data.

**Stream Processing | Apache Kafka | Spark Structured Streaming**

*Accuracy-versus-Latency Trade-offs in Windowed Stream Joins Under Combined Real-World Imperfections*

Big Data Systems, Algorithms and Networks (Fall 2026), Stony Brook University

## Overview

A streaming system that joins two live event streams, here e-commerce orders and their payment confirmations, must decide how long to wait for a late match before giving up. That waiting time is the **watermark**. Waiting longer catches more matches but increases latency and memory use. Waiting less is faster but silently drops late matches.

Real streams rarely suffer one problem at a time. Late arrivals, traffic bursts, and skewed keys usually occur together, yet most prior work tests them separately. This project measures how join completeness and latency degrade under each problem alone and in combination, and checks whether the combined damage adds up or compounds.

## Research questions

1. How much completeness is lost under each problem as the window tightens?
2. Is the damage from two problems together larger or smaller than the individual damages predict?
3. Which problem is the most damaging per second of window tightening?

## Approach

- Real order and payment timestamps form the base of the event streams.
- Controlled lateness, bursts, and skew are injected independently, so each can be switched on alone or in combination.
- Because the injection is controlled, the exact set of true matches is known, which allows completeness to be measured against ground truth.
- Each condition is run across a range of window sizes and several random seeds.

## Architecture

Clean data files, then a producer script that replays events and injects the problems, then an Apache Kafka broker with an orders topic and a payments topic, then a windowed join in Apache Spark Structured Streaming with a configurable watermark, then a metrics collector.

## Metrics

| Metric | Meaning |
|---|---|
| Completeness | Matches emitted divided by true matches |
| Latency | Delay between a match becoming possible and being emitted |
| State size | Data held by the join while waiting for matches |

## Experiment conditions

- Baseline with no problems
- Each problem alone: lateness, bursts, skew
- Each pair: lateness with skew, lateness with bursts, skew with bursts

## Repository structure

- `data/` cleaned datasets and ground truth
- `producer/` replay script and problem generators
- `streaming/` Spark
