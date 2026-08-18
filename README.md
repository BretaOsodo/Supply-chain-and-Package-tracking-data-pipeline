# Supply-chain-and-Package-tracking-data-pipeline

# Introduction & Goals

Every package that moves through a supply chain generates a trail of scans which are picked up at the warehouse, sorted at a hub, loaded onto a truck, dropped at the door. This project simulates that trail and builds the pipeline to turn it into live, trustworthy tracking: synthetic scan events from four scanner types (warehouse, hub, driver, delivery) are streamed through Kafka, validated and reconstructed into per-package state with Spark Structured Streaming and served to a tracking API backed by DynamoDB and Redis — while a separate path feeds ETA prediction, delay detection and analytics.

The interesting engineering problem isn't the happy path but it's that scanners go offline. A driver's handheld scanner in a dead zone doesn't lose its scan, it buffers it and uploads late, with the *original* scan time attached. That means events arrive out of order and the pipeline has to reconstruct the correct sequence using event-time processing rather than trusting the order things showed up in.
 

 
**Goal 1:** A package's tracked status should reflect reality even when the scan that produced it arrived late or out of order.
**How I know it worked:** State updates from a validated scan appear in the tracking API within 2 minutes of that scan being processed, regardless of how late it arrived relative to when it actually happened.
 
**Goal 2:** Bad data should never corrupt a package's tracked state.
**How I know it worked:** 100% of malformed events (bad `package_id` format, null `location_id`, unrecognized `scan_type`) are routed to a dead-letter path and never reach the state store this is verified by injecting a known percentage of corrupted events at the generator and confirming none leak through.
 
**Goal 3:** Delays should be caught before a customer would notice.
**How I know it worked:** A package that misses its expected scan window triggers a delay alert within 30 minutes of the threshold being breached.
 
## Architecture
![Architecture](image.png)
 
The pipeline has four stages: **ingestion** (scanners → Kafka, partitioned by `package_id` so one package's events always land in order), **validation** (a stateless Spark job splitting clean events from bad ones), **state processing** (a stateful Spark job reconstructing each package's true state using event-time watermarking), and **fan-out** (the validated state stream feeding a customer-facing read path plus independent ETA, delay-detection, and analytics consumers)

# Contents

- [The Data set](#the-data-set)
- [Constraints](#constraints)
- [Used Tools](#used-tools)
    - [Connect](#connect)
    - [Buffer](#Buffer)
    - [Processing](#processing)
    - [Storage](#storage)
    - [Visualization](#visualization)
- [Pipeline](#pipeline)
    - [Stream Processing](#stream-processing)

# The Data Set

The data is synthetic ,generated rather than pulled from a real carrier, because no public dataset gives you raw scanner-level events with realistic offline/dead-zone behavior baked in. A generator (  `data_generator`/`generated_data.py` ) models four scanner types each responsible for specific transitions in the package state machine (`ordered → picked → shipped → in_transit → out_for_delivery → delivered`) and each with its own probability of "going offline" and sending its scan late.
 
What I like about it: full control. I can dial the out-of-order rate up or down, inject a known percentage of malformed records and know exactly what the "correct" reconstructed state should be for every package — which makes it possible to actually verify the pipeline is doing the right thing, not just that it runs without crashing.
 
What's problematic: it's synthetic. Real scanner data would have correlated failures (a whole depot losing connectivity at once, not independent random scanners), clock drift between devices and messier location data than a clean list of hub codes. The pipeline is designed to tolerate that kind of mess but the generator doesn't currently produce it.

# Constraints

- **Budget:** $0. Everything runs locally on Docker Desktop — no cloud spend, no managed services.
- **Compute:** a single laptop running the whole stack — 2 Kafka brokers, Zookeeper-free KRaft mode, a Spark master + 2 workers (2 cores / 1GB RAM each), Postgres, Redis, Prometheus/Grafana/cAdvisor for monitoring, and pgAdmin, all as Docker containers on one machine.
- **Data I don't control:** none, technically — the data is self-generated. But that's itself a constraint: I can't validate the pipeline against the messiness of real-world scanner behavior (clock drift, correlated outages), only what I've thought to simulate.
- **Time:** self-directed, worked on incrementally rather than against a deadline — which shows in how many of the "what breaks" items below are still open rather than fixed.
"No budget, so everything runs on a laptop" explains a lot of the smaller decisions in this project — 2 Kafka brokers instead of 3, low worker memory, Docker Compose instead of Kubernetes — more than any of them being a deliberate architectural choice.

# Used Tools

## Connect 
A Python scanner simulator (`data_generator/generated_data.py`) generates synthetic scan events for four scanner types — warehouse, hub, driver, delivery — each modeling realistic connectivity: driver scanners have a much higher chance of "going offline" and sending a scan late than a fixed warehouse scanner does. Late events keep their true event-time timestamp, which is what makes it possible to actually test the pipeline's out-of-order handling rather than just assuming it works.

## Buffer 

**Apache Kafka**, 2 brokers in KRaft mode (no Zookeeper), via Confluent's images. Topics are partitioned by `package_id` so every event for one package — regardless of which scanner or device produced it — lands on the same partition in event-time order. That single guarantee is what the rest of the pipeline depends on.
 
Current topics:
- `scan-events` — raw events from the simulator
- `scan-events-validated` / `scan-events-dlq` — output of the validation job
- (planned) `package-state-changes` — output of the state-processing job, once built
Schema Registry and Kafdrop run alongside for schema management and a browsable UI.

## Processing 

**Spark Structured Streaming**, one master + two workers in Docker. The validation job is built and running: it reads `scan-events` once per micro-batch, checks each event against six rules (package ID format, scan type, location ID, timestamp, device ID, device prefix), and fans the same batch out to the validated topic, the DLQ, and S3 — all from a single `foreachBatch` callback rather than three separate streaming queries, which would otherwise triple the Kafka read load. See [`sources/jobs/validation_job.py`](sources/jobs/validation_job.py) and its own [README](sources/jobs/README.md) for the details, including the real bugs I hit getting it running (a Spark 3.x timestamp-parsing quirk, and mixing up Kafka's internal vs. external listener ports from inside a container).
 
The state-processing job — the stateful one, using `flatMapGroupsWithState` with event-time watermarking to reconstruct each package's true state despite out-of-order arrival — is not built yet.