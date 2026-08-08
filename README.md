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

# The Data Set

The data is synthetic ,generated rather than pulled from a real carrier, because no public dataset gives you raw scanner-level events with realistic offline/dead-zone behavior baked in. A generator (  `data_generator`/`generated_data.py` ) models four scanner types each responsible for specific transitions in the package state machine (`ordered → picked → shipped → in_transit → out_for_delivery → delivered`) and each with its own probability of "going offline" and sending its scan late.
 
What I like about it: full control. I can dial the out-of-order rate up or down, inject a known percentage of malformed records and know exactly what the "correct" reconstructed state should be for every package — which makes it possible to actually verify the pipeline is doing the right thing, not just that it runs without crashing.
 
What's problematic: it's synthetic. Real scanner data would have correlated failures (a whole depot losing connectivity at once, not independent random scanners), clock drift between devices and messier location data than a clean list of hub codes. The pipeline is designed to tolerate that kind of mess but the generator doesn't currently produce it.


# Constraints