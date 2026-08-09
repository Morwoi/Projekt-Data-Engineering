Nachname-Vorname_MATRNR_DataEngineering_P1_S

# Stream Processing Pipeline for Municipal Environmental Sensors

### Konzeptionsphase

Markus Wohlgenannt
Juli 10, 2026

## Objective and problem scope

This portfolio addresses Task 2 of the course Project: Data Engineering
(DLBDSEDE02): designing and implementing a stream processing pipeline for
municipal environmental sensors. The goal is to reliably capture and store
continuously arriving sensor readings so they remain usable for planning
dashboards and a citizen warning app, even during temporary outages of the
data source.

## 1. Data source

The prototype uses the public **Open-Meteo** weather API (no API key
required) as a stand-in for real municipal sensor hardware. For five
fixed stations, the system polls current temperature, humidity, wind
speed, precipitation and pressure every ten seconds, simulating a
near-real-time sensor feed. Each reading is one JSON object with named
numeric fields, representative of a real IoT gateway; new metrics (e.g.
future CO2 or fine-dust sensors) can be added as extra fields without
changing the pipeline. If the API is unreachable, a local fallback
simulator generates a plausible reading, so the stream never depends on
external network availability.

## 2. Goal, usages and success criteria

The system continuously ingests environmental measurements so that (a)
city planners can later analyze trends via dashboards, and (b) a
citizen-facing app can warn residents when values exceed recommended
thresholds. User stories: *"As a planner, I want the latest readings per
district within seconds of measurement"* and *"As a citizen-app backend,
I want to poll new values without missing any, even after a brief
outage."* The system succeeds if readings are never silently lost and
stay available with low latency; it fails if the stream stalls on a
temporary fault or data is dropped between ingestion and storage.

## 3. Technology choice: Kafka + MongoDB

**Kafka** is chosen over Spark Streaming because the workload is a
simple ingest-and-persist pipeline, not one needing in-stream aggregation.
Its durable, replayable log gives reliability (messages persist until
committed), scalability (topic partitioned per station, so more consumers
can join the same group without redesign), and maintainability (one topic,
one JSON schema). The local prototype runs a single broker, so it lacks
fault tolerance against broker loss; a cloud deployment would add brokers
and replication without any application changes. **MongoDB** stores the
readings because its schema-less documents match the JSON messages
directly and absorb new sensor types without migrations. Both run as
single-node Docker containers locally but scale horizontally (Kafka
partitions/brokers, MongoDB sharding/replica sets) in the cloud, with no
code changes.

## 4. Implementation plan

1. Provision Kafka (KRaft, single node) and MongoDB via Docker Compose.
2. Python producer polls Open-Meteo per station, publishes JSON to topic
   `sensor-readings`, with the fallback simulator as a safety net.
3. Python consumer reads the topic and writes each reading into MongoDB,
   committing offsets only after a successful write.
4. Containerize both services and wire them together in
   `docker-compose.yml` so the pipeline starts with one command.
5. Publish the code to a public GitHub repository with setup instructions.
