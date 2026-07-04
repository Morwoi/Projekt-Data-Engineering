# Municipal Environmental Sensor Streaming Pipeline

Course: DLBDSEDE02 - Project: Data Engineering | Task 2: Design and implement a stream processing pipeline

## System description

This project simulates a municipal environmental sensor network. Five
fictional sensor stations placed across the city continuously report
environmental measurements (temperature, humidity, wind speed,
precipitation, air pressure). A **Kafka producer** polls the public
[Open-Meteo](https://open-meteo.com) weather API once every 10 seconds per
station and publishes each reading as a JSON message to the Kafka topic
`sensor-readings`. A **Kafka consumer** reads the stream and persists every
reading into **MongoDB**, where it becomes available to downstream
applications such as planner dashboards or a citizen warning app.

**Reliability / backup plan:** if the public API is temporarily
unreachable (network outage, timeout, rate limiting), the producer
automatically switches to a local fallback simulator that generates a
plausible reading from the last known-good value, tags it
`"source": "simulated"`, and keeps the stream running without interruption.
This satisfies the requirement that the system "does not break if the data
is temporarily inaccessible" and also means the whole pipeline can be
demonstrated end-to-end without any internet access at all (e.g. during
grading/testing).

On the storage side, the consumer commits Kafka offsets only after a
successful MongoDB write (at-least-once delivery) and retries transient
MongoDB errors with backoff before giving up on a single message, so a
short database restart does not crash the consumer or silently drop data.

### Expected usages / end-user context

- **City planners** query MongoDB (directly or through a BI/dashboard tool)
  to analyze environmental trends over time and prioritize interventions.
- **A citizen-facing warning app** (not part of this prototype) would read
  the latest readings per station from MongoDB and push alerts when values
  exceed recommended thresholds.
- **Future sensor types** (CO2, noise, fine dust) can be added by extending
  the `metrics` fields in a reading without changing the pipeline
  architecture, since Kafka messages and MongoDB documents are both
  schema-flexible.

## Architecture

```
[Open-Meteo API]        [fallback simulator]
        \                     /
         v                   v
        +---------------------+
        |   Kafka Producer     |
        +---------------------+
                   |
                   v
        +---------------------+
        | Kafka topic:          |
        | sensor-readings       |
        +---------------------+
                   |
                   v
        +---------------------+
        |   Kafka Consumer     |
        +---------------------+
                   |
                   v
        +---------------------+
        |   MongoDB            |
        | environment_monitoring.sensor_readings |
        +---------------------+
```

Kafka runs in single-node KRaft mode (no ZooKeeper dependency), which keeps
the local prototype simple while using the same broker software and wire
protocol used in distributed, multi-broker production deployments -- moving
to a cloud-hosted, multi-node Kafka cluster later requires only a
configuration change, not a rewrite.

## How to run

Requirements: Docker Desktop (or Docker Engine + Compose plugin).

```bash
git clone <this-repository-url>
cd <repository-folder>
docker compose up --build
```

This builds the producer and consumer images, starts Kafka and MongoDB,
waits for both to report healthy, and then starts streaming simulated
sensor readings into MongoDB automatically -- no manual setup steps
required.

To inspect the stored data:

```bash
docker exec -it mongo mongosh environment_monitoring --eval "db.sensor_readings.find().sort({timestamp:-1}).limit(5).pretty()"
```

To stop everything:

```bash
docker compose down
```

Add `-v` to also remove the persisted Kafka/MongoDB volumes.

## Repository layout

```
docker-compose.yml      Orchestrates Kafka, MongoDB, producer, consumer
producer/                Kafka producer: fetches Open-Meteo data or falls back to simulator
consumer/                Kafka consumer: writes readings into MongoDB with retry
docs/                    Portfolio submission texts (Phase 1 concept, Phase 2 explanation)
```

## Configuration

All settings are environment variables with sensible defaults (see
`docker-compose.yml`), e.g. `FETCH_INTERVAL_SECONDS`, `KAFKA_TOPIC`,
`MONGO_DB`. No API key is required for Open-Meteo.
