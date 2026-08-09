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
MongoDB errors with backoff before giving up on a single message. Writes
are upserts keyed on `(station_id, timestamp)`, so a redelivered message
(possible under at-least-once delivery) updates the existing document
instead of creating a duplicate. If all retries on a message are
exhausted, it is parked in a `sensor_readings_dead_letters` collection
instead of being dropped, so a short database restart never silently
loses data.

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

### Addressing tutor feedback: two user types need different guarantees

Kafka's delivery reliability (nothing lost between producer and MongoDB)
and what each end user actually needs are not the same thing:

- **Planners** don't want raw 10-second samples -- `planner_queries.py`
  rolls readings up into daily per-station statistics instead, and
  `materialize_daily_stats.py` persists that rollup into
  `daily_station_stats` so history survives past the 90-day TTL on raw
  readings (see "Retention" below).
- **A citizen deciding whether to go outside** needs to know the *current*
  reading is trustworthy, not just that some reading was stored. The
  producer's fallback simulator keeps publishing fresh-looking synthetic
  data during an API outage (by design, so the pipeline itself never
  stalls) -- which means "message age" alone would report a broken sensor
  as fine. `citizen_status.py` tracks the age of the last *real*
  (`source="api"`) reading separately and classifies each station as
  `ok` / `stale` / `unavailable` accordingly; see the module docstring for
  the full reasoning. `verify_citizen_failover.py` forces a real outage
  against the running stack and checks this behaviour actually happens,
  rather than only asserting it.

**Verified run** (2026-08-09, against the live stack, station-01):

```
Baseline: OK  Reading reflects current conditions.

Forcing an Open-Meteo outage (API_TIMEOUT_SECONDS=0.001, producer recreated)...
[07:11:48] (outage) station-01: -> OK  Reading reflects current conditions.
[07:12:28] (outage) station-01: -> UNAVAILABLE  This station's real sensor has not
  returned a genuine reading in over 60s -- data is currently being synthesized by
  a fallback simulator to keep the pipeline running and does not reflect real
  conditions. Tell the citizen the sensor is broken; do not show a value.

Restoring the real API (API_TIMEOUT_SECONDS back to default, producer recreated)...
[07:14:09] (recovery) station-01: -> UNAVAILABLE  (same advisory as above)
[07:14:29] (recovery) station-01: -> OK  Reading reflects current conditions.

Result: outage watch ended at UNAVAILABLE, recovery watch ended at OK -- PASS.
```

This is the concrete evidence for the "fragile elderly citizen" scenario
from tutor feedback: the app-facing contract correctly stopped presenting
a value as current within 40s of the real sensor going dark, even though
the pipeline itself kept producing fresh-looking synthetic readings the
whole time -- and correctly resumed once the real sensor came back.

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
configuration change, not a rewrite. The `sensor-readings` topic is created
with 5 partitions (matching the number of stations) and messages are keyed
by `station_id`, so additional consumers in the same consumer group would
parallelize by station once more than one is deployed. Running only one
broker locally means there is no replication/fault-tolerance for the data
topic itself -- an accepted limitation of the local prototype, not of the
architecture.

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

To check whether any message failed all retries and was parked in the
dead-letter collection:

```bash
docker exec -it mongo mongosh environment_monitoring --eval "db.sensor_readings_dead_letters.find().pretty()"
```

To check the pipeline's health from the host machine (latest reading per
station, how stale each one is, and dead-letter count) instead of querying
Mongo by hand:

```bash
pip install pymongo
python scripts/check_status.py
```

To see the query strategy a city planner would actually use -- daily
per-station trend aggregates instead of raw readings:

```bash
python scripts/planner_queries.py --days 30
```

To see the reliability contract a citizen-facing app backend would call
(ok / stale / unavailable per station, never presenting a stale reading
as current conditions):

```bash
python scripts/citizen_status.py
```

To persist the daily aggregation into a `daily_station_stats` collection
(needed once history is wanted beyond `RAW_RETENTION_DAYS`, since raw
readings expire on a TTL index -- see `consumer/consumer.py`):

```bash
python scripts/materialize_daily_stats.py --days 2
```

To verify -- not just assert -- that the citizen-status contract catches a
real Open-Meteo outage instead of being fooled by the fallback simulator's
fresh-looking synthetic readings (requires `docker compose up -d` running
and the `docker` CLI on PATH; takes a few minutes, forces the producer's
API timeout to ~0 and back via `API_TIMEOUT_SECONDS`):

```bash
python scripts/verify_citizen_failover.py
```

MongoDB's port is published to the host as **27018**, not the default
27017 -- this avoids silently colliding with a locally installed MongoDB
service some machines already run on 27017 (connecting to the wrong
database there produces no error, just wrong/empty results). Container-to-
container traffic (producer/consumer -> `mongo:27017`) is unaffected since
it never goes through the published host port.

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
scripts/                 check_status.py: host-side pipeline health check (fails on staleness or dead letters)
                          planner_queries.py: daily trend aggregation for city planners
                          materialize_daily_stats.py: persists the daily aggregation past raw-data TTL expiry
                          citizen_status.py: ok/stale/unavailable reliability contract for a citizen app
                          verify_citizen_failover.py: end-to-end test that the contract catches a real outage
docs/                    Portfolio submission texts (Phase 1 concept, Phase 2 explanation)
```

## Configuration

All settings are environment variables with sensible defaults (see
`docker-compose.yml`), e.g. `FETCH_INTERVAL_SECONDS`, `KAFKA_TOPIC`,
`MONGO_DB`, `RAW_RETENTION_DAYS` (how long raw readings are kept before a
MongoDB TTL index expires them; default 90). No API key is required for
Open-Meteo.
