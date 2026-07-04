Nachname-Vorname_MATRNR_DataEngineering_P1_S

# Concept: Stream Processing Pipeline for Municipal Environmental Sensors

## 1. Data source

The prototype uses the public **Open-Meteo** weather API
(`https://api.open-meteo.com`, no API key required) as a stand-in for real
municipal sensor hardware. For five fixed geographic points across the
city ("stations"), the system requests current values for temperature,
relative humidity, wind speed, precipitation and surface pressure once
every ten seconds, simulating a continuous near-real-time sensor feed. The
response structure (one JSON object per station and timestamp, with named
numeric fields) is representative of what a real IoT sensor gateway would
emit, and new metrics (e.g. future CO2 or fine-dust sensors) can be added
as additional fields without changing the pipeline. **Backup plan:** if the
API cannot be reached (outage, timeout, rate limit), a local fallback
simulator generates a plausible reading from the last known value and
tags it accordingly, so the stream, and any live demonstration or grading
of the system, never depends on external network availability.

## 2. Goal, usages and success criteria

The system's purpose is to continuously ingest environmental measurements
so that (a) city planners can later analyze trends via dashboards, and (b)
a citizen-facing application can warn residents when values exceed
recommended thresholds. Typical user stories: *"As a planner, I want the
latest readings per district available within seconds of being measured"*
and *"As a citizen-app backend, I want to poll the newest values without
missing readings even if my service was briefly down."* The system
succeeds if readings are never silently lost and remain available with
low latency; it fails if the stream stalls on a temporary fault or if
data is dropped between ingestion and storage.

## 3. Technology choice: Kafka + MongoDB

**Kafka** is chosen over Spark Streaming because the workload here is a
straightforward "ingest-and-persist" pipeline rather than one requiring
in-stream aggregation or windowed analytics; Kafka's durable, replayable
log gives reliability (messages persist until consumed and committed),
scalability (partitions and consumer groups allow adding stations or
consumers later without redesign), and maintainability (a single topic
with a simple JSON schema). **MongoDB** stores the readings because its
flexible, schema-less documents match the JSON messages directly and can
absorb new sensor types without migrations, matching the same
maintainability requirement identified in Task 1. Both run as
single-node Docker containers locally but scale horizontally (Kafka
partitions/brokers, MongoDB sharding/replica sets) in a cloud deployment
without changing application code.

## 4. Implementation plan

1. Provision Kafka (KRaft, single node) and MongoDB via Docker Compose.
2. Implement a Python producer that polls Open-Meteo per station and
   publishes JSON messages to topic `sensor-readings`, with the fallback
   simulator as a safety net.
3. Implement a Python consumer that reads the topic and writes each
   reading into MongoDB, committing offsets only after a successful write.
4. Containerize both services and wire everything together in
   `docker-compose.yml` so the whole pipeline starts with one command.
5. Publish the code to a public GitHub repository with setup instructions.
