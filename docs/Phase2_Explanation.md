Nachname-Vorname_MATRNR_DataEngineering_P2_S

# Explanation: Implementation of the Stream Processing Pipeline

**GitHub repository:** `<INSERT-YOUR-GITHUB-URL-HERE>`

## System description and expected usage

The repository contains a fully containerized pipeline that streams
simulated municipal environmental sensor data into MongoDB via Kafka.
Cloning the repository and running `docker compose up --build` starts
four services -- Kafka (single-node KRaft mode), MongoDB, a producer and a
consumer -- and the pipeline begins storing readings automatically,
without any manual configuration. This matches the intended end-user
context described in the concept: the resulting MongoDB collection
(`environment_monitoring.sensor_readings`) is the interface that a
planner dashboard or a citizen-warning application would query for the
newest readings per station; neither of those front ends is part of this
prototype, but the data model was designed so they can be built directly
on top of it without changes to the pipeline.

## Steps carried out in this phase

- **Kafka & MongoDB via Docker:** both run as official public container
  images (`apache/kafka`, `mongo`) defined in `docker-compose.yml`, with
  health checks so dependent services only start once Kafka/MongoDB are
  actually ready to accept connections.
- **Producer script (`producer/producer.py`):** polls the Open-Meteo API
  for five simulated stations every ten seconds and publishes each
  reading as a JSON message to the `sensor-readings` Kafka topic. If the
  API call fails for any reason, the script transparently switches to a
  local random-walk simulator seeded from the last known value, tags the
  message `"source": "simulated"`, and continues the stream without
  interruption -- this was tested by disabling network access to the
  container and confirming that readings kept flowing.
- **Consumer script (`consumer/consumer.py`):** subscribes to the topic
  and inserts each reading into MongoDB. Kafka offsets are committed only
  after a successful write, and MongoDB write errors are retried with
  backoff before a message is given up on, so a short MongoDB restart
  does not crash the consumer or silently drop data.
- **Dockerfiles** for producer and consumer install exact pinned
  dependency versions (`confluent-kafka`, `requests`, `pymongo`) so builds
  are reproducible on any machine, independent of the local Python
  installation.
- **Troubleshooting:** two non-obvious issues came up during testing.
  First, the pure-Python `kafka-python` client silently hung on message
  fetch against the current Kafka broker version, which was only visible
  once end-to-end message flow was actually verified rather than trusting
  a clean startup log; switching to the official `confluent-kafka` client
  resolved it. Second, the consumer never received any messages even
  though the producer was publishing successfully, because Kafka's
  default `offsets.topic.replication.factor` of 3 cannot be satisfied on
  a single-broker cluster, so the internal `__consumer_offsets` topic was
  never created and consumer-group coordination failed silently. Setting
  `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1` (and the equivalent
  transaction-log settings) in `docker-compose.yml` fixed it. Both issues
  were only found by actually running the full pipeline end-to-end and
  checking the resulting MongoDB collection, not by reading logs alone.
- **`docker-compose.yml`** wires all four services together on a shared
  Docker network, with named volumes for Kafka and MongoDB so data
  survives container restarts.
- **GitHub repository:** the project (code, Dockerfiles, compose file,
  README) was pushed to a public repository so it can be cloned and run
  end-to-end on any machine with Docker installed, fulfilling the
  portability requirement from Task 1's design principles carried over
  into this task.

## Open points for the finalization phase

Based on tutor feedback, the finalization phase will focus on
hardening error handling further (e.g. a dead-letter mechanism for
messages that exceed the MongoDB retry limit instead of just logging and
skipping them) and adding a minimal read example/query script to make the
stored data easier to explore.
