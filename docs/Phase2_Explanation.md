Nachname-Vorname_MATRNR_DataEngineering_P2_S

# Explanation: Implementation of the Stream Processing Pipeline

**GitHub repository:** https://github.com/Morwoi/Projekt-Data-Engineering

## System description and expected usage

The repository contains a containerized pipeline that streams simulated
municipal sensor data into MongoDB via Kafka. Running `docker compose up
--build` starts Kafka, MongoDB, a producer and a consumer, and the
pipeline begins storing readings automatically. The resulting MongoDB
collection is what a planner dashboard or a citizen-warning app would
query against; neither front end is built here. Full architecture and
usage instructions are in the repository's README.md.

## Steps carried out in this phase

- Kafka (KRaft, single node) and MongoDB run as official containers,
  wired together with health checks in docker-compose.yml.
- producer/producer.py polls the Open-Meteo API per station every 10s
  and publishes JSON readings to Kafka, falling back to a local
  simulator if the API call fails so the stream doesn't stop.
- consumer/consumer.py writes each reading into MongoDB as an
  idempotent upsert, commits Kafka offsets only after a successful
  write, and parks messages that exhaust retries in a dead-letter
  collection instead of dropping them.
- scripts/planner_queries.py, materialize_daily_stats.py,
  citizen_status.py and check_status.py implement the query and
  reliability contracts described in the README.
- The project is on a public GitHub repository so it can be cloned and
  run end-to-end with one command.

## Reflection on feedback so far

Earlier feedback was that the technical choices were sound but not
clearly tied to what planners and the citizen app actually need from
the data. I addressed that with working code rather than just
description: planner_queries.py and materialize_daily_stats.py give
planners daily aggregates instead of raw samples, and citizen_status.py
separates "the data was delivered" from "this specific reading is safe
to show as current" - which matters because the fallback simulator
keeps producing fresh-looking readings during an outage, so message age
alone isn't enough to tell a broken sensor from a working one.
verify_citizen_failover.py forces a real outage against the running
stack to check that this actually works rather than just assuming the
logic is correct; details are in the module docstrings and the README.
