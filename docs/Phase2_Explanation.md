Nachname-Vorname_MATRNR_DataEngineering_P2_S

# Explanation: Implementation of the Stream Processing Pipeline

**GitHub repository:** `<INSERT-YOUR-GITHUB-URL-HERE>`

## System description and expected usage

The repository contains a fully containerized pipeline that streams
simulated municipal environmental sensor data into MongoDB via Kafka.
`docker compose up --build` starts Kafka, MongoDB, a producer and a
consumer, and the pipeline begins storing readings automatically. The
resulting collection is the interface a planner dashboard or a
citizen-warning app would query; neither front end is part of this
prototype. Full system description, architecture, and usage instructions
are in the repository's `README.md`.

## Steps carried out in this phase

- Kafka (KRaft, single node) and MongoDB run as official containers,
  wired together with health checks in `docker-compose.yml`.
- `producer/producer.py` polls the Open-Meteo API per station every 10s
  and publishes JSON readings to Kafka, falling back to a local simulator
  on any API failure so the stream never stops.
- `consumer/consumer.py` writes each reading into MongoDB as an
  idempotent upsert, commits Kafka offsets only after a successful write,
  and parks messages that exhaust retries in a (TTL-bounded) dead-letter
  collection instead of dropping them.
- `scripts/planner_queries.py`, `materialize_daily_stats.py`,
  `citizen_status.py`, and `check_status.py` implement the query/reliability
  contracts described in the README.
- The project was pushed to a public GitHub repository so it can be cloned
  and run end-to-end with one command.

## Response to tutor feedback

Feedback on the first draft was that the technical choices were sound but
disconnected from what planners and the citizen app actually need. This is
addressed with running code, not just description: `planner_queries.py`
and `materialize_daily_stats.py` give planners daily aggregates instead of
raw samples, and `citizen_status.py` distinguishes "data was delivered"
from "this specific reading is safe to show as current conditions" -- the
distinction the feedback's elderly-citizen scenario turns on.
`verify_citizen_failover.py` forces a real outage against the running
stack to confirm that behaviour rather than asserting it -- run against
the live pipeline, it observed the status flip from `ok` to `unavailable`
within 40s of the simulated outage and recover within 20s of restoring
the API (full transcript in the README). Full reasoning is in the module
docstrings and the README's "Addressing tutor feedback" section, not
repeated here to stay within the page limit.
