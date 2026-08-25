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
  reliability contracts described below and in the README.
- The project is on a public GitHub repository so it can be cloned and
  run end-to-end with one command.

## Reflection on feedback so far

Earlier feedback was that the two user types were sketched as
superficial stories, and that Kafka's delivery reliability is not the
same thing as what a planner or a citizen actually needs from the data
operationally - concretely: how much data and what kind of query does
planning actually require, and how does "reliable" cash out for a
citizen deciding whether it's safe to go outside?

I addressed both with working code, not just description. For
planners, planner_queries.py answers the query question directly: a
MongoDB aggregation rolls raw 10-second readings up into daily
per-station mean/min/max, because a regression is not a day-by-day
operation. It also excludes fallback-simulator readings from those
figures and reports what share of each day's readings were real
(api_coverage_pct) - averaging in synthetic values would quietly make a
trend number less trustworthy with no visible sign of it, so a planner
can see and discount a low-confidence day instead of that being baked
into the number silently.

For the citizen app, citizen_status.py separates "the data was
delivered" from "this specific reading is safe to show as current":
during an outage the fallback simulator keeps producing fresh-looking
readings, so message age alone would report a broken sensor as fine.
Beyond that distinction, I also operationalized that "reliable enough"
is itself use-case-dependent: a reading that's fine to label "a bit
older, use with caution" for the general public is not necessarily an
acceptable basis for a health-sensitive person (e.g. an elderly citizen
deciding whether to go outside) to act on. citizen_status.py now takes
an optional risk_profile ("standard" or "vulnerable") that halves the
staleness thresholds and switches the advisory text from a passive age
label to an active recommendation against relying on the reading.

verify_citizen_failover.py forces a real outage against the running
stack and checks that the status contract actually flips to
unavailable and back, rather than just assuming the logic is correct on
paper. Details are in the module docstrings and the README.
