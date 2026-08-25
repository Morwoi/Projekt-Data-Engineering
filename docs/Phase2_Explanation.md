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

The feedback on phase 1 was that the two user types were sketched as
superficial stories, and that Kafka's delivery reliability doesn't
actually tell you what a planner or a citizen needs from the data day
to day. Concretely: how much data, and what kind of query, does
planning really require, and what does "reliable" mean for a citizen
deciding whether it's safe to go outside?

I tried to answer both with working code rather than more description.
For planners, planner_queries.py answers the query question directly:
a MongoDB aggregation rolls raw 10-second readings up into daily
per-station mean/min/max, since a regression is never a day-by-day
operation. It also leaves fallback-simulator readings out of those
figures and reports what share of each day's readings were real
(api_coverage_pct). Averaging in synthetic values would have made a
trend number less trustworthy with nothing showing that in the output,
so a planner can spot and discount a low-confidence day themselves
rather than that going unnoticed inside the average.

For the citizen app, citizen_status.py separates "the data was
delivered" from "this specific reading is safe to show as current".
During an outage the fallback simulator keeps producing fresh-looking
readings, so message age alone would call a broken sensor fine. On top
of that, "reliable enough" turned out to be use-case-dependent too: a
reading that's okay to label "a bit older, use with caution" for the
general public isn't necessarily something a health-sensitive person
(an elderly citizen deciding whether to go outside, for example)
should act on. citizen_status.py now takes an optional risk_profile
("standard" or "vulnerable") that halves the staleness thresholds and
turns the advisory from a passive age label into an active
recommendation against relying on the reading.

verify_citizen_failover.py forces a real outage against the running
stack and checks that the status contract actually flips to
unavailable and back, rather than just assuming the logic is correct on
paper. Details are in the module docstrings and the README.
