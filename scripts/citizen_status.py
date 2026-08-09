"""
Citizen-app reliability contract for the municipal environmental sensor
project.

Tutor feedback pointed out that "the reliability of Kafka" and "the
reliability for citizens" are two different things, and asked concretely:
can a fragile, elderly citizen trust the app enough to go outside if a
sensor is broken? This module answers that with an explicit contract a
citizen-facing app backend would call instead of reading MongoDB directly.

The distinction that matters:

- **Delivery reliability** (what the pipeline already guarantees): no
  reading is silently lost between producer and MongoDB. At-least-once
  Kafka delivery plus upsert writes plus a dead-letter collection (see
  consumer/consumer.py) mean every reading that was produced eventually
  lands in the database, or is visibly parked for follow-up.
- **Presentation reliability** (what citizens actually need): whether the
  *newest stored reading* is still a trustworthy description of *current*
  conditions. Two distinct failure modes can break this, and both must be
  caught:
  1. Nothing is arriving at all (message age -- the obvious case).
  2. Something is arriving on schedule, but it isn't real: when
     Open-Meteo is unreachable, the producer's fallback simulator keeps
     publishing fresh-looking, plausible readings every cycle so the
     *pipeline* never stalls (see producer/producer.py). That is correct
     behaviour for demonstrating the pipeline, but it means a citizen app
     that only checks "how old is the newest reading?" would report a
     broken sensor as perfectly fine indefinitely, because synthetic data
     keeps its timestamp fresh. This is exactly the "sensor broken, can
     the fragile elderly citizen trust the app?" scenario from feedback,
     and it is caught here, not by age alone.

This module never lets a stale, missing, or fallback-only reading pass as
a confident current value. It classifies every station into one of three
states and returns an advisory message the app is expected to show
verbatim:

- "ok"          -- a reading sourced from the real sensor/API is fresh
                    enough to trust. Safe to present as current conditions.
- "stale"       -- a real reading exists but is older than
                    STALE_AFTER_SECONDS; the app must label it with its
                    age, not present it as "now".
- "unavailable" -- either no reading has arrived recently enough
                    (UNAVAILABLE_AFTER_SECONDS), or the station has been
                    running on the fallback simulator for longer than
                    SIMULATED_GRACE_SECONDS with no real reading in
                    between. Either way the real sensor cannot currently
                    be trusted, and the app must say so instead of
                    guessing.

Run from the host (requires `pip install pymongo`) while
`docker compose up` is running:

    python scripts/citizen_status.py
"""

import os
import sys
from datetime import datetime, timezone

from pymongo import MongoClient, DESCENDING
from pymongo.errors import PyMongoError

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27018")
MONGO_DB = os.environ.get("MONGO_DB", "environment_monitoring")
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "sensor_readings")

FETCH_INTERVAL_SECONDS = float(os.environ.get("FETCH_INTERVAL_SECONDS", "10"))
# Expressed as multiples of the fetch interval so the demo (10s polling)
# and a real deployment (e.g. 5min polling) use the same logic. In a real
# deployment with a 5-minute sensor interval this would be "stale after
# 15 min, unavailable after 45 min" -- short enough that a citizen deciding
# whether to go outside right now is never shown a number from hours ago.
STALE_AFTER_SECONDS = FETCH_INTERVAL_SECONDS * 3
UNAVAILABLE_AFTER_SECONDS = FETCH_INTERVAL_SECONDS * 9
# How long the fallback simulator is allowed to stand in for a real
# reading before the app must stop trusting it. A single missed API call
# is a non-event (the simulator seeds itself from the last real value and
# a citizen won't notice); a sensor that has been synthetic for minutes
# means the real sensor is actually broken.
SIMULATED_GRACE_SECONDS = FETCH_INTERVAL_SECONDS * 6

STATUS_OK = "ok"
STATUS_STALE = "stale"
STATUS_UNAVAILABLE = "unavailable"


def classify(latest, latest_real_age_seconds, now):
    """Turn one station's latest stored reading (or None, if it has never
    reported), plus the age of its most recent *real* (source="api")
    reading, into the citizen-facing status contract. A station is never
    reported as trustworthy-current based on message age alone -- it also
    has to be backed by a real reading, not just the fallback simulator
    keeping the pipeline alive."""
    if latest is None:
        return {
            "status": STATUS_UNAVAILABLE,
            "age_seconds": None,
            "reading": None,
            "advisory": "No data has ever been received for this station. "
                        "Do not rely on this app for current conditions here.",
        }

    timestamp = datetime.fromisoformat(latest["timestamp"])
    age_seconds = (now - timestamp).total_seconds()

    if age_seconds > UNAVAILABLE_AFTER_SECONDS:
        return {
            "status": STATUS_UNAVAILABLE,
            "age_seconds": age_seconds,
            "reading": None,
            "advisory": (
                "Sensor has not reported recently enough to trust. Do not "
                "display a current-conditions value; tell the citizen the "
                "sensor is offline and to consult another source before "
                "going outside."
            ),
        }

    if latest_real_age_seconds is None or latest_real_age_seconds > SIMULATED_GRACE_SECONDS:
        return {
            "status": STATUS_UNAVAILABLE,
            "age_seconds": age_seconds,
            "reading": None,
            "advisory": (
                "This station's real sensor has not returned a genuine "
                "reading in over "
                f"{SIMULATED_GRACE_SECONDS:.0f}s -- data is currently being "
                "synthesized by a fallback simulator to keep the pipeline "
                "running and does not reflect real conditions. Tell the "
                "citizen the sensor is broken; do not show a value."
            ),
        }

    if age_seconds <= STALE_AFTER_SECONDS:
        status, advisory = STATUS_OK, "Reading reflects current conditions."
    else:
        status = STATUS_STALE
        advisory = (
            f"Last reading is {age_seconds:.0f}s old and may no longer reflect "
            "current conditions. Show it labeled with its age, not as \"now\"."
        )

    return {
        "status": status,
        "age_seconds": age_seconds,
        "reading": latest,
        "advisory": advisory,
    }


def get_all_station_statuses(collection, station_ids):
    now = datetime.now(timezone.utc)
    result = {}
    for station_id in station_ids:
        latest = collection.find_one(
            {"station_id": station_id}, sort=[("timestamp", DESCENDING)]
        )
        latest_real = collection.find_one(
            {"station_id": station_id, "source": "api"}, sort=[("timestamp", DESCENDING)]
        )
        latest_real_age_seconds = None
        if latest_real is not None:
            real_timestamp = datetime.fromisoformat(latest_real["timestamp"])
            latest_real_age_seconds = (now - real_timestamp).total_seconds()
        result[station_id] = classify(latest, latest_real_age_seconds, now)
    return result


def main():
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
    except PyMongoError as exc:
        print(f"Cannot reach MongoDB at {MONGO_URI}: {exc}")
        print("Is `docker compose up` running?")
        sys.exit(1)

    collection = client[MONGO_DB][MONGO_COLLECTION]
    station_ids = sorted(collection.distinct("station_id"))
    if not station_ids:
        print("No readings stored yet -- check producer/consumer logs.")
        sys.exit(1)

    statuses = get_all_station_statuses(collection, station_ids)

    print("Citizen-facing station status (what the app backend would serve)")
    print("=" * 72)
    for station_id, info in statuses.items():
        print(f"\n{station_id}: {info['status'].upper()}")
        print(f"  {info['advisory']}")
        if info["reading"] is not None:
            print(
                f"  temperature={info['reading']['temperature_c']}C  "
                f"age={info['age_seconds']:.0f}s"
            )


if __name__ == "__main__":
    main()
