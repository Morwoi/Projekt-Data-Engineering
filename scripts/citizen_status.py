"""
Citizen-app reliability contract for the sensor pipeline.

Delivery reliability (no reading lost between producer and MongoDB, see
consumer/consumer.py) is not the same thing as presentation reliability:
whether the *newest stored reading* is still a trustworthy description of
*current* conditions. Two things can break the second one:

1. Nothing has arrived recently (message age, the obvious case).
2. Something is arriving on schedule, but it isn't real. When Open-Meteo
   is unreachable, the producer's fallback simulator keeps publishing
   fresh-looking readings every cycle so the pipeline itself doesn't stall
   (see producer/producer.py). A citizen app that only checks message age
   would report a broken sensor as fine indefinitely, since the synthetic
   data keeps its timestamp current.

Each station is classified into one of three states, with an advisory
message the app is expected to show:

- "ok"          a real (source="api") reading is fresh enough to trust.
- "stale"       a real reading exists but is older than STALE_AFTER_SECONDS.
- "unavailable" either nothing recent has arrived, or the station has been
                running on the fallback simulator longer than
                SIMULATED_GRACE_SECONDS with no real reading in between.

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
# Thresholds scale with the fetch interval so the 10s demo and a real
# deployment (e.g. 5min polling) use the same logic.
STALE_AFTER_SECONDS = FETCH_INTERVAL_SECONDS * 3
UNAVAILABLE_AFTER_SECONDS = FETCH_INTERVAL_SECONDS * 9
# How long the fallback simulator can stand in before we stop trusting it.
SIMULATED_GRACE_SECONDS = FETCH_INTERVAL_SECONDS * 6

STATUS_OK = "ok"
STATUS_STALE = "stale"
STATUS_UNAVAILABLE = "unavailable"


def classify(latest, latest_real_age_seconds, now):
    """Classify a station's latest reading into ok/stale/unavailable. Age
    alone isn't enough - a station only counts as trustworthy if backed
    by a recent real (source="api") reading, not just the fallback
    simulator keeping the pipeline alive."""
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
