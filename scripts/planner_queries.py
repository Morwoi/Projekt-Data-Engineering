"""
City-planner query example for the municipal environmental sensor project.

Tutor feedback on the concept/explanation drafts pointed out that "a
regression is not a day-by-day operation" -- a planner doing trend analysis
should not be handed 8,640 raw 10-second samples per station per day and
told to query MongoDB like a transactional store. This script shows the
query strategy planners are actually expected to use: a MongoDB aggregation
pipeline that rolls raw readings up into daily per-station statistics
(mean/min/max per metric, reading count), which is what a planning
dashboard or a regression/trend model would consume.

Raw readings are only retained for RAW_RETENTION_DAYS (see
consumer/consumer.py, default 90 days, enforced by a TTL index) -- long
enough to compute meaningful daily/weekly trends and to re-run this
aggregation over the recent past, but not an unbounded archive. A
dashboard that needs history beyond that window would persist this
aggregation's output (e.g. into its own `daily_station_stats` collection)
before the underlying raw documents expire; that materialization step is
out of scope for this prototype but the query below is exactly what would
feed it.

Run from the host (requires `pip install pymongo`) while
`docker compose up` is running:

    python scripts/planner_queries.py --days 30
    python scripts/planner_queries.py --days 7 --station station-01
"""

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone

from pymongo import MongoClient, ASCENDING
from pymongo.errors import PyMongoError

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27018")
MONGO_DB = os.environ.get("MONGO_DB", "environment_monitoring")
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "sensor_readings")

# Metrics a planner cares about as trends, each rolled up to daily
# mean/min/max. Precipitation is additionally summed, since "total rainfall
# that day" is the planning-relevant figure, not its average.
METRICS = ["temperature_c", "humidity_pct", "wind_speed_kmh", "pressure_hpa"]


def daily_trend(collection, days=30, station_id=None):
    """Aggregation pipeline: one document per (station, day) with
    mean/min/max per metric, total precipitation, and how many raw
    readings the aggregate is based on. This is the shape a regression or
    a trend chart actually consumes -- not a stream of raw readings."""
    group_fields = {}
    for metric in METRICS:
        group_fields[f"{metric}_avg"] = {"$avg": f"${metric}"}
        group_fields[f"{metric}_min"] = {"$min": f"${metric}"}
        group_fields[f"{metric}_max"] = {"$max": f"${metric}"}

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    initial_match = {"timestamp": {"$gte": cutoff.isoformat()}}
    if station_id:
        initial_match["station_id"] = station_id

    pipeline = [
        # Cheap pre-filter on the indexed (station_id, timestamp) string
        # fields before the more expensive date parsing/grouping below.
        {"$match": initial_match},
        # timestamp is stored as an ISO-8601 string (see producer.py); parse
        # it to a real date so it can be truncated to a calendar day.
        {"$addFields": {"_ts": {"$dateFromString": {"dateString": "$timestamp"}}}},
        {"$group": {
            "_id": {
                "station_id": "$station_id",
                "day": {"$dateTrunc": {"date": "$_ts", "unit": "day"}},
            },
            **group_fields,
            "precipitation_mm_total": {"$sum": "$precipitation_mm"},
            "reading_count": {"$sum": 1},
            "simulated_count": {
                "$sum": {"$cond": [{"$eq": ["$source", "simulated"]}, 1, 0]}
            },
        }},
        {"$sort": {"_id.station_id": ASCENDING, "_id.day": ASCENDING}},
    ]
    return list(collection.aggregate(pipeline))


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--days", type=int, default=30, help="how many days back to aggregate (default: 30)")
    parser.add_argument("--station", default=None, help="restrict to one station_id (default: all stations)")
    args = parser.parse_args()

    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
    except PyMongoError as exc:
        print(f"Cannot reach MongoDB at {MONGO_URI}: {exc}")
        print("Is `docker compose up` running?")
        sys.exit(1)

    collection = client[MONGO_DB][MONGO_COLLECTION]
    rows = daily_trend(collection, days=args.days, station_id=args.station)

    if not rows:
        print(f"No readings in the last {args.days} day(s)"
              + (f" for {args.station}" if args.station else "") + ".")
        return

    header = (f"{'Station':<14} {'Day':<12} {'Temp avg':>9} {'Temp min/max':>14} "
              f"{'Humidity avg':>13} {'Wind avg':>9} {'Precip total':>13} {'#Readings':>10} {'#Simulated':>11}")
    print(header)
    print("-" * len(header))
    for row in rows:
        station_id = row["_id"]["station_id"]
        day = row["_id"]["day"].strftime("%Y-%m-%d")
        print(
            f"{station_id:<14} {day:<12} "
            f"{row['temperature_c_avg']:>8.1f}C "
            f"{row['temperature_c_min']:>5.1f}/{row['temperature_c_max']:<5.1f}C "
            f"{row['humidity_pct_avg']:>12.1f}% "
            f"{row['wind_speed_kmh_avg']:>8.1f} "
            f"{row['precipitation_mm_total']:>12.1f}mm "
            f"{row['reading_count']:>10} "
            f"{row['simulated_count']:>11}"
        )

    print()
    print("'#Simulated' counts readings that came from the fallback simulator "
          "(source=\"simulated\") rather than the live API on that station/day -- "
          "a planner doing regression should treat days with a high simulated "
          "share as lower-confidence data points, not filter them out silently.")


if __name__ == "__main__":
    main()
