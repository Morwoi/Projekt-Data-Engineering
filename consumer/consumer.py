"""
Sensor data consumer for the municipal environmental monitoring pipeline.

Reads JSON sensor readings from a Kafka topic and persists them into
MongoDB. Offsets are committed manually, only after a successful write, so
that a MongoDB outage or transient write error causes the message to be
retried instead of silently lost (at-least-once delivery). Because
at-least-once delivery can redeliver the same message, writes are upserts
keyed on (station_id, timestamp) rather than plain inserts, so a redelivery
updates the existing document instead of creating a duplicate. MongoDB
writes are retried with backoff before a message is given up on; if all
retries fail, the reading is parked in a dead-letter collection instead of
being silently dropped.

Each stored document also gets a `stored_at` BSON date, backing a TTL index
that expires raw readings after RAW_RETENTION_DAYS (default 90). Planner
analysis works off daily/weekly aggregates (see
scripts/planner_queries.py), not individual raw samples, so raw data does
not need to be kept indefinitely; this keeps the collection bounded on a
long-running deployment.
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaException
from pymongo import MongoClient
from pymongo.errors import OperationFailure, PyMongoError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [consumer] %(levelname)s %(message)s",
)
log = logging.getLogger("consumer")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "sensor-readings")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "sensor-consumer-group")

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27018")
MONGO_DB = os.environ.get("MONGO_DB", "environment_monitoring")
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "sensor_readings")

MAX_WRITE_RETRIES = int(os.environ.get("MAX_WRITE_RETRIES", "5"))
RETRY_BACKOFF_SECONDS = float(os.environ.get("RETRY_BACKOFF_SECONDS", "2"))

# How long raw, per-reading documents are kept before MongoDB expires them.
# Planners analyze trends (daily/weekly aggregates, regression), not
# individual 10-second samples, so indefinite raw retention isn't needed;
# see scripts/planner_queries.py for the aggregation queries this supports.
RAW_RETENTION_DAYS = float(os.environ.get("RAW_RETENTION_DAYS", "90"))

# Dead letters need a human to look at them, so they're kept much longer
# than raw readings -- but still bounded, so a bad week doesn't leave this
# collection growing forever after the messages in it are long resolved.
DEAD_LETTER_RETENTION_DAYS = float(os.environ.get("DEAD_LETTER_RETENTION_DAYS", "30"))


def connect_consumer(retries=30, delay=5):
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    for attempt in range(1, retries + 1):
        try:
            consumer.list_topics(timeout=5)
            consumer.subscribe([KAFKA_TOPIC])
            log.info("Connected to Kafka at %s, subscribed to '%s'", KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)
            return consumer
        except KafkaException:
            log.warning(
                "Kafka not reachable yet (attempt %s/%s), retrying in %ss...",
                attempt, retries, delay,
            )
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka after repeated retries")


def connect_mongo(retries=30, delay=5):
    for attempt in range(1, retries + 1):
        try:
            client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            log.info("Connected to MongoDB at %s", MONGO_URI)
            return client
        except PyMongoError:
            log.warning(
                "MongoDB not reachable yet (attempt %s/%s), retrying in %ss...",
                attempt, retries, delay,
            )
            time.sleep(delay)
    raise RuntimeError("Could not connect to MongoDB after repeated retries")


def write_with_retry(collection, document):
    """Upsert one reading by its natural key (station_id, timestamp), retrying
    on transient MongoDB errors. Using upsert instead of insert_one makes the
    write idempotent: Kafka's at-least-once delivery means the same message
    can be redelivered (e.g. consumer crash between write and offset commit),
    and without a natural-key upsert that would create duplicate documents.
    Returns True if the write eventually succeeded, False if it was given up
    on (caller decides what to do with the message)."""
    natural_key = {
        "station_id": document["station_id"],
        "timestamp": document["timestamp"],
    }
    # A separate BSON-date field (distinct from the ISO-string "timestamp"
    # the reading carries) so a TTL index can expire raw documents -- Mongo
    # TTL indexes only work on a real Date type.
    to_store = {**document, "stored_at": datetime.now(timezone.utc)}
    for attempt in range(1, MAX_WRITE_RETRIES + 1):
        try:
            collection.update_one(natural_key, {"$set": to_store}, upsert=True)
            return True
        except PyMongoError as exc:
            log.warning(
                "MongoDB write failed (attempt %s/%s) for %s: %s",
                attempt, MAX_WRITE_RETRIES, document.get("station_id"), exc,
            )
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)
    log.error(
        "Giving up on message for %s after %s attempts -- moving to dead letters",
        document.get("station_id"), MAX_WRITE_RETRIES,
    )
    return False


def ensure_ttl_index(collection, field, expire_after_seconds):
    """Create the TTL index, tolerating a RAW_RETENTION_DAYS change across
    restarts. MongoDB refuses to redefine an existing index's options
    in-place (IndexOptionsConflict, code 85) -- it would rather the caller
    drop and recreate it, which is what we do here instead of crashing on
    startup just because the retention window changed."""
    try:
        collection.create_index(field, expireAfterSeconds=expire_after_seconds)
    except OperationFailure as exc:
        if exc.code != 85:
            raise
        log.info(
            "TTL index on '%s' exists with different options, recreating "
            "with expireAfterSeconds=%s", field, expire_after_seconds,
        )
        collection.drop_index(f"{field}_1")
        collection.create_index(field, expireAfterSeconds=expire_after_seconds)


def move_to_dead_letters(dead_letters, document, reason):
    """Best-effort: park a message that could not be written after all
    retries so it isn't silently lost. If MongoDB is unreachable even for
    this write, we can only log -- there is no further fallback."""
    try:
        dead_letters.insert_one({
            "reading": document,
            "reason": reason,
            # A real BSON date, not a Unix float -- required for the TTL
            # index in main() to work at all (Mongo TTL indexes only expire
            # documents on a Date-typed field).
            "failed_at": datetime.now(timezone.utc),
        })
    except PyMongoError as exc:
        log.error(
            "Could not record dead letter for %s either: %s",
            document.get("station_id"), exc,
        )


def main():
    consumer = connect_consumer()
    mongo_client = connect_mongo()
    db = mongo_client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    dead_letters = db[f"{MONGO_COLLECTION}_dead_letters"]
    collection.create_index([("station_id", 1), ("timestamp", 1)], unique=True)
    ensure_ttl_index(collection, "stored_at", int(RAW_RETENTION_DAYS * 86400))
    ensure_ttl_index(dead_letters, "failed_at", int(DEAD_LETTER_RETENTION_DAYS * 86400))

    log.info("Consumer ready, waiting for messages...")
    while True:
        message = consumer.poll(1.0)
        if message is None:
            continue
        if message.error():
            log.warning("Kafka poll error: %s", message.error())
            continue

        reading = json.loads(message.value().decode("utf-8"))
        success = write_with_retry(collection, reading)
        if success:
            log.info(
                "Stored %s reading for %s (offset %s)",
                reading.get("source"), reading.get("station_id"), message.offset(),
            )
        else:
            move_to_dead_letters(dead_letters, reading, "mongo write retries exhausted")
        # Commit even on a given-up message: a poison message must not
        # block the whole partition forever. It has already been parked in
        # the dead-letter collection above for manual follow-up.
        consumer.commit(message=message, asynchronous=False)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        log.exception("Consumer crashed")
        sys.exit(1)
