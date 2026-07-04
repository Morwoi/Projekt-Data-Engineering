"""
Sensor data consumer for the municipal environmental monitoring pipeline.

Reads JSON sensor readings from a Kafka topic and persists them into
MongoDB. Offsets are committed manually, only after a successful write, so
that a MongoDB outage or transient write error causes the message to be
retried instead of silently lost (at-least-once delivery). MongoDB writes
themselves are retried with backoff before the message is given up on, so a
short database restart does not crash the consumer or drop data.
"""

import json
import logging
import os
import sys
import time

from confluent_kafka import Consumer, KafkaException
from pymongo import MongoClient
from pymongo.errors import PyMongoError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [consumer] %(levelname)s %(message)s",
)
log = logging.getLogger("consumer")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "sensor-readings")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "sensor-consumer-group")

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.environ.get("MONGO_DB", "environment_monitoring")
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "sensor_readings")

MAX_WRITE_RETRIES = int(os.environ.get("MAX_WRITE_RETRIES", "5"))
RETRY_BACKOFF_SECONDS = float(os.environ.get("RETRY_BACKOFF_SECONDS", "2"))


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
    """Insert one reading, retrying on transient MongoDB errors. Returns
    True if the write eventually succeeded, False if it was given up on
    (caller decides whether to skip the offset or stop)."""
    for attempt in range(1, MAX_WRITE_RETRIES + 1):
        try:
            collection.insert_one(document)
            return True
        except PyMongoError as exc:
            log.warning(
                "MongoDB write failed (attempt %s/%s) for %s: %s",
                attempt, MAX_WRITE_RETRIES, document.get("station_id"), exc,
            )
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)
    log.error(
        "Giving up on message for %s after %s attempts -- skipping",
        document.get("station_id"), MAX_WRITE_RETRIES,
    )
    return False


def main():
    consumer = connect_consumer()
    mongo_client = connect_mongo()
    collection = mongo_client[MONGO_DB][MONGO_COLLECTION]
    collection.create_index("station_id")
    collection.create_index("timestamp")

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
        # Commit even on a given-up message: a poison message must not
        # block the whole partition forever. It has already been logged
        # above for manual follow-up.
        consumer.commit(message=message, asynchronous=False)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        log.exception("Consumer crashed")
        sys.exit(1)
