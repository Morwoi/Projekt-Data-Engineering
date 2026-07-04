"""
Sensor data producer for the municipal environmental monitoring pipeline.

Polls the public Open-Meteo weather API (https://open-meteo.com, no API key
required) once per configured interval for a list of simulated sensor
stations spread across a city. Each response is normalized into a flat
JSON "sensor reading" and published to a Kafka topic.

If the API is temporarily unreachable (network outage, timeout, rate limit,
non-2xx response), the producer falls back to a local synthetic-data
simulator so the stream never stops -- this is the required fallback/backup
behaviour for Task 2 ("does not break if data is temporarily inaccessible").
Every message carries a "source" field ("api" or "simulated") so downstream
consumers and dashboards can distinguish real from fallback readings.
"""

import json
import logging
import os
import random
import signal
import sys
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer
from confluent_kafka import KafkaException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [producer] %(levelname)s %(message)s",
)
log = logging.getLogger("producer")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "sensor-readings")
FETCH_INTERVAL_SECONDS = float(os.environ.get("FETCH_INTERVAL_SECONDS", "10"))
API_TIMEOUT_SECONDS = float(os.environ.get("API_TIMEOUT_SECONDS", "5"))
API_BASE_URL = os.environ.get(
    "OPEN_METEO_URL", "https://api.open-meteo.com/v1/forecast"
)
CURRENT_VARS = "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation,surface_pressure"

# Simulated sensor stations placed at fixed points across a (fictional) city.
# In production, this list would come from a station/asset registry table.
STATIONS = [
    {"station_id": "station-01", "name": "City Center", "latitude": 52.5200, "longitude": 13.4050},
    {"station_id": "station-02", "name": "Harbor District", "latitude": 53.5511, "longitude": 9.9937},
    {"station_id": "station-03", "name": "Industrial Park", "latitude": 51.2277, "longitude": 6.7735},
    {"station_id": "station-04", "name": "Residential North", "latitude": 52.3667, "longitude": 4.8945},
    {"station_id": "station-05", "name": "Green Belt", "latitude": 48.1351, "longitude": 11.5820},
]

# Last known-good reading per station, used as a seed for the fallback
# simulator so simulated values stay in a plausible range instead of jumping
# to an arbitrary default.
_last_good = {}

_shutdown = False


def _handle_shutdown(signum, frame):
    global _shutdown
    log.info("Shutdown signal received, finishing current cycle...")
    _shutdown = True


signal.signal(signal.SIGINT, _handle_shutdown)
signal.signal(signal.SIGTERM, _handle_shutdown)


def connect_producer(retries=30, delay=5):
    """Create the Kafka producer and wait until the cluster metadata can
    actually be fetched, so the container survives Kafka starting up
    slower than the producer during `docker compose up`."""
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "acks": "all",
        "retries": 5,
        "linger.ms": 200,
    })
    for attempt in range(1, retries + 1):
        try:
            producer.list_topics(timeout=5)
            log.info("Connected to Kafka at %s", KAFKA_BOOTSTRAP_SERVERS)
            return producer
        except KafkaException:
            log.warning(
                "Kafka not reachable yet (attempt %s/%s), retrying in %ss...",
                attempt, retries, delay,
            )
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka after repeated retries")


def _delivery_report(err, msg):
    if err is not None:
        log.warning("Message delivery failed for %s: %s", msg.key(), err)


def fetch_from_api(station):
    """Fetch current weather for a station from Open-Meteo. Raises on any
    network/HTTP/parsing problem so the caller can fall back."""
    params = {
        "latitude": station["latitude"],
        "longitude": station["longitude"],
        "current": CURRENT_VARS,
        "timezone": "UTC",
    }
    response = requests.get(API_BASE_URL, params=params, timeout=API_TIMEOUT_SECONDS)
    response.raise_for_status()
    payload = response.json()
    current = payload["current"]

    return {
        "temperature_c": current["temperature_2m"],
        "humidity_pct": current["relative_humidity_2m"],
        "wind_speed_kmh": current["wind_speed_10m"],
        "precipitation_mm": current["precipitation"],
        "pressure_hpa": current["surface_pressure"],
    }


def simulate_reading(station):
    """Fallback generator: a small random walk around the last known-good
    values (or sensible defaults on the very first call) so the pipeline
    keeps producing plausible-looking data during an API outage."""
    seed = _last_good.get(station["station_id"], {
        "temperature_c": 18.0,
        "humidity_pct": 60.0,
        "wind_speed_kmh": 10.0,
        "precipitation_mm": 0.0,
        "pressure_hpa": 1013.0,
    })
    return {
        "temperature_c": round(seed["temperature_c"] + random.uniform(-0.5, 0.5), 1),
        "humidity_pct": max(0, min(100, round(seed["humidity_pct"] + random.uniform(-2, 2), 1))),
        "wind_speed_kmh": max(0, round(seed["wind_speed_kmh"] + random.uniform(-1.5, 1.5), 1)),
        "precipitation_mm": max(0, round(seed["precipitation_mm"] + random.uniform(-0.1, 0.2), 2)),
        "pressure_hpa": round(seed["pressure_hpa"] + random.uniform(-0.5, 0.5), 1),
    }


def build_reading(station):
    """Try the real API first, transparently fall back to the simulator."""
    try:
        metrics = fetch_from_api(station)
        source = "api"
        _last_good[station["station_id"]] = metrics
    except (requests.RequestException, KeyError, ValueError) as exc:
        log.warning(
            "Open-Meteo unreachable for %s (%s) -- using fallback simulator",
            station["station_id"], exc,
        )
        metrics = simulate_reading(station)
        source = "simulated"

    return {
        "station_id": station["station_id"],
        "station_name": station["name"],
        "latitude": station["latitude"],
        "longitude": station["longitude"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": source,
        **metrics,
    }


def main():
    producer = connect_producer()
    log.info(
        "Starting sensor stream: %s stations, every %ss, topic '%s'",
        len(STATIONS), FETCH_INTERVAL_SECONDS, KAFKA_TOPIC,
    )

    while not _shutdown:
        cycle_start = time.time()
        for station in STATIONS:
            reading = build_reading(station)
            producer.produce(
                KAFKA_TOPIC,
                key=station["station_id"],
                value=json.dumps(reading),
                callback=_delivery_report,
            )
            producer.poll(0)
            log.info("Published %s reading for %s", reading["source"], station["station_id"])

        producer.flush()
        elapsed = time.time() - cycle_start
        time.sleep(max(0.0, FETCH_INTERVAL_SECONDS - elapsed))

    log.info("Flushing producer...")
    producer.flush()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        log.exception("Producer crashed")
        sys.exit(1)
