"""
Log Producer — publishes structured JSON log events to Kafka.

Design notes (AGENTS.md §1 — explain every choice):

1.  KafkaProducer with value_serializer:  serialise the event dict to UTF-8
    JSON bytes inside the producer config once, so every send() call is clean
    Python with no manual json.dumps() at the call site.

2.  EVENTS_PER_SECOND env var:  controls throughput at runtime without an
    image rebuild.  We sleep 1/rate seconds between publishes — simple and
    accurate enough for a simulation; not a token-bucket but fine here.

3.  ANOMALY_MODE:  one env var with three sub-modes lets any container inject
    labeled anomalies without touching the other containers or the code.
    Labeled ground truth is required later to validate the detector.

4.  /healthz HTTP endpoint on a daemon thread:  AGENTS.md rule 6 — every
    service needs a health check.  A tiny BaseHTTPRequestHandler on port 8080
    in a daemon thread adds zero extra dependencies.  Daemon threads exit
    automatically when the main process stops.

5.  acks='all':  the broker leader waits for all in-sync replicas before
    acknowledging.  Marginally slower but teaches the correct production habit
    from day one; easy to relax if needed.

6.  message_max_bytes / request_timeout_ms:  sensible defaults to avoid silent
    hangs during early-stage debugging when Kafka may not be fully up yet.
"""

from __future__ import annotations

import json
import logging
import os
import random
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

from kafka import KafkaProducer
from kafka.errors import KafkaError

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
log = logging.getLogger("log-producer")

# ---------------------------------------------------------------------------
# Configuration — all tunables come from environment variables (README §2.3).
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS: str = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"
)
KAFKA_TOPIC: str = os.environ.get("KAFKA_TOPIC", "logs.raw")
SERVICE_NAME: str = os.environ.get("SERVICE_NAME", "unknown-service")
REGION: str = os.environ.get("REGION", "us-east-1")
EVENTS_PER_SECOND: float = float(os.environ.get("EVENTS_PER_SECOND", "5"))
# ANOMALY_MODE options: "" (normal) | "latency_spike" | "error_burst" | "unusual_status"
ANOMALY_MODE: str = os.environ.get("ANOMALY_MODE", "").strip().lower()
HEALTHZ_PORT: int = int(os.environ.get("HEALTHZ_PORT", "8080"))

# ---------------------------------------------------------------------------
# Realistic baseline distributions (normal traffic)
# ---------------------------------------------------------------------------
NORMAL_STATUS_WEIGHTS: dict[int, float] = {
    200: 0.88,
    201: 0.05,
    204: 0.03,
    400: 0.02,
    404: 0.01,
    500: 0.01,
}
_STATUS_CODES = list(NORMAL_STATUS_WEIGHTS.keys())
_STATUS_WEIGHTS = list(NORMAL_STATUS_WEIGHTS.values())

NORMAL_MESSAGES = [
    "Request processed successfully",
    "Cache hit",
    "DB query completed",
    "Auth token validated",
    "Payment intent created",
    "Inventory reserved",
    "Session refreshed",
    "Rate limit check passed",
]

ERROR_MESSAGES = [
    "Upstream timeout",
    "Database connection refused",
    "Unhandled exception in handler",
    "Circuit breaker open",
    "Memory pressure — GC pause",
]


# ---------------------------------------------------------------------------
# Event generation
# ---------------------------------------------------------------------------

def _normal_latency_ms() -> float:
    """Log-normal latency with median ~80 ms, realistic long tail."""
    return max(1.0, random.lognormvariate(4.4, 0.5))


def build_event() -> dict[str, Any]:
    """
    Build one structured log event.

    ANOMALY_MODE controls which failure mode is injected:
      - latency_spike   — p50 latency jumps 20-40×, mimicking a GC pause / slow DB.
      - error_burst     — ~80 % of events are 5xx, mimicking a crashing downstream.
      - unusual_status  — strange status codes (206, 301, 503) at high frequency,
                          mimicking a misconfigured proxy or runaway redirect loop.
    Normal mode (empty ANOMALY_MODE): realistic latency + healthy status distribution.
    """
    ts = time.time()

    if ANOMALY_MODE == "latency_spike":
        latency_ms = _normal_latency_ms() * random.uniform(20, 40)
        status_code = random.choices(_STATUS_CODES, _STATUS_WEIGHTS)[0]
        message = random.choice(NORMAL_MESSAGES + ERROR_MESSAGES)

    elif ANOMALY_MODE == "error_burst":
        latency_ms = _normal_latency_ms()
        # 80 % 5xx errors
        if random.random() < 0.80:
            status_code = random.choice([500, 502, 503, 504])
            message = random.choice(ERROR_MESSAGES)
        else:
            status_code = 200
            message = random.choice(NORMAL_MESSAGES)

    elif ANOMALY_MODE == "unusual_status":
        # Inject unusual-but-not-strictly-error codes at high rate
        unusual = [206, 301, 302, 304, 401, 403, 429, 503]
        if random.random() < 0.70:
            status_code = random.choice(unusual)
            message = "Unexpected response from upstream"
        else:
            status_code = random.choices(_STATUS_CODES, _STATUS_WEIGHTS)[0]
            message = random.choice(NORMAL_MESSAGES)
        latency_ms = _normal_latency_ms()

    else:
        # Normal traffic
        latency_ms = _normal_latency_ms()
        status_code = random.choices(_STATUS_CODES, _STATUS_WEIGHTS)[0]
        message = random.choice(NORMAL_MESSAGES)

    return {
        "timestamp": ts,
        "service_name": SERVICE_NAME,
        "status_code": status_code,
        "latency_ms": round(latency_ms, 3),
        "message": message,
        "region": REGION,
        "anomaly_mode": ANOMALY_MODE or "normal",
    }


# ---------------------------------------------------------------------------
# /healthz  (AGENTS.md rule 6 — every service needs a health-check endpoint)
# ---------------------------------------------------------------------------

class _HealthHandler(BaseHTTPRequestHandler):
    """Minimal HTTP handler — responds 200 OK to GET /healthz."""

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/healthz":
            body = b'{"status":"ok"}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, fmt: str, *args: Any) -> None:  # noqa: ANN401
        # Suppress the default BaseHTTPServer access log spam.
        pass


def _start_healthz_server() -> None:
    """Start the health-check HTTP server on a background daemon thread."""
    server = HTTPServer(("0.0.0.0", HEALTHZ_PORT), _HealthHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    log.info("Health endpoint listening on :%d/healthz", HEALTHZ_PORT)


# ---------------------------------------------------------------------------
# Kafka producer
# ---------------------------------------------------------------------------

def _build_producer() -> KafkaProducer:
    """
    Create a KafkaProducer with reasonable defaults.

    value_serializer:   converts the event dict to UTF-8 JSON bytes once,
                        centrally, so send() calls stay clean.
    acks='all':         wait for all in-sync replicas to confirm — correct
                        production default; teaches durability habits early.
    retries=5:          automatic retry on transient send failures before
                        raising to the application layer.
    request_timeout_ms: prevents indefinite hangs during Kafka cold-start.
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
        request_timeout_ms=30_000,
        max_block_ms=60_000,  # how long send() blocks if buffer is full
    )


def _on_send_error(exc: Exception) -> None:
    log.error("Kafka send error: %s", exc)


def run() -> None:
    """Main loop — publish events at EVENTS_PER_SECOND until interrupted."""
    _start_healthz_server()

    sleep_s = 1.0 / max(EVENTS_PER_SECOND, 0.01)
    log.info(
        "Starting producer  service=%s  region=%s  rate=%.1f ev/s  mode=%s  topic=%s",
        SERVICE_NAME,
        REGION,
        EVENTS_PER_SECOND,
        ANOMALY_MODE or "normal",
        KAFKA_TOPIC,
    )

    # Retry connecting to Kafka — it may not be ready at container start.
    producer: KafkaProducer | None = None
    for attempt in range(1, 11):
        try:
            producer = _build_producer()
            log.info("Connected to Kafka at %s", KAFKA_BOOTSTRAP_SERVERS)
            break
        except KafkaError as exc:
            log.warning(
                "Kafka not ready (attempt %d/10): %s — retrying in 5 s",
                attempt,
                exc,
            )
            time.sleep(5)

    if producer is None:
        log.error("Could not connect to Kafka after 10 attempts — exiting.")
        raise SystemExit(1)

    sent = 0
    try:
        while True:
            event = build_event()
            producer.send(KAFKA_TOPIC, value=event).add_errback(_on_send_error)
            sent += 1
            if sent % 100 == 0:
                log.info("Published %d events (latest status=%s)", sent, event["status_code"])
            time.sleep(sleep_s)
    except KeyboardInterrupt:
        log.info("Shutdown signal received — flushing and closing producer.")
    finally:
        producer.flush(timeout=10)
        producer.close()
        log.info("Producer closed after %d events.", sent)


if __name__ == "__main__":
    run()
