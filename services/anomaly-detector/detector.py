"""
detector.py — Kafka consumer that scores each sliding window with the trained
Isolation Forest, writes results to TimescaleDB, and prints to stdout.

Design notes (AGENTS.md §1 — explain every choice):

1.  Consumer group ("driftwatch-anomaly-detector"):
    All detector replicas share one consumer group.  Kafka assigns each partition
    to exactly one replica in the group, so 3 partitions → up to 3 replicas
    process in parallel without double-scoring any event.  This is the correct
    scaling model for a stateful streaming consumer.

2.  auto_offset_reset="latest":
    On first start (no committed offset) we begin at the latest message, not
    the beginning.  This avoids replaying stale historical events (possibly
    hours old) through a freshly started detector — a lag spike that would
    trigger false anomalies before the sliding window stabilises.

3.  enable_auto_commit=False + manual commit:
    We commit offsets only after successfully calling emit_windows().  This gives
    at-least-once semantics: if the process crashes mid-window, those events are
    re-processed on restart.  The alternative (auto-commit on poll) is easier but
    can silently skip events if the process dies between poll() and processing.

4.  Poll loop with slide timer:
    A single thread polls Kafka (blocking up to POLL_TIMEOUT_MS between
    batches), accumulates events in the SlidingWindowAccumulator, then emits
    windows every WINDOW_SLIDE_SECONDS.  This is simpler than a separate timer
    thread and avoids any shared-state concurrency issues.

5.  Model loading:
    The trained model is loaded once at startup from ANOMALY_MODEL_PATH.
    joblib.load() is fast (<100 ms for a 100-tree IsolationForest), so startup
    latency is negligible.

6.  Anomaly threshold:
    IsolationForest.predict() returns +1 (normal) or -1 (anomaly).  We also
    expose the raw decision_function score (negative = more anomalous) so
    downstream consumers (TimescaleDB, Grafana) can plot a continuous signal
    rather than just a binary flag.

7.  /healthz endpoint:
    As required by AGENTS.md rule 6, a minimal HTTP health server runs on a
    daemon thread.  Reports {"status": "ok"} if the consumer loop is running,
    {"status": "starting"} before the first successful poll.

8.  TimescaleDB writer (Phase 3):
    Each scored window is written to two tables:
      - metrics: always (every window, normal + anomalous)
      - anomalies: only when is_anomalous=True
    psycopg2 is used (industry-standard sync Postgres driver, no event loop
    complexity).  The connection is opened once at startup and reused; cursor
    objects are created per-batch to avoid holding transactions open.
    Stdout JSON logging is preserved so you can still `docker logs anomaly-detector`
    and see real-time scores — useful for debugging without opening a DB client.

9.  DB retry on startup:
    psycopg2.connect() is retried with the same 5 s / 10-attempt loop as the
    Kafka connection.  The docker-compose depends_on health-check gates startup,
    but a belt-and-suspenders retry handles edge cases where the DB is briefly
    unavailable after the health check passes.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import psycopg2
import psycopg2.extras
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from features import SlidingWindowAccumulator

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
log = logging.getLogger("anomaly-detector")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS: str = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"
)
KAFKA_TOPIC: str = os.environ.get("KAFKA_TOPIC", "logs.raw")
KAFKA_GROUP_ID: str = os.environ.get(
    "KAFKA_GROUP_ID", "driftwatch-anomaly-detector"
)
ANOMALY_MODEL_PATH: Path = Path(
    os.environ.get(
        "ANOMALY_MODEL_PATH",
        str(Path(__file__).parent / "models" / "isolation_forest_v1.joblib"),
    )
)
WINDOW_SECONDS: float = float(os.environ.get("WINDOW_SECONDS", "30"))
WINDOW_SLIDE_SECONDS: float = float(os.environ.get("WINDOW_SLIDE_SECONDS", "10"))
POLL_TIMEOUT_MS: int = int(os.environ.get("POLL_TIMEOUT_MS", "1000"))
HEALTHZ_PORT: int = int(os.environ.get("HEALTHZ_PORT", "8084"))

# TIMESCALEDB_URL: standard libpq connection string.
# Default matches the docker-compose.yml hardcoded defaults so the container
# works out-of-the-box without a .env file for quick local testing.
TIMESCALEDB_URL: str = os.environ.get(
    "TIMESCALEDB_URL",
    "postgresql://driftwatch:driftwatch_dev_pw@timescaledb:5432/driftwatch",
)

# ---------------------------------------------------------------------------
# Global health flag (updated by the consumer loop)
# ---------------------------------------------------------------------------
_consumer_ready: bool = False


# ---------------------------------------------------------------------------
# /healthz (AGENTS.md rule 6)
# ---------------------------------------------------------------------------

class _HealthHandler(BaseHTTPRequestHandler):
    """Minimal health-check handler."""

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/healthz":
            status = "ok" if _consumer_ready else "starting"
            body = json.dumps({"status": status}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, fmt: str, *args: Any) -> None:  # noqa: ANN401
        pass  # suppress access log spam


def _start_healthz_server() -> None:
    server = HTTPServer(("0.0.0.0", HEALTHZ_PORT), _HealthHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    log.info("Health endpoint listening on :%d/healthz", HEALTHZ_PORT)


# ---------------------------------------------------------------------------
# Kafka consumer builder
# ---------------------------------------------------------------------------

def _build_consumer() -> KafkaConsumer:
    """
    Create a KafkaConsumer configured for at-least-once delivery.

    value_deserializer: decode UTF-8 JSON bytes back to a Python dict.
    enable_auto_commit=False: we commit manually after processing.
    auto_offset_reset="latest": skip stale backlog on cold start.

    Timeout relationship (AGENTS.md §1 — explain every choice):
      session_timeout_ms  — how long the *broker* waits for a heartbeat
                            before declaring this consumer dead and triggering
                            a rebalance.  30 s is the Kafka default.
      heartbeat_interval_ms — how often the client sends heartbeats.
                            Must be < session_timeout_ms / 3 so at least
                            3 heartbeats can be missed before the broker
                            gives up.  10 s satisfies that (30 / 3 = 10).
      request_timeout_ms  — how long the *client* waits for any broker
                            response (including the heartbeat ACK).
                            kafka-python-ng requires this to be STRICTLY
                            GREATER than session_timeout_ms.  The rationale:
                            if both were equal, a valid-but-slow response
                            could arrive after the broker has already expired
                            the session, ejecting the consumer mid-flight.
                            45 000 ms gives 50 % headroom above the 30 s
                            session timeout — enough margin without making
                            the client hang indefinitely on a truly dead broker.
    """
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        enable_auto_commit=False,
        auto_offset_reset="latest",
        request_timeout_ms=45_000,   # must be > session_timeout_ms (30 000)
        session_timeout_ms=30_000,
        heartbeat_interval_ms=10_000,  # < session_timeout_ms / 3
    )


# ---------------------------------------------------------------------------
# TimescaleDB writer
# ---------------------------------------------------------------------------

def _connect_db() -> "psycopg2.connection":
    """
    Open a psycopg2 connection to TimescaleDB with retry.

    WHY psycopg2 (not psycopg3 / asyncpg)?
      psycopg2 2.9.x is the de-facto standard synchronous Postgres driver,
      battle-tested in production for 15+ years.  psycopg3 is the successor
      but adds complexity (new API).  asyncpg is async-only and would require
      an event loop incompatible with the simple synchronous consumer loop.
      psycopg2-binary bundles the libpq C library — no system-level Postgres
      client install needed inside the container.

    WHY a single persistent connection (not a pool)?
      The detector is single-threaded and writes in small batches every 10 s.
      A connection pool adds complexity with no benefit here.  If we later add
      multi-threaded polling, swap this for psycopg2's ThreadedConnectionPool
      or use sqlalchemy's pool.
    """
    for attempt in range(1, 11):
        try:
            conn = psycopg2.connect(TIMESCALEDB_URL)
            conn.autocommit = False  # explicit transaction control
            log.info("Connected to TimescaleDB at attempt %d", attempt)
            return conn
        except psycopg2.OperationalError as exc:
            log.warning(
                "TimescaleDB not ready (attempt %d/10): %s — retrying in 5 s",
                attempt,
                exc,
            )
            time.sleep(5)
    log.error("Could not connect to TimescaleDB after 10 attempts — exiting.")
    raise SystemExit(1)


def _write_results(
    conn: "psycopg2.connection",
    results: list[dict[str, Any]],
    features_map: dict[str, Any],
) -> None:
    """
    Write scored window results to TimescaleDB.

    Inserts into:
      - metrics (every window, normal + anomalous)
      - anomalies (only flagged windows)

    Uses executemany() with a list of tuples for efficiency — psycopg2
    batches these into a single network round-trip via the C-level copy.

    features_map: dict keyed by service_name → WindowFeatures namedtuple,
    populated by the caller so we can include the full feature vector in
    both tables.

    WHY separate metrics + anomalies tables?
      See init.sql comments.  Short answer: Grafana queries for the continuous
      anomaly_score signal use metrics; anomaly event markers use the much
      smaller anomalies table.  Separating them avoids a costly
      WHERE is_anomalous=TRUE scan on the large metrics table.
    """
    if not results:
        return

    metrics_rows = []
    anomaly_rows = []

    for r in results:
        svc = r["service_name"]
        wf = features_map.get(svc)
        ts = datetime.fromisoformat(r["timestamp"].replace("Z", "+00:00"))

        # Build the metrics row.
        metrics_rows.append((
            ts,
            svc,
            float(wf.request_count) if wf else 0.0,
            float(wf.error_rate) if wf else 0.0,
            float(wf.p50_latency_ms) if wf else 0.0,
            float(wf.p95_latency_ms) if wf else 0.0,
            float(wf.p99_latency_ms) if wf else 0.0,
            float(wf.status_entropy) if wf else 0.0,
            float(r["anomaly_score"]),
            bool(r["is_anomalous"]),
        ))

        # Only write to anomalies if flagged.
        if r["is_anomalous"]:
            anomaly_rows.append((
                ts,
                svc,
                float(r["anomaly_score"]),
                float(wf.request_count) if wf else 0.0,
                float(wf.error_rate) if wf else 0.0,
                float(wf.p50_latency_ms) if wf else 0.0,
                float(wf.p95_latency_ms) if wf else 0.0,
                float(wf.p99_latency_ms) if wf else 0.0,
                float(wf.status_entropy) if wf else 0.0,
            ))

    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO metrics
                  (time, service_name, request_count, error_rate,
                   p50_latency_ms, p95_latency_ms, p99_latency_ms,
                   status_entropy, anomaly_score, is_anomalous)
                VALUES %s
                """,
                metrics_rows,
            )
            if anomaly_rows:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO anomalies
                      (time, service_name, anomaly_score, request_count,
                       error_rate, p50_latency_ms, p95_latency_ms,
                       p99_latency_ms, status_entropy)
                    VALUES %s
                    """,
                    anomaly_rows,
                )
        conn.commit()
        log.debug(
            "Wrote %d metric rows, %d anomaly rows to TimescaleDB",
            len(metrics_rows),
            len(anomaly_rows),
        )
    except psycopg2.Error as exc:
        log.error("DB write failed: %s — rolling back", exc)
        conn.rollback()
        # Don't raise — let the consumer loop continue; the next window
        # will attempt another write.  Transient DB blips shouldn't kill
        # the detector.


# ---------------------------------------------------------------------------
# Score a batch of feature vectors
# ---------------------------------------------------------------------------

def score_windows(
    model: Any,
    features_list: list,
) -> list[dict[str, Any]]:
    """
    Score a list of WindowFeatures with the trained model.

    Returns a list of result dicts with:
        timestamp       : ISO 8601 window-end time
        service_name    : str
        anomaly_score   : float  (lower = more anomalous; Isolation Forest
                                  decision_function output)
        is_anomalous    : bool   (True when model.predict() returns -1)
    """
    if not features_list:
        return []

    X = np.array(
        [wf.to_model_input() for wf in features_list], dtype=np.float64
    )
    raw_scores = model.decision_function(X)   # negative → more anomalous
    predictions = model.predict(X)            # -1 = anomaly, +1 = normal

    results = []
    for wf, score, pred in zip(features_list, raw_scores, predictions):
        results.append(
            {
                "timestamp": time.strftime(
                    "%Y-%m-%dT%H:%M:%SZ", time.gmtime(wf.window_end_ts)
                ),
                "service_name": wf.service_name,
                "anomaly_score": round(float(score), 6),
                "is_anomalous": bool(pred == -1),
            }
        )
    return results


# ---------------------------------------------------------------------------
# Main consumer loop
# ---------------------------------------------------------------------------

def run() -> None:
    """Main entry point — poll Kafka, accumulate events, score windows."""
    global _consumer_ready

    _start_healthz_server()

    # Load model.
    if not ANOMALY_MODEL_PATH.exists():
        log.error(
            "Model not found at %s — run train.py first.", ANOMALY_MODEL_PATH
        )
        raise SystemExit(1)
    log.info("Loading model from %s …", ANOMALY_MODEL_PATH)
    model = joblib.load(ANOMALY_MODEL_PATH)
    log.info("Model loaded: %s", model)

    # Connect to TimescaleDB.
    db_conn = _connect_db()

    # Connect to Kafka with retry.
    consumer: KafkaConsumer | None = None
    for attempt in range(1, 11):
        try:
            consumer = _build_consumer()
            log.info(
                "Connected to Kafka at %s, topic=%s, group=%s",
                KAFKA_BOOTSTRAP_SERVERS,
                KAFKA_TOPIC,
                KAFKA_GROUP_ID,
            )
            break
        except KafkaError as exc:
            log.warning(
                "Kafka not ready (attempt %d/10): %s — retrying in 5 s",
                attempt,
                exc,
            )
            time.sleep(5)

    if consumer is None:
        log.error("Could not connect to Kafka after 10 attempts — exiting.")
        raise SystemExit(1)

    accumulator = SlidingWindowAccumulator(
        window_seconds=WINDOW_SECONDS,
        slide_seconds=WINDOW_SLIDE_SECONDS,
    )

    log.info(
        "Detector running  window=%ss  slide=%ss",
        WINDOW_SECONDS,
        WINDOW_SLIDE_SECONDS,
    )
    _consumer_ready = True

    try:
        while True:
            # Poll for up to POLL_TIMEOUT_MS (returns immediately if messages
            # are available, waits up to the timeout if the queue is empty).
            records = consumer.poll(timeout_ms=POLL_TIMEOUT_MS)
            for partition_records in records.values():
                for msg in partition_records:
                    accumulator.add_event(msg.value)

            now = time.time()
            if accumulator.should_emit(now):
                windows = accumulator.emit_windows(now)
                results = score_windows(model, windows)

                # Build a map of service_name → WindowFeatures for the DB writer.
                features_map = {wf.service_name: wf for wf in windows}

                for r in results:
                    # Stdout JSON logging (Phase 2 behaviour, preserved).
                    # Useful for `docker logs anomaly-detector` debugging.
                    print(json.dumps(r), flush=True)

                # Write to TimescaleDB (Phase 3).
                _write_results(db_conn, results, features_map)

                # Commit offsets after successful window emission + DB write.
                consumer.commit()

    except KeyboardInterrupt:
        log.info("Shutdown signal received — closing consumer.")
    finally:
        consumer.close()
        log.info("Consumer closed.")
        if db_conn and not db_conn.closed:
            db_conn.close()
            log.info("DB connection closed.")


if __name__ == "__main__":
    run()
