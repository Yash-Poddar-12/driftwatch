"""
detector.py — Kafka consumer that scores each sliding window with the trained
Isolation Forest and prints results to stdout.

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
    downstream consumers (Phase 3 TimescaleDB) can plot a continuous signal
    rather than just a binary flag.

7.  /healthz endpoint:
    As required by AGENTS.md rule 6, a minimal HTTP health server runs on a
    daemon thread.  Reports {"status": "ok"} if the consumer loop is running,
    {"status": "starting"} before the first successful poll.

8.  TimescaleDB NOT wired up here (Phase 3 scope):
    Scores are printed to stdout only.  Phase 3 will add a DB writer.  Keeping
    the detector stateless (no DB connection) means it can restart freely and
    scales horizontally without connection-pool issues.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any

import joblib
import numpy as np
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
    """
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        enable_auto_commit=False,
        auto_offset_reset="latest",
        request_timeout_ms=30_000,
        session_timeout_ms=30_000,
        heartbeat_interval_ms=10_000,
    )


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
                for r in results:
                    # Phase 2: print to stdout only.
                    # Phase 3 will add: write r to TimescaleDB.
                    print(json.dumps(r), flush=True)
                # Commit offsets after successful window emission.
                consumer.commit()

    except KeyboardInterrupt:
        log.info("Shutdown signal received — closing consumer.")
    finally:
        consumer.close()
        log.info("Consumer closed.")


if __name__ == "__main__":
    run()
