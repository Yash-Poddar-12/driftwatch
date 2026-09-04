-- init.sql — TimescaleDB schema for DriftWatch
--
-- Design notes (AGENTS.md §1 — explain every choice):
--
-- WHY TWO TABLES instead of one?
--   `metrics` stores every sliding-window feature vector produced by the
--   detector — request_count, error_rate, latency percentiles, status_entropy.
--   `anomalies` stores only the windows flagged as anomalous (is_anomalous=true),
--   plus the continuous anomaly_score for all windows.  Separating them means
--   Grafana can query the anomaly_score time-series efficiently without filtering
--   a large table, and an alerting rule can target `anomalies` only.
--   Both tables share `time` + `service_name` as the natural key for a time-
--   series workload; hypertables partition on `time` automatically.
--
-- WHY TimescaleDB (hypertables) over plain Postgres tables?
--   TimescaleDB wraps Postgres with automatic time-based chunking.  Each
--   chunk covers a configurable time interval (default 7 days).  Queries
--   that filter by time range only touch the relevant chunks, giving orders-
--   of-magnitude speedup vs. a sequential scan on a plain table.  This is the
--   same reason you'd use InfluxDB or Prometheus for metrics, but TimescaleDB
--   keeps full SQL compatibility — so Grafana can use the standard PostgreSQL
--   data source, and the owner can query with plain psql.
--
-- WHY NOT store raw log events?
--   Each sliding window aggregates ~50-150 raw events into 6 numbers.  Storing
--   the raw events would be 100x more data with no additional benefit for the
--   detector's output stream.  Raw events live in Kafka (with 24-hour retention)
--   and can be archived separately if needed.
--
-- INDEX STRATEGY:
--   TimescaleDB's create_hypertable() automatically creates a time-range index
--   on the `time` column.  We add a BTREE index on (time, service_name) to
--   support the most common Grafana query pattern: "give me all rows for
--   service X in the last N minutes."

-- ---------------------------------------------------------------------------
-- Enable TimescaleDB extension
-- ---------------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ---------------------------------------------------------------------------
-- metrics — every scored sliding window (normal + anomalous)
-- ---------------------------------------------------------------------------
-- Stores the full feature vector so Grafana can plot ALL metric trends,
-- not just anomalous ones.  The detector writes here on every emit_windows()
-- call regardless of is_anomalous.
CREATE TABLE IF NOT EXISTS metrics (
    -- `time` is the window_end_ts (Unix epoch converted to timestamptz).
    -- TimescaleDB requires the partition column to be the first column for
    -- efficient range queries.  Using TIMESTAMPTZ (not TIMESTAMP) stores the
    -- UTC offset, preventing bugs if the container timezone ever changes.
    time            TIMESTAMPTZ     NOT NULL,

    -- Which simulated microservice produced this window.
    service_name    TEXT            NOT NULL,

    -- Feature vector (same fields as features.py WindowFeatures).
    -- DOUBLE PRECISION (float8) matches Python's float64 / numpy float64
    -- exactly — no silent precision loss.
    request_count   DOUBLE PRECISION NOT NULL,
    error_rate      DOUBLE PRECISION NOT NULL,
    p50_latency_ms  DOUBLE PRECISION NOT NULL,
    p95_latency_ms  DOUBLE PRECISION NOT NULL,
    p99_latency_ms  DOUBLE PRECISION NOT NULL,
    status_entropy  DOUBLE PRECISION NOT NULL,

    -- anomaly_score from IsolationForest.decision_function().
    -- Lower (more negative) = more anomalous.  Stored here so Grafana can
    -- plot a continuous signal on the same panel as the features.
    anomaly_score   DOUBLE PRECISION NOT NULL,

    -- Binary flag: true when IsolationForest.predict() returns -1.
    is_anomalous    BOOLEAN         NOT NULL DEFAULT FALSE
);

-- Convert to a hypertable, partitioned by `time`.
-- chunk_time_interval = 1 day: each chunk covers 24 hours of windows.
-- At 1 window/10 s per service × 3 services = ~26 000 rows/day — a 1-day
-- chunk is large enough to avoid chunk-metadata overhead but small enough
-- to prune quickly with a DROP CHUNKS retention policy later.
SELECT create_hypertable(
    'metrics',
    'time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Compound index for the most common Grafana query:
--   WHERE service_name = $1 AND time >= $2 AND time <= $3
-- TimescaleDB's chunk exclusion already handles the time range; the BTREE on
-- service_name narrows within each chunk.
CREATE INDEX IF NOT EXISTS idx_metrics_service_time
    ON metrics (service_name, time DESC);

-- ---------------------------------------------------------------------------
-- anomalies — flagged windows only
-- ---------------------------------------------------------------------------
-- A narrower table written only when is_anomalous=true.  Grafana uses this
-- to render "anomaly event" markers (distinct visual points) on top of the
-- continuous metrics chart without a costly WHERE is_anomalous = true scan
-- on the larger metrics table.
CREATE TABLE IF NOT EXISTS anomalies (
    time            TIMESTAMPTZ     NOT NULL,
    service_name    TEXT            NOT NULL,
    anomaly_score   DOUBLE PRECISION NOT NULL,

    -- Snapshot of the window's feature vector at the time of flagging.
    -- Useful for post-hoc analysis: "what did the metrics look like when
    -- this anomaly was flagged?"
    request_count   DOUBLE PRECISION NOT NULL,
    error_rate      DOUBLE PRECISION NOT NULL,
    p50_latency_ms  DOUBLE PRECISION NOT NULL,
    p95_latency_ms  DOUBLE PRECISION NOT NULL,
    p99_latency_ms  DOUBLE PRECISION NOT NULL,
    status_entropy  DOUBLE PRECISION NOT NULL
);

SELECT create_hypertable(
    'anomalies',
    'time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_anomalies_service_time
    ON anomalies (service_name, time DESC);
