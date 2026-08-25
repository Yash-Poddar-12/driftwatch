# ADR 0004 — TimescaleDB for Time-Series Storage

**Status:** Accepted  
**Date:** 2026-08-19  
**Author:** Antigravity / Claude Sonnet 4.6  
**Branch:** `agent/antigravity/phase3-storage-dashboard`

---

## Context

Phase 3 requires persisting scored sliding windows (feature vectors + anomaly scores)
so Grafana can render live time-series charts.  Options considered:

| Option | Pros | Cons |
|--------|------|------|
| Plain Postgres | Simple, no extension | Full table scans on time-range queries; no built-in time partitioning |
| InfluxDB | Purpose-built for metrics | Custom query language (Flux/InfluxQL); different from SQL the owner already knows |
| **TimescaleDB** | SQL-compatible, hypertables (automatic time chunking), easy Grafana integration via standard PostgreSQL datasource | Requires the timescaledb extension; slightly more setup than plain Postgres |
| Prometheus | Industry standard for infra metrics | Pull-based (doesn't work well for ML model outputs); requires PromQL |

## Decision

Use **TimescaleDB** (`timescale/timescaledb:latest-pg16`).

**Primary reason:** TimescaleDB is PostgreSQL with an extension — the owner can query
it with plain `psql`, write standard SQL in Grafana queries, and understand every
piece of it.  No new query language to learn.

**Secondary reason:** Hypertables give time-based partitioning for free.  A query
like `WHERE time > now() - interval '5 minutes'` only touches the relevant chunks
instead of scanning the whole table — the right architecture for a growing metric
store.

## Schema Design

Two tables (see `infra/timescaledb/init.sql`):

- **`metrics`** — every scored window (normal + anomalous).  Stores the full
  feature vector so Grafana can plot all metric trends.
- **`anomalies`** — flagged windows only (is_anomalous=true), with the same feature
  snapshot.  Grafana uses this smaller table for anomaly event markers so it
  doesn't need a `WHERE is_anomalous=TRUE` scan on the large metrics table.

## Consequences

- The anomaly-detector now requires `psycopg2-binary==2.9.12` in requirements.txt.
- The detector is no longer stateless: it holds a DB connection.  This is acceptable
  for Phase 3 (one detector replica).  For horizontal scaling (Phase 8), swap for
  `psycopg2.pool.ThreadedConnectionPool`.
- The `timescaledb-data` named volume persists data across `docker compose down/up`.
  Use `docker compose down -v` to wipe it for a clean slate.
