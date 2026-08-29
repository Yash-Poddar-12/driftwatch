# 0005-kubernetes-statefulset-vs-deployment.md
#
# Architecture Decision Record — Kubernetes workload type selection
#
# Context, decision, consequences — per AGENTS.md §3 documentation convention.

## Context

Phase 4 ports all DriftWatch services from Docker Compose to Kubernetes.
Each service must be mapped to the correct Kubernetes workload type:
`Deployment` or `StatefulSet`.  This is the most common Kubernetes
interview question for this codebase, so the rationale must be explicit.

---

## Decision

| Service | Type | Reason |
|---|---|---|
| Kafka | `StatefulSet` | Stable network identity + per-pod PVC |
| TimescaleDB | `StatefulSet` | Per-pod PVC for data durability |
| log-producer-auth | `Deployment` | Stateless event generator |
| log-producer-payments | `Deployment` | Stateless event generator |
| log-producer-inventory | `Deployment` | Stateless event generator |
| anomaly-detector | `Deployment` | Stateless consumer; model baked in image |
| Grafana | `Deployment` | Stateless; dashboard config provisioned from ConfigMap |

---

## Why Kafka needs a StatefulSet (not a Deployment)

### 1. Stable network identity

Kafka brokers advertise their own hostname in metadata responses.  When a
producer connects to the bootstrap server and asks "who are the brokers?",
Kafka replies with the list of `ADVERTISED_LISTENERS` addresses.  Clients
then open direct connections to those addresses.

- **Deployment pod name:** `kafka-7d9f4-xkqp2` — random, changes on every
  restart.  Clients cached the old hostname; new connections fail.
- **StatefulSet pod name:** `kafka-0` — deterministic ordinal.  The headless
  Service gives it a stable DNS A record:
  `kafka-0.kafka-headless.default.svc.cluster.local`
  This is predictable across restarts, rescheduling, and rolling updates.

### 2. Per-pod persistent storage (volumeClaimTemplates)

Kafka stores all topic data, offsets, and KRaft metadata on local disk
(`/tmp/kraft-combined-logs`).  If a pod restarts and gets a different node,
the new pod MUST find the same data.

- **Deployment with a PVC:** One shared PVC for all replicas (wrong for
  multi-broker — each broker needs its own data directory) OR a single
  replica with a PVC that may not reattach to the same node (race condition
  with ReadWriteOnce storage classes).
- **StatefulSet volumeClaimTemplate:** Creates `kafka-data-kafka-0` for pod
  `kafka-0`, `kafka-data-kafka-1` for pod `kafka-1`, etc.  When pod `kafka-0`
  restarts, Kubernetes guarantees it reattaches `kafka-data-kafka-0` — not
  any other pod's PVC.  Data survives pod deletion, node drain, and rollout.

### 3. Ordered startup/shutdown

StatefulSets start pods in ordinal order (0, 1, 2) and terminate in reverse.
For a Kafka cluster with multiple brokers, the KRaft controller (conventionally
node 0) must be fully up before the brokers try to register with it.
Deployments start all replicas simultaneously with no ordering guarantee.

---

## Why TimescaleDB needs a StatefulSet (not a Deployment)

Same reasoning as Kafka, focused on storage:

- PostgreSQL stores all data in `/var/lib/postgresql/data` (the PGDATA
  directory).  If a pod restarts with an empty data directory, the database
  is blank — all metrics and anomaly data are lost.
- StatefulSet + `volumeClaimTemplate` creates `timescaledb-data-timescaledb-0`
  exclusively for the database pod.  PVCs are retained even when the pod is
  deleted (Kubernetes default `persistentVolumeClaimRetentionPolicy: Retain`).
- TimescaleDB does not need ordered startup or stable network identity (it has
  no cluster peers), but the storage guarantee is sufficient reason for
  StatefulSet.

---

## Why the remaining services use Deployment

### Log producers

Each producer generates fresh events and publishes to Kafka.  If a producer
pod restarts, it simply starts generating new events.  There is no on-disk
state and no network identity requirement — Kafka consumers don't connect
back to producers.  Deployments are correct.

### Anomaly detector

- **Model:** baked into the Docker image at build time (`train.py` runs during
  `docker build`).  No filesystem state to persist.
- **Sliding window accumulator:** in-memory.  Lost on restart, refills within
  `WINDOW_SECONDS` (30 s) as new Kafka events arrive.
- **Kafka offsets:** managed by the Kafka broker in `__consumer_offsets`.
  A restarted detector pod rejoins the consumer group and continues from the
  last committed offset — no pod-local offset storage needed.
- Therefore: Deployment, with safe horizontal scaling via consumer group
  semantics (additional replicas split partitions).

### Grafana

- All configuration (datasource, dashboard JSON) is provisioned from
  ConfigMaps at startup — no on-disk state that needs persistence.
- `allowUiUpdates: false` in `dashboards.yml` prevents users from creating
  local state that would need preserving.
- Grafana's internal SQLite DB (for user accounts, alert state) is not used
  in this setup.
- Therefore: Deployment.

---

## Consequences

- Kafka and TimescaleDB PVCs persist across `kubectl delete pod`.
  To fully reset: `kubectl delete pvc --all` (destructive — data loss).
- The stable Kafka DNS name (`kafka-0.kafka-headless...`) must be kept in
  sync with `KAFKA_ADVERTISED_LISTENERS` in kafka.yaml.  If the namespace
  or headless service name changes, both must be updated together.
- For a multi-broker Kafka cluster (3 nodes), the StatefulSet design scales
  correctly: replicas: 3, and each gets its own PVC.  The CONTROLLER_QUORUM_VOTERS
  and ADVERTISED_LISTENERS env vars would need updating.
- TimescaleDB in production should be replaced with AWS RDS for PostgreSQL +
  TimescaleDB extension (managed, automated backups) — the StatefulSet path
  is correct for self-hosted local dev and a useful learning exercise, but
  managed services reduce operational burden at the cost of vendor lock-in.
  See ADR 0004-timescaledb-storage.md for the initial storage choice context.
