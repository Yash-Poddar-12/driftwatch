# TASKS.md — Task Board

> Single source of truth for what's done, in progress, or blocked. **Every agent (any tool, any model) must check this file before starting work, and update it before/after working.** Full coordination rules: `AGENTS.md` Section 5.

**Status legend:** ⬜ Not started · 🟨 In progress (owner + tool noted) · ✅ Done · 🚧 Blocked

---

## Phase 1 — Producers + Kafka (local, Docker Compose)
| Task | Status | Assigned Tool | Notes |
|---|---|---|---|
| Scaffold `services/log-producer/` (Dockerfile, producer.py, requirements.txt) | ✅ | Antigravity / Claude Sonnet 4.6 | branch: `agent/antigravity/log-producer-service`. Built producer.py (rate control, 3-mode ANOMALY_MODE, /healthz daemon thread, Kafka retry loop). Multi-stage Dockerfile, non-root user, python:3.11-slim, kafka-python-ng==2.2.3 pinned. |
| Add Kafka + producers to `docker-compose.yml` | ✅ | Antigravity / Claude Sonnet 4.6 | apache/kafka:4.3.1 KRaft mode (no ZooKeeper), named volume, 3-partition topic. Three producer replicas: auth-service/us-east-1, payments-service/eu-west-1, inventory-service/ap-southeast-1. `/healthz` exposed on host ports 8081-8083. Follow-up: Phase 1 verification (console consumer) is still ✅. |
| Verify events visible via console consumer | ✅ | — | |

## Phase 2 — Anomaly Detector (local)
| Task | Status | Assigned Tool | Notes |
|---|---|---|---|
| Scaffold `services/anomaly-detector/` | ✅ | Antigravity / Claude Sonnet 4.6 | `features.py`, `train.py`, `detector.py`, `Dockerfile`, `requirements.txt`, `.gitignore`. Dockerfile bakes the trained model in at build time. /healthz on port 8084. |
| Sliding-window feature extraction (`features.py`) | ✅ | Antigravity / Claude Sonnet 4.6 | `SlidingWindowAccumulator` with per-service deque (O(1) prune). 6-vector: request_count, error_rate, p50/p95/p99_ms, status_entropy. Shannon entropy hand-rolled. |
| Train + integrate Isolation Forest v1 model | ✅ | Antigravity / Claude Sonnet 4.6 | Offline synthetic training (5000 windows, seed=42). Model at `services/anomaly-detector/models/isolation_forest_v1.joblib`. ADRs: 0001-isolation-forest-v1.md, 0002-synthetic-training-data.md. |
| Unit tests for detector scoring | ✅ | Antigravity / Claude Sonnet 4.6 | 20/20 passing. Tests: percentile math, entropy, extract_features, sliding-window pruning/timing, model correctly flags latency-spike and error-burst windows as anomalous; normal window not flagged. |

## Phase 3 — Storage + Dashboard (local)
| Task | Status | Assigned Tool | Notes |
|---|---|---|---|
| Add TimescaleDB to `docker-compose.yml` + schema | ✅ | Antigravity / Claude Sonnet 4.6 | `timescale/timescaledb:latest-pg16`, `timescaledb-data` named volume, `infra/timescaledb/init.sql` auto-runs on first boot creating `metrics` + `anomalies` hypertables with 1-day chunks and service_name indexes. ADR: `docs/decisions/0004-timescaledb-storage.md`. |
| Wire detector output into TimescaleDB | ✅ | Antigravity / Claude Sonnet 4.6 | `psycopg2-binary==2.9.12` added to requirements. `_connect_db()` (10-attempt retry), `_write_results()` (batch inserts via `execute_values`). Stdout JSON logging preserved. Live verified: 153 metric rows + 5 anomaly rows written during single test run. |
| Add Grafana + provision dashboard JSON | ✅ | Antigravity / Claude Sonnet 4.6 | `grafana/grafana-oss:13.0.2`, 8-panel dashboard provisioned via config files (datasources.yml + dashboards.yml + driftwatch.json). Panels: request count, error rate, p50/p95/p99 latency, anomaly score (red-dot override on flagged windows), status entropy, anomaly event table, summary stats. URL: http://localhost:3000. |
| `scripts/seed_anomalies.py` for demo/testing | ✅ | Antigravity / Claude Sonnet 4.6 | CLI: `--service`, `--mode` (latency_spike/error_burst/unusual_status), `--duration`. Spawns `docker compose run --rm` with ANOMALY_MODE, streams output, auto-stops. Follow-up: run seed + confirm Grafana red dots in Phase 3 sign-off. |

## Phase 4 — Kubernetes (local, Kind/Minikube)
| Task | Status | Assigned Tool | Notes |
|---|---|---|---|
| Base K8s manifests (`infra/k8s/base/`) | ✅ | Antigravity / Claude Sonnet 4.6 | Kafka StatefulSet+headless svc, TimescaleDB StatefulSet+PVC, 3 log-producer Deployments, anomaly-detector Deployment, Grafana Deployment. All with readiness/liveness probes, resource limits. Kustomize base with configMapGenerator for provisioning files. |
| Local overlay (`infra/k8s/overlays/local/`) | ✅ | Antigravity / Claude Sonnet 4.6 | Resource patches: Kafka 1536Mi (JVM headroom for broker+probe), Grafana 256Mi, detector 256Mi. nc TCP probe on Kafka (kafka-topics.sh spawns second JVM → OOMKill). |
| Verify full pipeline running on Kind/Minikube | ✅ | Antigravity / Claude Sonnet 4.6 | All 7 pods 1/1 Running on driftwatch-local. TimescaleDB confirms 847+ metric windows and 79+ anomalies stored. Grafana /api/health returns `{"database":"ok","version":"13.0.2"}`. Bug fixes: CRLF+BOM in provisioning ConfigMaps (Grafana crash), Kafka memory 768Mi→1536Mi, probe switched to nc. |


## Phase 5 — CI Pipeline
| Task | Status | Assigned Tool | Notes |
|---|---|---|---|
| `ci.yml`: lint + unit tests on PR | ✅ | Codex / GPT-5 | Verified: Black reformatted the six existing violations and `black --check services tests scripts` is clean. Local pytest remains 16 passed / 8 failed, with every failure the existing `ModuleNotFoundError: psycopg2` before model scoring; CI installs the pinned detector requirements. Black 26.5.1 and Ruff 0.16.6 are pinned to prevent rules drift; pytest intentionally remains flexible. |
| `ci.yml`: build all Docker images on PR | 🟨 | Codex / GPT-5 | Buildx matrix is implemented with `docker/build-push-action@v7` and isolated `type=gha` cache scopes. Pending a PR-run verification because the local Docker daemon was unavailable. |

## Phase 6 — AWS Infra (manual first pass)
| Task | Status | Assigned Tool | Notes |
|---|---|---|---|
| VPC (public/private subnets, NAT) | ⬜ | — | |
| EKS cluster | ⬜ | — | |
| ECR repositories | ⬜ | — | |
| IRSA roles | ⬜ | — | |

## Phase 7 — CD Pipeline
| Task | Status | Assigned Tool | Notes |
|---|---|---|---|
| `cd.yml`: build, tag, push to ECR | ⬜ | — | |
| `cd.yml`: deploy to EKS on merge to `main` | ⬜ | — | |
| Staging namespace smoke test before promote | ⬜ | — | |

## Phase 8 — Autoscaling + Polish
| Task | Status | Assigned Tool | Notes |
|---|---|---|---|
| Install + configure KEDA | ⬜ | — | |
| HPA/KEDA scaling rule on anomaly-detector | ⬜ | — | |
| Final dashboard polish | ⬜ | — | |
| Write up results / benchmark numbers | ⬜ | — | |
