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
| Scaffold `services/anomaly-detector/` | 🟨 | Antigravity / Claude Sonnet 4.6 | branch: `agent/antigravity/anomaly-detector-service` |
| Sliding-window feature extraction (`features.py`) | 🟨 | Antigravity / Claude Sonnet 4.6 | 30s window / 10s slide, per-service, extracts request count, error rate, p50/p95/p99 latency, entropy |
| Train + integrate Isolation Forest v1 model | 🟨 | Antigravity / Claude Sonnet 4.6 | offline synthetic training, model saved to models/, ADR in docs/decisions/ |
| Unit tests for detector scoring | 🟨 | Antigravity / Claude Sonnet 4.6 | pytest, synthetic normal vs. anomalous windows |

## Phase 3 — Storage + Dashboard (local)
| Task | Status | Assigned Tool | Notes |
|---|---|---|---|
| Add TimescaleDB to `docker-compose.yml` + schema | ⬜ | — | |
| Wire detector output into TimescaleDB | ⬜ | — | |
| Add Grafana + provision dashboard JSON | ⬜ | — | |
| `scripts/seed_anomalies.py` for demo/testing | ⬜ | — | |

## Phase 4 — Kubernetes (local, Kind/Minikube)
| Task | Status | Assigned Tool | Notes |
|---|---|---|---|
| Base K8s manifests (`infra/k8s/base/`) | ⬜ | — | |
| Local overlay (`infra/k8s/overlays/local/`) | ⬜ | — | |
| Verify full pipeline running on Kind/Minikube | ⬜ | — | |

## Phase 5 — CI Pipeline
| Task | Status | Assigned Tool | Notes |
|---|---|---|---|
| `ci.yml`: lint + unit tests on PR | ⬜ | — | |
| `ci.yml`: build all Docker images on PR | ⬜ | — | |

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
