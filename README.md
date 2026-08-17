# Multi-Region Log Anomaly Detection Pipeline

A real-time, cloud-native pipeline that ingests streaming logs from multiple simulated services, detects anomalies using machine learning, and visualizes results on a live dashboard — built end-to-end on Docker, Kubernetes, AWS, and a full CI/CD pipeline.

---

> **For AI agents:** rules, conventions, and multi-tool coordination now live in **`AGENTS.md`** at the repository root (the emerging cross-tool standard read by Codex, Antigravity, and others). Claude Code reads `CLAUDE.md`, which just points to `AGENTS.md`. Current task status lives in **`TASKS.md`** — check it before starting any work. This README stays focused on explaining the project itself.

---

## 1. Project Overview

### 1.1 What this is
Modern distributed systems emit huge volumes of logs across many services. Traditional log monitoring is threshold-based ("alert if error rate > 5%"), which is slow to catch novel failure patterns and generates a lot of noise. This project builds a small but architecturally realistic system that:

1. Simulates several independent microservices producing logs continuously
2. Streams those logs through a real message broker (Kafka) instead of writing to flat files
3. Runs a machine learning model that scores incoming log windows for anomalousness in near-real-time
4. Persists metrics and anomaly scores to a time-series database
5. Visualizes everything on a live Grafana dashboard, with flagged anomalies highlighted
6. Is fully containerized, deployed to Kubernetes, and shipped through a proper CI/CD pipeline to AWS

The "multi-region" framing means the system is designed so log producers can represent services running in different regions/availability zones, and the pipeline aggregates and detects anomalies across all of them centrally — a realistic pattern for companies operating global infrastructure.

### 1.2 Why this project (motivation)
- **AIOps / observability** is a growing, well-compensated niche combining SWE + ML + infra — exactly the combination this project demonstrates.
- It touches **every** major piece of the modern cloud-native stack (containers, orchestration, cloud, CI/CD, streaming, ML) in one coherent system, rather than as disconnected toy examples.
- It produces a **demoable artifact** (a live dashboard showing real anomaly detection) — far more compelling in an interview than a script that prints to console.
- The ML component is genuinely substitutable/extensible (Isolation Forest → Autoencoder → other approaches), giving room to show iteration and experimentation.

### 1.3 Problem statement (the "elevator pitch")
> "I built a real-time log anomaly detection pipeline that ingests streaming logs from multiple simulated microservices via Kafka, uses an ML model to flag anomalous behavior in sliding time windows, and visualizes results on a live Grafana dashboard — all containerized, deployed to Kubernetes on AWS EKS, and shipped through a full CI/CD pipeline with GitHub Actions."

---

## 2. System Architecture

### 2.1 High-level data flow

```
 ┌─────────────────┐     ┌───────┐     ┌────────────────────┐     ┌───────────────────┐     ┌──────────────────┐
 │  Log Producers   │────▶│ Kafka │────▶│  Anomaly Detector   │────▶│  Time-Series DB   │────▶│  Grafana Dashboard│
 │ (simulated svcs) │     │(topic)│     │  (ML consumer svc)  │     │  (TimescaleDB)     │     │   (live charts)   │
 └─────────────────┘     └───────┘     └────────────────────┘     └───────────────────┘     └──────────────────┘
```

- **Log Producers** publish structured JSON log events (timestamp, service name, status code, latency, message) to a Kafka topic at a configurable rate, with an injectable anomaly mode for testing.
- **Kafka** decouples producers from consumers, buffers bursts, and allows multiple independent consumers to read the same stream (e.g. the anomaly detector and, later, a raw-log archiver).
- **Anomaly Detector** is a Kafka consumer that groups events into sliding time windows per service, extracts features, scores each window with an ML model, and writes results downstream.
- **Time-Series DB** stores both raw aggregated metrics and anomaly scores/flags, indexed by time and service.
- **Grafana** queries the database and renders live time-series charts, with anomaly points visually flagged and optional alerting rules.

### 2.2 Deployment architecture (the infra layer)

```
                          ┌─────────────────────────────────────────────────────────┐
                          │                      GitHub Actions                      │
                          │   test → build images → push to ECR → deploy to EKS      │
                          └───────────────────────────┬───────────────────────────────┘
                                                        │
                                                        ▼
 ┌───────────────────────────────────────────────────────────────────────────────────────┐
 │                                     AWS (EKS Cluster)                                    │
 │  ┌───────────────┐   ┌───────────────┐   ┌────────────────────┐   ┌──────────────────┐  │
 │  │ Log Producer   │   │ Kafka         │   │ Anomaly Detector    │   │ TimescaleDB +     │  │
 │  │ Deployments    │──▶│ StatefulSet   │──▶│ Deployment + HPA    │──▶│ Grafana Deployment│  │
 │  └───────────────┘   └───────────────┘   └────────────────────┘   └──────────────────┘  │
 │             ConfigMaps / Secrets · Services · Ingress · IAM roles for service accounts   │
 └───────────────────────────────────────────────────────────────────────────────────────┘
                     Images pulled from ECR · VPC with public/private subnets
```

### 2.3 Design principles
- **Local-first development.** Every component must run locally via Docker Compose before it's ported to Kubernetes. This keeps iteration fast and cheap (no AWS bill while debugging logic).
- **Stateless where possible.** Producers and the detector are stateless and horizontally scalable. Kafka and the database are the only stateful components (StatefulSets / persistent volumes).
- **Config via environment, not code.** All tunables (window size, anomaly thresholds, Kafka topic names, DB connection strings) come from environment variables / ConfigMaps, never hardcoded.
- **Observability is part of the product, not an afterthought.** The Grafana dashboard is a first-class deliverable, not a debugging tool bolted on at the end.

---

## 3. Repository Structure

```
log-anomaly-pipeline/
├── README.md                        # this file
├── docker-compose.yml               # local dev orchestration
├── .env.example                     # sample environment variables (no real secrets)
├── .github/
│   └── workflows/
│       ├── ci.yml                   # lint + test on every PR
│       └── cd.yml                   # build, push to ECR, deploy to EKS on merge to main
├── docs/
│   └── decisions/                   # Architecture Decision Records (ADRs)
├── services/
│   ├── log-producer/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   ├── producer.py
│   │   └── tests/
│   ├── anomaly-detector/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   ├── detector.py
│   │   ├── features.py
│   │   ├── models/                  # trained model artifacts
│   │   └── tests/
│   └── dashboard-provisioning/      # Grafana dashboard JSON + datasource configs
├── infra/
│   ├── k8s/
│   │   ├── base/                    # plain manifests (Deployments, Services, ConfigMaps)
│   │   └── overlays/
│   │       ├── local/                # Kind/Minikube overrides
│   │       └── prod/                 # EKS overrides
│   ├── helm/                        # Helm chart(s), once manifests are stable
│   └── aws/                         # VPC, EKS, ECR, IAM definitions (Terraform or CDK)
├── scripts/
│   ├── seed_anomalies.py            # injects known anomalies for testing/demo
│   └── benchmark.py                 # measures detection latency/accuracy
└── tests/
    └── integration/                 # end-to-end pipeline tests
```

---

## 4. Detailed Component Breakdown

### 4.1 Log Producers
Small, independently containerized Python services that simulate real microservices emitting logs. Each producer:
- Publishes structured JSON events to a Kafka topic (`logs.raw`) at a configurable events/second rate
- Emits realistic fields: `timestamp`, `service_name`, `status_code`, `latency_ms`, `message`, `region`
- Supports an `ANOMALY_MODE` environment variable to deliberately inject latency spikes, error bursts, or unusual status code distributions — this gives you labeled ground truth to validate the detector against
- Run 3 instances representing different services (`auth-service` in `us-east-1`, `payments-service` in `eu-west-1`, `inventory-service` in `ap-southeast-1`)
- Exposes a `/healthz` HTTP endpoint on port 8080 (AGENTS.md rule 6); host ports 8081–8083 are mapped for easy `curl` debugging

**Implementation:** `services/log-producer/producer.py` — single-file Python service with a daemon-thread HTTP health server, Kafka connection-retry loop (10 attempts × 5 s), and `build_event()` that switches behaviour based on `ANOMALY_MODE`. Multi-stage `Dockerfile` (python:3.11-slim, non-root `appuser`).

**What you'll learn:** structuring realistic synthetic data generators, Kafka producer clients (`kafka-python-ng`), daemon threads in Python, multi-stage Docker builds, environment-driven configuration.

### 4.2 Kafka
The streaming backbone. Producers publish, the anomaly detector (and potentially other consumers) subscribe.
- Single topic (`logs.raw`) with 3 partitions — partition count matches the number of producer services so the future detector can run one consumer thread per partition
- Consumer group semantics so multiple detector replicas split the partition load
- Runs via `apache/kafka:4.3.1` in **KRaft mode** (no ZooKeeper — Kafka 4.x removed it entirely). `KAFKA_PROCESS_ROLES=broker,controller` lets a single container fill both roles locally
- `PLAINTEXT_HOST://localhost:29092` listener exposed so you can run `kafka-console-consumer.sh` from your laptop to verify events are flowing

**What you'll learn:** topics, partitions, consumer groups, offset commits, at-least-once vs. exactly-once delivery tradeoffs, why stateful workloads need `StatefulSet` rather than `Deployment` in Kubernetes, KRaft vs ZooKeeper architecture.

### 4.3 Anomaly Detector
The ML core of the system. A Kafka consumer service that:
1. Reads raw log events from Kafka continuously
2. Buckets them into sliding time windows (e.g. 30-second windows, sliding every 10 seconds) per service
3. Extracts features per window: request count, error rate, p50/p95/p99 latency, status code distribution entropy
4. Scores each window's feature vector with a trained model
5. Writes `(timestamp, service_name, anomaly_score, is_anomalous)` to the time-series database

**Model approach (staged):**
- **v1 — Isolation Forest** (scikit-learn): fast to train, interpretable, good baseline. Train on a period of "normal" traffic, then score live windows.
- **v2 — Autoencoder** (PyTorch or TensorFlow): trained to reconstruct normal feature vectors; high reconstruction error = anomaly. More expressive, gives you a deep learning story alongside the classical baseline, and a natural "v1 vs v2" comparison section for your writeup.

**What you'll learn:** streaming feature engineering, sliding window aggregation, unsupervised anomaly detection, model serving inside a long-running consumer process, comparing classical ML vs. deep learning approaches on the same problem.

### 4.4 Time-Series Storage
TimescaleDB (a Postgres extension) stores:
- `metrics` hypertable: per-window aggregated stats per service
- `anomalies` hypertable: flagged anomalous windows with scores

Chosen over InfluxDB for this project because it's SQL-based (lower learning curve if you already know Postgres) while still being purpose-built for time-series workloads (automatic partitioning by time, efficient range queries).

**What you'll learn:** time-series data modeling, hypertables, retention policies, writing efficient time-range queries.

### 4.5 Grafana Dashboard
Connects to TimescaleDB and renders:
- Request rate and error rate per service over time
- Latency percentiles per service
- Anomaly score over time, with flagged points highlighted distinctly
- Optional Grafana alerting rule that fires when anomaly score crosses a threshold

**What you'll learn:** data source provisioning, dashboard-as-code (JSON dashboard definitions checked into the repo), basic alerting configuration.

### 4.6 Docker
Every service gets its own `Dockerfile`:
- Multi-stage builds (build dependencies in one stage, copy only what's needed into a slim runtime image)
- Non-root user in the final image
- `docker-compose.yml` orchestrates all services locally with proper `depends_on` and health checks, so `docker compose up` brings up the entire pipeline in one command

**What you'll learn:** multi-stage builds, image size optimization, container networking, Compose for multi-service local development.

### 4.7 Kubernetes
Once the system works via Compose, it's ported to Kubernetes manifests:
- **Deployments** for producers, the anomaly detector, and Grafana (stateless, horizontally scalable)
- **StatefulSet** for Kafka (and optionally TimescaleDB, if not using a managed DB) — needs stable identity and persistent volumes
- **Services** (ClusterIP internally; the Grafana dashboard exposed via a `LoadBalancer` or `Ingress`)
- **ConfigMaps/Secrets** for all configuration and credentials
- **Liveness/readiness probes** on every Deployment so Kubernetes can detect and restart unhealthy pods
- **Horizontal Pod Autoscaler (HPA)** on the anomaly detector — ideally scaled by Kafka consumer lag via **KEDA** (Kubernetes Event-Driven Autoscaling) rather than plain CPU, since a lagging consumer is the real signal that more replicas are needed

Start locally with **Kind** or **Minikube** — free, fast to iterate, no cloud cost while debugging YAML.

**What you'll learn:** Deployments vs. StatefulSets, Services, ConfigMaps/Secrets, health probes, autoscaling (including event-driven autoscaling via KEDA — a step beyond basic CPU-based HPA), Helm chart packaging.

### 4.8 AWS
Once manifests work locally, the same system deploys to **EKS**:
- **ECR** stores all container images; CI/CD pushes here
- **VPC** with public subnets (for load balancers) and private subnets (for worker nodes) — real networking, including NAT gateways and security groups
- **IAM Roles for Service Accounts (IRSA)** so pods get scoped AWS permissions instead of using long-lived credentials
- Optional: **MSK** (Managed Streaming for Kafka) instead of self-hosted Kafka, as a deliberate "self-hosted vs. managed" comparison you can speak to

**What you'll learn:** EKS cluster setup, ECR image publishing, VPC subnet design, IAM roles for Kubernetes workloads, and the operational tradeoffs between self-managed and managed infrastructure.

### 4.9 CI/CD (GitHub Actions)
Two workflows:
- **`ci.yml`** — runs on every pull request: lints code, runs unit tests (including a test that asserts the detector correctly flags a known synthetic anomaly), builds Docker images to verify they compile
- **`cd.yml`** — runs on merge to `main`: builds and tags images, pushes to ECR, deploys to EKS (via `kubectl apply` against the `infra/k8s/overlays/prod` manifests, or a Helm upgrade), with an optional smoke test against a staging namespace before promoting

**What you'll learn:** designing multi-stage pipelines, environment-specific deployment configs (overlays), automated testing gates before deployment, secrets management in CI (GitHub Actions secrets → AWS credentials).

---

## 5. Local Development Setup

### 5.1 Prerequisites
- Docker Desktop (or Docker Engine + Compose plugin)
- Python 3.11+
- `kubectl` and `kind` (or `minikube`) for the Kubernetes stage
- An AWS account (only needed once you reach the cloud deployment stage)

### 5.2 Quick start (local, Docker Compose)
```bash
cp .env.example .env
docker compose up --build
```
This should bring up: log producers, Kafka (+ Zookeeper or KRaft mode), the anomaly detector, TimescaleDB, and Grafana. Grafana will be reachable at `http://localhost:3000`.

### 5.3 Injecting a test anomaly
```bash
python scripts/seed_anomalies.py --service payments-service --duration 60
```
This temporarily spikes latency/error rate for the named service so you can confirm the detector flags it and the dashboard reflects it.

---

## 6. Build Roadmap

| Phase | Goal | Environment |
|---|---|---|
| 1 | Producers publishing to Kafka, visible via console consumer | Docker Compose |
| 2 | Anomaly detector consuming from Kafka, printing scores to console | Docker Compose |
| 3 | Add TimescaleDB + Grafana, full local pipeline working end-to-end | Docker Compose |
| 4 | Port all services to Kubernetes manifests, same functionality | Kind / Minikube |
| 5 | Add CI pipeline (lint, test, build) | GitHub Actions |
| 6 | Provision AWS infra (VPC, EKS, ECR) and deploy manually once | AWS |
| 7 | Add CD pipeline (auto-deploy to EKS on merge) | GitHub Actions → AWS |
| 8 | Add HPA/KEDA autoscaling, polish dashboard, write up results | AWS |

---

## 7. Testing Strategy
- **Unit tests** for feature extraction (`features.py`) and detector scoring logic, using fixed synthetic input/output pairs
- **Integration test** that spins up the full Compose stack, injects a known anomaly via `seed_anomalies.py`, and asserts an anomaly row appears in the database within a bounded time window
- **CI gate**: no image is built/pushed unless unit tests pass

---

## 8. Environment Variables (see `.env.example` for the full list)

| Variable | Purpose |
|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address(es) |
| `KAFKA_TOPIC` | Topic name for raw log events |
| `WINDOW_SECONDS` | Sliding window size for feature aggregation |
| `WINDOW_SLIDE_SECONDS` | How often a new window is emitted |
| `ANOMALY_MODEL_PATH` | Path to the trained model artifact |
| `TIMESCALEDB_URL` | Connection string for the time-series database |
| `GRAFANA_ADMIN_PASSWORD` | Local dev only — never commit a real value |

---

## 9. Skills This Project Demonstrates (for resume / interview framing)

- **Streaming systems:** Kafka producers/consumers, partitioning, consumer groups
- **Applied ML:** unsupervised anomaly detection, streaming feature engineering, classical (Isolation Forest) vs. deep learning (autoencoder) comparison
- **Containers:** multi-stage Docker builds, Compose orchestration
- **Kubernetes:** Deployments, StatefulSets, ConfigMaps/Secrets, health probes, HPA/KEDA event-driven autoscaling, Helm
- **Cloud infrastructure:** AWS EKS, ECR, VPC networking, IAM roles for service accounts
- **CI/CD:** multi-stage GitHub Actions pipelines, environment-specific deployment overlays, automated testing gates
- **Observability:** Grafana dashboard-as-code, time-series data modeling

---

## 10. Future Enhancements (optional, post-MVP)
- Swap self-hosted Kafka for AWS MSK and document the tradeoffs
- Add a second AWS region and actually federate anomaly detection across regions (matching the "multi-region" name more literally)
- Add Prometheus for infrastructure-level metrics (pod CPU/memory) alongside the application-level anomaly metrics already in Grafana
- Add a simple alerting integration (Slack webhook) when a high-confidence anomaly is detected
- Chaos-test the pipeline itself (kill the detector mid-processing) and verify no events are lost, to demonstrate resilience

---

## 11. License
MIT (or your preference — update before publishing).
