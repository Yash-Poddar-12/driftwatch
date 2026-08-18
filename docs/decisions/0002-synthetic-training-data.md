# ADR 0002 — Synthetic Training Data for the Anomaly Detector

**Date:** 2026-08-18
**Status:** Accepted
**Author:** Antigravity / Claude Sonnet 4.6

---

## Context

The Isolation Forest needs a corpus of "normal" feature vectors to train on.
We have three options:

1. **Live capture:** run the Compose stack for N hours in normal mode, capture
   windows from Kafka, use those as training data.
2. **Synthetic generation:** replicate producer.py's statistical distributions
   offline to generate artificial "normal" windows without any Kafka dependency.
3. **Hybrid:** a small synthetic seed + online retraining against live traffic.

The system is brand-new, so option 1 requires a human to run the stack and
wait. Option 3 adds significant complexity and an online-learning component
that is out of scope for Phase 2.

## Decision

Use **synthetic generation** (option 2): `train.py` generates 5 000 synthetic
normal-traffic windows using the exact same statistical parameters documented
in `producer.py`:

- Latency: `random.lognormvariate(mu=4.4, sigma=0.5)` — median ≈ 81 ms.
- Status codes: weighted sample from `{200: 88 %, 201: 5 %, 204: 3 %, 400: 2 %, 404: 1 %, 500: 1 %}`.
- Events per window: Gaussian around 150 (± 20 %) to simulate natural
  throughput variance across 30-second windows at ~5 events/s.

The training parameters are intentionally coupled to `producer.py`. If the
producer's distributions change, `train.py` must be updated and `train.py`
re-run to retrain the model.

### Why synthetic is acceptable here

- The producer distributions are **explicitly documented** and **deterministic
  given a seed**. A synthetic dataset is reproducible in CI; a live-captured
  dataset is not (it depends on when/how long the stack ran).
- The 6-feature space is low-dimensional. With only 6 features and clear
  normal distributions, 5 000 synthetic samples is more than enough for
  Isolation Forest to learn the decision boundary. The model is not
  interpolating between sparse data points — it is fitting a tree ensemble
  over a well-characterised distribution.
- **Drift warning:** if real traffic deviates significantly from the synthetic
  distributions (e.g. a new producer with different rates is added), the model
  should be retrained. The plan for Phase 5 (CI) includes a retraining step
  triggered by `train.py` in the CI pipeline.

## Consequences

- **Positive:** Training is fully offline, reproducible, requires no Kafka
  connection, and completes in < 5 s. The baked-into-Docker-image approach
  means every container tag ships with a deterministic model.
- **Negative:** The model is only as good as the fidelity of the synthetic
  distributions to real traffic. A significantly different real-world traffic
  pattern could cause elevated false positives until a retrain.
- **Mitigation:** The `contamination=0.05` hyperparameter (see ADR 0001)
  provides a 5 % buffer. Additionally, `scripts/seed_anomalies.py` (Phase 3)
  will provide a standard anomaly injection tool to validate detection recall
  on a fresh model without needing production traffic.
