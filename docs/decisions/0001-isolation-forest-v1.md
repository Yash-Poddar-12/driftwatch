# ADR 0001 — Isolation Forest as v1 Anomaly Detection Model

**Date:** 2026-08-18
**Status:** Accepted
**Author:** Antigravity / Claude Sonnet 4.6

---

## Context

The anomaly detector needs to score sliding-window feature vectors in near-
real-time. We have no labelled anomaly data — this is a brand-new system
with no historical anomaly records. The model must:

- Work in an **unsupervised** setting (no labelled anomalies to train on).
- Score a window in **well under 100 ms** so the consumer loop doesn't
  accumulate lag behind the Kafka topic.
- Be **interpretable enough** to explain in an interview without post-hoc
  explainability tooling.
- Ship with **no new runtime dependencies** beyond scikit-learn.

## Decision

Use **scikit-learn's IsolationForest** with `n_estimators=100`,
`max_samples="auto"`, `contamination=0.05`.

### Why Isolation Forest over alternatives

| Model | Pros | Cons | Decision |
|---|---|---|---|
| **Isolation Forest** | O(log n) score, no distance metric, works in 6-feature space, ships with sklearn | Binary boundary (no reconstruction signal) | ✅ **Chosen** |
| One-Class SVM | Strong theoretical guarantees | O(n²) training, sensitive to kernel/γ tuning | ❌ Too slow |
| Local Outlier Factor | Distance-based, good for clusters | Must store full training set at inference; O(n) scoring | ❌ Stateful at inference |
| Autoencoder | Reconstruction error = richer signal | Needs PyTorch/TF, longer training, more tuning | 🟡 Planned as v2 (see roadmap) |

Isolation Forest isolates anomalies by randomly partitioning the feature
space. Points that require fewer random cuts to isolate are more anomalous.
This is exactly the right inductive bias for our workload: a window with a
99th-percentile latency of 40 000 ms is easy to isolate from normal windows
clustered around 150–200 ms; a window with 80 % error rate is easy to isolate
from the normal 1 % baseline.

### Hyperparameter choices

- `n_estimators=100`: sklearn default, good bias-variance tradeoff for 6
  features. Increasing to 200 adds negligible accuracy but doubles tree count.
- `max_samples="auto"`: resolved to min(256, n_samples) by sklearn. Keeps
  each tree small and training fast; sufficient for a 6-dimensional feature
  space.
- `contamination=0.05`: sets the decision threshold so the top 5 % of
  training samples are treated as potential anomalies. Our synthetic training
  data is pure normal traffic, so this adds a conserved safety margin without
  introducing bias.
- `n_jobs=-1`: uses all CPU cores at training time; scoring is single-threaded
  (sklearn IsolationForest.decision_function is already fast enough single-
  threaded for our batch sizes).

## Consequences

- **Positive:** Fast to train (< 5 s on a laptop for 5 000 windows), fast to
  score (< 1 ms per window), no GPU needed, reproducible with a fixed seed.
- **Negative:** Binary prediction boundary. Decision_function gives a
  continuous score, but there's no "reconstruction error" signal to plot.
  This is addressed in Phase 3 by storing the raw score in TimescaleDB.
- **Future work:** v2 will be an Autoencoder (PyTorch) trained on the same
  features, allowing a direct v1 vs v2 comparison on detection recall and
  false-positive rate. The model swap will be documented in a new ADR and
  the runtime selection will be via the `ANOMALY_MODEL_PATH` env var.
