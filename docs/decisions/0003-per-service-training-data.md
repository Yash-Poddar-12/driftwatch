# ADR 0003 — Per-Service Training Data to Fix Systematic False Positives

**Date:** 2026-08-18
**Status:** Accepted
**Author:** Antigravity / Claude Sonnet 4.6

---

## Context

After deploying the Phase 2 anomaly detector, live observation revealed a
systematic false-positive rate that varied significantly by service:

| Service | `EVENTS_PER_SECOND` | Observed false-positive rate (normal mode) |
|---|---|---|
| auth-service | 5 | ~5 % (matches `contamination=0.05` target) |
| payments-service | 3 | **~33 %** ← problem |
| inventory-service | 4 | ~0 % |

The issue was **not** flakiness or a model quality problem in general — it was
a systematic bias specific to payments-service running in normal mode.

## Root Cause

`train.py`'s `generate_training_data()` called `_generate_normal_window()` with
a hardcoded `window_size=150` for every synthetic sample:

```python
# BEFORE (buggy):
def _generate_normal_window(rng, service_name="synthetic-service", window_size=150, now=0.0):
    n = max(10, int(rng.gauss(window_size, window_size * 0.2)))
    ...
```

`window_size=150` corresponds to `5 ev/s × 30 s = 150 events` — which is
correct only for **auth-service**.  The other two services run at:

- `payments-service`: `3 ev/s × 30 s = 90 events/window` → 40 % fewer events than training average
- `inventory-service`: `4 ev/s × 30 s = 120 events/window` → 20 % fewer events than training average

Isolation Forest built a decision boundary centred on `request_count ≈ 150`.
Payments-service windows consistently land at `request_count ≈ 90`, which
sits in the outer 33rd percentile of the training distribution's
`request_count` dimension — triggering the anomaly flag even with perfectly
normal latency and error rates.

Inventory-service at 120 events happened to fall within the model's 20 % noise
band (Gaussian ±20 % ≈ ±30 events → [120, 180]) and so was not flagged.

## Decision: One Shared Model with Per-Service Training Windows

Two architecturally valid fixes were considered:

### Option A — Three separate models (one per service)
Train and save `isolation_forest_auth.joblib`,
`isolation_forest_payments.joblib`, `isolation_forest_inventory.joblib`.
At runtime, `detector.py` loads the model matching the window's `service_name`.

**Pros:** Each model is tuned exactly to one service's distribution; adding a
new service is easy (just train a new model file).

**Cons:**
- Three model files to maintain, build, and load.
- `detector.py` needs a dispatch table (service → model path); more code.
- If a new service is introduced, deployment fails silently if no model file
  exists for it.
- Adds complexity without meaningful accuracy benefit here: all three services
  share the same latency and error-rate distributions — only `request_count`
  differs.

### Option B — One model, mixed per-service training windows ✅ Chosen

Generate synthetic training windows for each service using their actual
`EVENTS_PER_SECOND` from `docker-compose.yml`, then train a single
`IsolationForest` on the union.  Round-robin sampling gives each service
equal representation (2 000 windows each out of 6 000 total).

**Pros:**
- Single model file — simpler build, runtime, and deployment.
- The model learns a multi-modal `request_count` distribution: three clusters
  at ~90, ~120, and ~150 events/window.  Isolation Forest handles multi-modal
  distributions well because it uses random feature splits rather than a
  single global centroid.
- `detector.py` is unchanged — it scores every window with the same model
  regardless of service name.

**Cons:**
- `train.py` must be updated whenever a producer's `EVENTS_PER_SECOND` changes
  (same is true of Option A).
- The model's `request_count` decision boundary is less tight per-service than
  a dedicated model.  In practice this increases false-negative tolerance at
  the extremes slightly, but for a 6-feature space this is acceptable.

**Why Option B wins here:**  The features that actually carry anomaly signal
(error rate, latency percentiles, entropy) are identical across all three
services.  Only `request_count` differs, and that difference is a known,
stable quantity from `docker-compose.yml`.  One model that knows about all
three normal operating points is operationally simpler than three models for
a gain that would only matter if each service had fundamentally different
latency or error-rate baselines (which they don't).

## Implementation

`ServiceProfile` namedtuple added to `train.py` capturing `(name, events_per_second)`.
`SERVICE_PROFILES` list mirrors `docker-compose.yml` exactly.
`_generate_normal_window()` now accepts a `ServiceProfile` and computes
`base_count = events_per_second × 30` as the window's expected event count.
`generate_training_data()` round-robins across profiles for equal mix.
`N_SAMPLES` bumped from 5 000 → 6 000 to be evenly divisible by 3 services.

## Consequences

- **Positive:** payments-service false-positive rate drops from ~33 % to the
  target ~5 % (matching `contamination=0.05`).
- **Positive:** Future model accuracy is coupled to `docker-compose.yml` via
  `SERVICE_PROFILES` — the link between infra config and training config is
  now explicit and documented.
- **Negative (operational):** If a producer's `EVENTS_PER_SECOND` changes, or
  a new producer is added, `SERVICE_PROFILES` in `train.py` must be updated
  and the model retrained.  This is a conscious tradeoff — we chose a simpler
  runtime at the cost of a tighter coupling between `train.py` and the Compose
  config.
- **Future work:** Phase 5 (CI) should add a step that validates `SERVICE_PROFILES`
  in `train.py` against `EVENTS_PER_SECOND` values parsed from
  `docker-compose.yml`, failing the build if they diverge.
