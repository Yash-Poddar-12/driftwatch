"""
train.py — Offline Isolation Forest training for the anomaly detector.

Design notes (AGENTS.md §1 — explain every choice):

1.  Why train offline, not live?
    The detector needs a pre-fitted model at startup so it can score windows
    immediately without a warm-up period.  Training live would require
    accumulating enough "known normal" windows first — complicating the
    consumer loop and delaying detection.  Offline training is the standard
    pattern for unsupervised anomaly detection in production (train once,
    score continuously).

2.  Why Isolation Forest (not e.g. One-Class SVM or LOF)?
    See ADR: docs/decisions/0001-isolation-forest-v1.md.
    Short answer: sub-linear scoring time (O(log n) per sample), no distance
    metric needed, works well in low-feature-count regimes like ours (6 features),
    and ships with scikit-learn — no additional inference runtime to manage.

3.  Why synthetic training data?
    We do not have weeks of historical "normal" traffic because this is a brand-
    new system.  The producer's normal-traffic distributions are fully documented
    in producer.py (lognormal latency, known status-code weights).  We replicate
    those distributions here faithfully — the resulting training set is
    statistically identical to what a live normal run would produce.
    See ADR docs/decisions/0002-synthetic-training-data.md for the full rationale.

4.  Reproducibility:
    RANDOM_SEED controls numpy + sklearn RNGs.  Set via env var so CI runs are
    deterministic, and the owner can pass a different seed for experiments.

5.  Model artefact:
    Saved with joblib to models/isolation_forest_v1.joblib.  joblib is faster
    and more space-efficient than pickle for numpy-heavy sklearn objects.
    The filename includes "v1" so future model versions live side-by-side without
    overwriting — swapping to v2 is an env-var change in detector.py, not a
    file rename.

6.  contamination parameter:
    Set to 0.05 (5 %).  This tells Isolation Forest to treat 5 % of the
    training samples as if they were anomalies when setting the decision
    boundary.  In practice our synthetic training data is pure normal traffic,
    so this adds a small safety margin without introducing false positives on
    real normal windows.

7.  Per-service window sizes (BUG FIX — see ADR 0003):
    Each service has a different EVENTS_PER_SECOND in docker-compose.yml:
      auth-service      5 ev/s → ~150 events per 30 s window
      payments-service  3 ev/s →  ~90 events per 30 s window
      inventory-service 4 ev/s → ~120 events per 30 s window
    The original code used window_size=150 for every synthetic sample,
    matching only auth-service.  Payments-service windows (~90 events) landed
    in the low tail of the request_count distribution the model learned,
    causing ~33 % false-positive rate for that service in normal operation.
    Fix: generate training windows for each service using their actual event
    rate, and mix all three services equally in the training corpus.  One
    shared model (not three separate ones) — see ADR 0003 for the tradeoff.
"""

from __future__ import annotations

import os
import random
import time
from pathlib import Path
from typing import NamedTuple

import joblib
import numpy as np
from sklearn.ensemble import IsolationForest

from features import WindowFeatures, extract_features

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

RANDOM_SEED: int = int(os.environ.get("RANDOM_SEED", "42"))
N_SAMPLES: int = int(os.environ.get("N_TRAIN_SAMPLES", "6000"))  # divisible by 3 services
WINDOW_SECONDS: float = 30.0   # must match WINDOW_SECONDS in docker-compose.yml
MODELS_DIR: Path = Path(__file__).parent / "models"
MODEL_PATH: Path = MODELS_DIR / "isolation_forest_v1.joblib"

# Isolation Forest hyperparameters.
N_ESTIMATORS: int = 100     # number of trees; 100 is the sklearn default and good baseline
MAX_SAMPLES: str | int = "auto"  # "auto" → min(256, n_samples); fast and generalises well
CONTAMINATION: float = 0.05     # assumed anomaly fraction in training data

# ---------------------------------------------------------------------------
# Per-service traffic profiles — mirrors docker-compose.yml exactly.
# ---------------------------------------------------------------------------

class ServiceProfile(NamedTuple):
    name: str
    events_per_second: float  # from EVENTS_PER_SECOND env var in docker-compose.yml

# These must be kept in sync with the SERVICE_NAME / EVENTS_PER_SECOND values
# in docker-compose.yml (log-producer-auth, log-producer-payments,
# log-producer-inventory).  If a new producer is added, add it here too.
SERVICE_PROFILES: list[ServiceProfile] = [
    ServiceProfile("auth-service",      events_per_second=5.0),
    ServiceProfile("payments-service",  events_per_second=3.0),
    ServiceProfile("inventory-service", events_per_second=4.0),
]

# ---------------------------------------------------------------------------
# Synthetic normal-traffic distributions (mirror producer.py exactly)
# ---------------------------------------------------------------------------

# Status code weights copied verbatim from producer.py NORMAL_STATUS_WEIGHTS.
_STATUS_CODES = [200, 201, 204, 400, 404, 500]
_STATUS_WEIGHTS = [0.88, 0.05, 0.03, 0.02, 0.01, 0.01]
_STATUS_WEIGHTS_CUM: list[float] = []
_cum = 0.0
for _w in _STATUS_WEIGHTS:
    _cum += _w
    _STATUS_WEIGHTS_CUM.append(_cum)


def _sample_status_code(rng: random.Random) -> int:
    """Weighted random status code from the same distribution as producer.py."""
    r = rng.random()
    for code, cum in zip(_STATUS_CODES, _STATUS_WEIGHTS_CUM):
        if r <= cum:
            return code
    return _STATUS_CODES[-1]  # fallback (floating-point edge)


def _sample_latency_ms(rng: random.Random) -> float:
    """
    Log-normal latency with mu=4.4, sigma=0.5 — same as producer.py
    _normal_latency_ms().  median ≈ e^4.4 ≈ 81 ms.
    """
    return max(1.0, rng.lognormvariate(4.4, 0.5))


def _generate_normal_window(
    rng: random.Random,
    service: ServiceProfile,
    window_seconds: float = WINDOW_SECONDS,
    now: float = 0.0,
) -> WindowFeatures:
    """
    Build one synthetic normal-traffic window for a specific service.

    window_size (expected event count) = events_per_second × window_seconds.
    We vary it by ±20 % (Gaussian) to simulate natural throughput variance
    across 30-second windows.

    Why per-service window_size?
    Each producer emits at a different rate (3 / 4 / 5 ev/s).  A model
    trained only on 150-event windows will flag 90-event windows (payments)
    as anomalous in the request_count dimension even when every other feature
    is perfectly normal.  See train.py docstring note 7 and ADR 0003.
    """
    base_count = service.events_per_second * window_seconds
    n = max(10, int(rng.gauss(base_count, base_count * 0.2)))
    events = [
        {
            "latency_ms": _sample_latency_ms(rng),
            "status_code": _sample_status_code(rng),
            "service_name": service.name,
            "timestamp": now,
        }
        for _ in range(n)
    ]
    return extract_features(service.name, now, events)


def generate_training_data(
    n_samples: int = N_SAMPLES,
    seed: int = RANDOM_SEED,
    window_seconds: float = WINDOW_SECONDS,
    profiles: list[ServiceProfile] = SERVICE_PROFILES,
) -> np.ndarray:
    """
    Generate n_samples synthetic normal-traffic feature vectors, sampling
    each service's realistic event rate in equal proportion.

    Returns an (n_samples, 6) float64 array where the 6 columns are:
        [request_count, error_rate, p50_ms, p95_ms, p99_ms, status_entropy]

    Equal mix across all services means the model learns the union of all
    normal operating regions rather than over-fitting to whichever service
    happens to produce the most events.  See ADR 0003.
    """
    rng = random.Random(seed)
    rows: list[list[float]] = []
    now = time.time()
    n_profiles = len(profiles)
    for i in range(n_samples):
        # Round-robin across services → equal representation.
        svc = profiles[i % n_profiles]
        wf = _generate_normal_window(rng, service=svc, window_seconds=window_seconds, now=now)
        rows.append(wf.to_model_input())
    return np.array(rows, dtype=np.float64)


# ---------------------------------------------------------------------------
# Training
# ---------------------------------------------------------------------------

def train(
    n_samples: int = N_SAMPLES,
    seed: int = RANDOM_SEED,
    save_path: Path = MODEL_PATH,
) -> IsolationForest:
    """
    Train an Isolation Forest on per-service synthetic normal-traffic data
    and save it.

    Parameters
    ----------
    n_samples : int
        Total number of synthetic normal windows to train on (split equally
        across all SERVICE_PROFILES).
    seed : int
        RNG seed for reproducibility.
    save_path : Path
        Where to persist the trained model artefact.

    Returns
    -------
    IsolationForest
        The fitted model (also persisted to save_path).
    """
    print(f"[train] Generating {n_samples} synthetic normal-traffic windows "
          f"({n_samples // len(SERVICE_PROFILES)} per service) ...")
    for svc in SERVICE_PROFILES:
        base = svc.events_per_second * WINDOW_SECONDS
        print(f"[train]   {svc.name}: {svc.events_per_second} ev/s "
              f"-> ~{base:.0f} events/window (+/-20 %)")

    X = generate_training_data(n_samples=n_samples, seed=seed)
    print(f"[train] Training data shape: {X.shape}")
    print(f"[train] Feature stats (mean across all services):\n"
          f"  request_count={X[:, 0].mean():.1f}, error_rate={X[:, 1].mean():.4f},\n"
          f"  p50={X[:, 2].mean():.1f} ms, p95={X[:, 3].mean():.1f} ms,\n"
          f"  p99={X[:, 4].mean():.1f} ms, status_entropy={X[:, 5].mean():.4f}")

    clf = IsolationForest(
        n_estimators=N_ESTIMATORS,
        max_samples=MAX_SAMPLES,
        contamination=CONTAMINATION,
        random_state=seed,
        n_jobs=-1,    # use all available cores — fast even on a laptop
    )
    clf.fit(X)
    print("[train] Isolation Forest trained.")

    save_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(clf, save_path)
    print(f"[train] Model saved to {save_path}")
    return clf


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    train()
