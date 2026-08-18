"""
tests/test_detector.py — Unit tests for features.py and detector.py scoring.

What we test (AGENTS.md §7 — tests before infra):
  1. Feature extraction correctness on hand-crafted event lists.
  2. A trained model correctly scores a clearly anomalous window as anomalous
     and a clearly normal window as normal.
  3. Edge cases: empty window, single-event window.

Design notes:
- Tests are fully offline: no Kafka, no running Docker stack required.
- The model is trained fresh inside the test session using train.generate_training_data()
  and a fixed seed so results are deterministic.
- "Clearly anomalous" means:
    - Latency 20–40× above normal (latency_spike mode in producer.py)
    - Error rate 80 % (error_burst mode in producer.py)
  This gives a very large feature-space distance from the normal cluster and
  makes the test robust even if contamination / hyperparameters shift slightly.
"""

from __future__ import annotations

import math
import sys
from pathlib import Path

import numpy as np
import pytest

# Make the service source importable from the tests directory.
_SRC = Path(__file__).parent.parent / "services" / "anomaly-detector"
sys.path.insert(0, str(_SRC))

from features import (  # noqa: E402  (import after path manipulation)
    SlidingWindowAccumulator,
    WindowFeatures,
    _percentile,
    _status_entropy,
    extract_features,
)
from train import generate_training_data  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_events(
    latencies: list[float],
    status_codes: list[int],
    service: str = "test-svc",
    now: float = 1_000_000.0,
) -> list[dict]:
    return [
        {"latency_ms": lat, "status_code": code, "service_name": service, "timestamp": now}
        for lat, code in zip(latencies, status_codes)
    ]


def _fit_model(n_samples: int = 3000, seed: int = 42):
    """Train a model inline (no file I/O) for use in tests."""
    from sklearn.ensemble import IsolationForest
    X = generate_training_data(n_samples=n_samples, seed=seed)
    clf = IsolationForest(
        n_estimators=100,
        max_samples="auto",
        contamination=0.05,
        random_state=seed,
    )
    clf.fit(X)
    return clf


# ---------------------------------------------------------------------------
# 1. Percentile helper
# ---------------------------------------------------------------------------

class TestPercentile:
    def test_p50_odd(self):
        assert _percentile([1.0, 2.0, 3.0], 50) == 2.0

    def test_p50_even(self):
        # nearest-rank: ceil(50/100 * 4) - 1 = 1 → sorted[1] = 2
        assert _percentile([4.0, 2.0, 1.0, 3.0], 50) == 2.0

    def test_p95(self):
        vals = list(range(1, 101))  # 1..100
        assert _percentile(vals, 95) == 95

    def test_p99(self):
        vals = list(range(1, 101))
        assert _percentile(vals, 99) == 99

    def test_empty(self):
        assert _percentile([], 50) == 0.0


# ---------------------------------------------------------------------------
# 2. Status entropy
# ---------------------------------------------------------------------------

class TestStatusEntropy:
    def test_single_code_zero_entropy(self):
        """All events with the same status code → entropy = 0."""
        assert _status_entropy([200] * 100) == pytest.approx(0.0)

    def test_two_equal_codes(self):
        """Two equally probable codes → entropy = ln(2) ≈ 0.693."""
        entropy = _status_entropy([200, 500] * 50)
        assert entropy == pytest.approx(math.log(2), abs=1e-9)

    def test_empty(self):
        assert _status_entropy([]) == 0.0

    def test_higher_than_single(self):
        """More diverse distribution should have higher entropy."""
        low = _status_entropy([200] * 90 + [500] * 10)
        high = _status_entropy([200, 201, 204, 400, 404, 500] * 10)
        assert high > low


# ---------------------------------------------------------------------------
# 3. extract_features
# ---------------------------------------------------------------------------

class TestExtractFeatures:
    def test_normal_window_basic(self):
        latencies = [80.0] * 90 + [200.0] * 9 + [500.0]   # 100 events
        status_codes = [200] * 98 + [500, 500]              # 2 % error rate
        wf = extract_features("svc", 1_000.0, _make_events(latencies, status_codes))
        assert wf.request_count == 100
        assert wf.error_rate == pytest.approx(0.02)
        assert wf.p50_latency_ms == 80.0
        assert wf.p95_latency_ms == 200.0
        assert wf.status_entropy > 0.0

    def test_empty_window(self):
        wf = extract_features("svc", 1_000.0, [])
        assert wf.request_count == 0.0
        assert wf.error_rate == 0.0
        assert wf.p50_latency_ms == 0.0
        assert wf.status_entropy == 0.0

    def test_single_event(self):
        events = _make_events([120.0], [200])
        wf = extract_features("svc", 1_000.0, events)
        assert wf.request_count == 1.0
        assert wf.error_rate == 0.0
        assert wf.p50_latency_ms == 120.0

    def test_error_burst(self):
        """80 % 5xx events → error_rate ≈ 0.80."""
        n = 100
        codes = [500] * 80 + [200] * 20
        events = _make_events([50.0] * n, codes)
        wf = extract_features("svc", 1_000.0, events)
        assert wf.error_rate == pytest.approx(0.80)


# ---------------------------------------------------------------------------
# 4. SlidingWindowAccumulator
# ---------------------------------------------------------------------------

class TestSlidingWindowAccumulator:
    def test_emit_returns_per_service(self):
        acc = SlidingWindowAccumulator(window_seconds=30, slide_seconds=10)
        now = 1_000.0
        for svc in ["auth", "payments", "inventory"]:
            acc.add_event(
                {"service_name": svc, "timestamp": now, "latency_ms": 80.0, "status_code": 200}
            )
        windows = acc.emit_windows(now)
        service_names = {w.service_name for w in windows}
        assert service_names == {"auth", "payments", "inventory"}

    def test_pruning_old_events(self):
        acc = SlidingWindowAccumulator(window_seconds=30, slide_seconds=10)
        base = 1_000.0
        # Add one old event (60 s before now) and one recent.
        acc.add_event({"service_name": "svc", "timestamp": base - 60, "latency_ms": 80.0, "status_code": 200})
        acc.add_event({"service_name": "svc", "timestamp": base, "latency_ms": 80.0, "status_code": 200})
        windows = acc.emit_windows(base)
        # Only the recent event survives the 30 s window.
        svc_window = next(w for w in windows if w.service_name == "svc")
        assert svc_window.request_count == 1.0

    def test_should_emit_timing(self):
        acc = SlidingWindowAccumulator(window_seconds=30, slide_seconds=10)
        now = 1_000.0
        acc.emit_windows(now)
        assert not acc.should_emit(now + 5)
        assert acc.should_emit(now + 10)


# ---------------------------------------------------------------------------
# 5. Model scoring: normal vs. anomalous windows
# ---------------------------------------------------------------------------

class TestModelScoring:
    """
    These tests train a fresh IsolationForest in-process and verify that
    clearly anomalous feature vectors score differently from normal ones.
    """

    @pytest.fixture(scope="class")
    def model(self):
        return _fit_model()

    def _score(self, model, wf: WindowFeatures) -> tuple[float, bool]:
        """Return (decision_function_score, is_anomalous)."""
        import numpy as np
        from detector import score_windows
        results = score_windows(model, [wf])
        r = results[0]
        return r["anomaly_score"], r["is_anomalous"]

    def test_normal_window_is_not_anomalous(self, model):
        """
        A window that looks exactly like normal producer output should NOT
        be flagged as anomalous.
        """
        # Realistic normal window: ~150 events, low error rate, normal latencies.
        import random
        rng = random.Random(99)
        latencies = [max(1.0, rng.lognormvariate(4.4, 0.5)) for _ in range(150)]
        codes = [200] * 135 + [201] * 8 + [204] * 4 + [400] * 2 + [404] + [500]
        events = _make_events(latencies, codes)
        wf = extract_features("svc", 1_000.0, events)
        _, is_anomalous = self._score(model, wf)
        # A perfectly normal window should not be flagged.
        assert not is_anomalous, (
            f"Normal window was incorrectly flagged as anomalous: {wf.to_model_input()}"
        )

    def test_latency_spike_is_anomalous(self, model):
        """
        A window with 20-40× normal latency (latency_spike mode) should be
        flagged as anomalous.
        """
        import random
        rng = random.Random(7)
        # Latency spike: multiply normal latency by 30×.
        latencies = [max(1.0, rng.lognormvariate(4.4, 0.5)) * 30 for _ in range(150)]
        codes = [200] * 148 + [500, 500]
        events = _make_events(latencies, codes)
        wf = extract_features("svc", 1_000.0, events)
        score, is_anomalous = self._score(model, wf)
        assert is_anomalous, (
            f"Latency-spike window was NOT flagged as anomalous. "
            f"score={score:.4f}, features={wf.to_model_input()}"
        )

    def test_error_burst_is_anomalous(self, model):
        """
        A window with 80 % 5xx errors (error_burst mode) should be flagged
        as anomalous.
        """
        import random
        rng = random.Random(13)
        latencies = [max(1.0, rng.lognormvariate(4.4, 0.5)) for _ in range(150)]
        codes = [500] * 120 + [200] * 30   # 80 % error rate
        events = _make_events(latencies, codes)
        wf = extract_features("svc", 1_000.0, events)
        score, is_anomalous = self._score(model, wf)
        assert is_anomalous, (
            f"Error-burst window was NOT flagged as anomalous. "
            f"score={score:.4f}, features={wf.to_model_input()}"
        )

    def test_anomaly_score_ordering(self, model):
        """
        The raw anomaly score for a latency-spike window must be lower
        (more anomalous) than for a normal window.
        (Isolation Forest: lower decision_function → more anomalous.)
        """
        import random

        # Normal window.
        rng_n = random.Random(5)
        lat_n = [max(1.0, rng_n.lognormvariate(4.4, 0.5)) for _ in range(150)]
        codes_n = [200] * 140 + [201] * 6 + [500] * 4
        wf_normal = extract_features("svc", 1_000.0, _make_events(lat_n, codes_n))

        # Anomalous window (extreme latency spike).
        rng_a = random.Random(6)
        lat_a = [max(1.0, rng_a.lognormvariate(4.4, 0.5)) * 35 for _ in range(150)]
        codes_a = [200] * 148 + [500, 500]
        wf_anomalous = extract_features("svc", 1_000.0, _make_events(lat_a, codes_a))

        from detector import score_windows
        r_n = score_windows(model, [wf_normal])[0]
        r_a = score_windows(model, [wf_anomalous])[0]

        assert r_a["anomaly_score"] < r_n["anomaly_score"], (
            f"Expected anomalous score ({r_a['anomaly_score']:.4f}) < "
            f"normal score ({r_n['anomaly_score']:.4f})"
        )
