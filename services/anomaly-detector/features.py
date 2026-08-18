"""
features.py — Sliding-window feature extraction for the anomaly detector.

Design notes (AGENTS.md §1 — explain every choice):

1.  Sliding-window mechanics:
    We maintain a per-service deque of raw events keyed by (service_name).
    A "window" is the subset of events whose timestamp falls within the last
    WINDOW_SECONDS.  Every WINDOW_SLIDE_SECONDS, emit_windows() is called and
    we slide every service's window forward, dropping events older than
    WINDOW_SECONDS.

    Why sliding vs. tumbling?  Tumbling windows (non-overlapping) miss anomalies
    that start mid-window.  A 30 s / 10 s sliding window gives 3x coverage of
    any 10-second anomaly burst, greatly reducing missed detections — the same
    reason Prometheus uses rate() over a sliding interval rather than a counter
    snapshot.

2.  Features chosen (5 total):
    - request_count        : raw throughput signal; a sudden drop (service down)
                             or spike (traffic surge) is anomalous.
    - error_rate           : fraction of 5xx events; the most direct signal of
                             a broken backend.
    - p50_latency_ms       : median latency — robust to outliers, reflects the
                             typical user experience.
    - p95_latency_ms       : 95th-percentile latency — the "tail" users notice;
                             spikes here predate outright failures.
    - p99_latency_ms       : 99th-percentile — extreme tail; GC pauses and memory
                             pressure appear here first.
    - status_entropy       : Shannon entropy of the status-code distribution in
                             the window.  Normal traffic has a stable, low-entropy
                             distribution (mostly 200/201).  Unusual status mixes
                             (redirect loops, mass 4xx) raise entropy without
                             necessarily raising the 5xx error rate, so this
                             catches anomalies that error_rate misses.

3.  Thread safety:
    SlidingWindowAccumulator is NOT thread-safe by design.  It is consumed by a
    single-threaded Kafka consumer loop (see detector.py).  If you later add
    multi-threaded polling, wrap accesses in a threading.Lock.

4.  Empty-window behaviour:
    If a window contains zero events (service went silent), we return a feature
    vector of all zeros.  A trained Isolation Forest will correctly score a
    zero-count window as anomalous because normal traffic always has some events.
"""

from __future__ import annotations

import math
from collections import defaultdict, deque
from typing import Any, NamedTuple


# ---------------------------------------------------------------------------
# Feature vector type
# ---------------------------------------------------------------------------

class WindowFeatures(NamedTuple):
    """Extracted feature vector for one (service, time-window) pair."""

    service_name: str
    window_end_ts: float          # Unix timestamp of the window's trailing edge
    request_count: float
    error_rate: float             # fraction of events with status_code >= 500
    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    status_entropy: float

    def to_model_input(self) -> list[float]:
        """Return only the numeric features, in the order the model expects."""
        return [
            self.request_count,
            self.error_rate,
            self.p50_latency_ms,
            self.p95_latency_ms,
            self.p99_latency_ms,
            self.status_entropy,
        ]


# ---------------------------------------------------------------------------
# Percentile helper
# ---------------------------------------------------------------------------

def _percentile(values: list[float], pct: float) -> float:
    """
    Compute the pct-th percentile of a sorted or unsorted list.

    Uses nearest-rank method — no external dependencies, deterministic output.
    Returns 0.0 for an empty list.
    """
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    # nearest-rank: index = ceil(pct/100 * n) - 1, clamped to [0, n-1]
    index = max(0, math.ceil(pct / 100.0 * len(sorted_vals)) - 1)
    return sorted_vals[index]


# ---------------------------------------------------------------------------
# Status-code entropy
# ---------------------------------------------------------------------------

def _status_entropy(status_codes: list[int]) -> float:
    """
    Shannon entropy (nats) of the status-code distribution.

    H = - Σ p_i * ln(p_i)

    A perfectly uniform distribution (all codes equally likely) maximises
    entropy.  A single dominant code (all 200) gives low entropy ≈ 0.
    Normal production traffic clusters heavily on 200/201 → low entropy.
    An anomaly (redirect loop, mass 4xx) spreads codes → higher entropy.
    """
    if not status_codes:
        return 0.0
    total = len(status_codes)
    counts: dict[int, int] = {}
    for code in status_codes:
        counts[code] = counts.get(code, 0) + 1
    entropy = 0.0
    for count in counts.values():
        p = count / total
        entropy -= p * math.log(p)  # natural log (nats)
    return entropy


# ---------------------------------------------------------------------------
# Core accumulator
# ---------------------------------------------------------------------------

class SlidingWindowAccumulator:
    """
    Accumulate raw log events and emit feature vectors on a sliding schedule.

    Parameters
    ----------
    window_seconds : float
        Width of the time window (how far back we look).  Default: 30 s.
    slide_seconds : float
        How often a new window is emitted.  Default: 10 s.

    Typical usage:
        acc = SlidingWindowAccumulator()
        acc.add_event(event_dict)          # called for every Kafka message
        features = acc.emit_windows(now)   # called every slide_seconds
    """

    def __init__(
        self,
        window_seconds: float = 30.0,
        slide_seconds: float = 10.0,
    ) -> None:
        self.window_seconds = window_seconds
        self.slide_seconds = slide_seconds
        # One deque of events per service.  deque chosen over list for O(1)
        # popleft() when pruning old events from the left end.
        self._buffers: dict[str, deque[dict[str, Any]]] = defaultdict(deque)
        self._last_emit_ts: float = 0.0  # wall clock of last window emission

    def add_event(self, event: dict[str, Any]) -> None:
        """
        Append a raw log event to the per-service buffer.

        event must contain:
            timestamp    : float  (Unix epoch seconds)
            service_name : str
            status_code  : int
            latency_ms   : float
        """
        svc = event.get("service_name", "unknown")
        self._buffers[svc].append(event)

    def should_emit(self, now: float) -> bool:
        """Return True if slide_seconds have elapsed since the last emission."""
        return (now - self._last_emit_ts) >= self.slide_seconds

    def emit_windows(self, now: float) -> list[WindowFeatures]:
        """
        Slide all per-service windows forward and return one WindowFeatures
        per service that has data (or ever had data).

        Old events (timestamp < now - window_seconds) are pruned in-place.
        """
        self._last_emit_ts = now
        cutoff = now - self.window_seconds
        results: list[WindowFeatures] = []

        for svc, buf in self._buffers.items():
            # Prune events older than the window.
            while buf and buf[0]["timestamp"] < cutoff:
                buf.popleft()

            # Extract features from the surviving events.
            events_in_window = list(buf)
            features = _extract_features(svc, now, events_in_window)
            results.append(features)

        return results


# ---------------------------------------------------------------------------
# Feature extraction from a window's event list
# ---------------------------------------------------------------------------

def _extract_features(
    service_name: str,
    window_end_ts: float,
    events: list[dict[str, Any]],
) -> WindowFeatures:
    """
    Compute the 6 numeric features from the raw events in one window.

    Called by SlidingWindowAccumulator.emit_windows().
    Also called directly from unit tests and train.py (synthetic data path).
    """
    if not events:
        return WindowFeatures(
            service_name=service_name,
            window_end_ts=window_end_ts,
            request_count=0.0,
            error_rate=0.0,
            p50_latency_ms=0.0,
            p95_latency_ms=0.0,
            p99_latency_ms=0.0,
            status_entropy=0.0,
        )

    n = len(events)
    latencies: list[float] = [e["latency_ms"] for e in events]
    status_codes: list[int] = [e["status_code"] for e in events]
    error_count = sum(1 for c in status_codes if c >= 500)

    return WindowFeatures(
        service_name=service_name,
        window_end_ts=window_end_ts,
        request_count=float(n),
        error_rate=error_count / n,
        p50_latency_ms=_percentile(latencies, 50),
        p95_latency_ms=_percentile(latencies, 95),
        p99_latency_ms=_percentile(latencies, 99),
        status_entropy=_status_entropy(status_codes),
    )


# Re-export _extract_features as a public name for train.py / tests.
extract_features = _extract_features
