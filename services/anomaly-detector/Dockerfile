# Dockerfile — anomaly-detector service
#
# Design notes (AGENTS.md §1 — explain every choice):
#
# Multi-stage build:
#   Stage 1 (builder): installs build tools and compiles C-extension wheels
#   (numpy, scikit-learn use Cython/C extensions that need a compiler).
#   Stage 2 (runtime): python:3.11-slim with only the compiled wheels copied
#   in — no compiler, no build headers, smaller final image.
#
# python:3.11-slim:
#   scikit-learn 1.9.0 requires Python ≥ 3.11.  "slim" drops apt caches and
#   documentation, saving ~100 MB vs the full image while keeping pip/venv.
#
# Non-root user (appuser):
#   Running as root inside a container is a security anti-pattern.  Even in
#   a local Compose setup, forming the habit of non-root containers matters:
#   you carry this Dockerfile into the K8s/EKS stage where it's enforced by
#   PodSecurityAdmission policies.
#
# PYTHONDONTWRITEBYTECODE / PYTHONUNBUFFERED:
#   Prevent .pyc clutter in the image and ensure log lines reach Docker's log
#   collector immediately (important for a service that prints scores to stdout).
#
# train.py run at build time:
#   The model artefact is baked into the image during docker build.  This means
#   the container starts immediately without a separate init step, keeps the
#   runtime image self-contained, and makes deployments reproducible (same image
#   tag → same model version).  If you later want to hot-swap models without a
#   rebuild, mount a volume over /app/models/ and skip the RUN python train.py
#   layer, but for Phase 2 baking it in is simpler.

# ── Stage 1: builder ────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /build

# Install build dependencies for numpy/scikit-learn C extensions.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        g++ \
        && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip --no-cache-dir && \
    pip install --no-cache-dir --prefix=/install -r requirements.txt

# ── Stage 2: runtime ────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

# Prevent .pyc files and force stdout/stderr to be unbuffered.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Non-root user (AGENTS.md rule 5, standard container security practice).
RUN useradd --create-home --shell /bin/bash appuser

WORKDIR /app

# Copy installed Python packages from builder.
COPY --from=builder /install /usr/local

# Copy service source.
COPY --chown=appuser:appuser . .

# Create the models directory with correct ownership before switching user.
RUN mkdir -p /app/models && chown appuser:appuser /app/models

USER appuser

# Train the model at build time so the container is self-contained.
# The model artefact lands in /app/models/isolation_forest_v1.joblib.
RUN python train.py

# Expose the /healthz port (AGENTS.md rule 6).
EXPOSE 8084

HEALTHCHECK --interval=10s --timeout=5s --retries=5 --start-period=15s \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8084/healthz')"

CMD ["python", "detector.py"]
