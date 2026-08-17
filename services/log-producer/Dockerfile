# syntax=docker/dockerfile:1
# ─── Stage 1: build ─────────────────────────────────────────────────────────
# We install Python deps into an isolated venv here so they can be cleanly
# copied to the runtime stage.  Nothing from the build stage (pip cache, wheel
# temp files, etc.) leaks into the final image.
# Pinning python:3.11-slim per AGENTS.md rule 5 — never use :latest.
FROM python:3.11-slim AS builder

WORKDIR /build

# Layer-cache trick: copy requirements first so pip install is only re-run
# when requirements.txt changes, not on every code change.
COPY requirements.txt .

RUN python -m venv /opt/venv \
    && /opt/venv/bin/pip install --upgrade pip --quiet \
    && /opt/venv/bin/pip install --no-cache-dir -r requirements.txt

# ─── Stage 2: runtime ────────────────────────────────────────────────────────
# Only the venv and application code are copied here.
# Final image contains no build tools, no pip, no wheel cache.
FROM python:3.11-slim AS runtime

# PYTHONDONTWRITEBYTECODE=1  — skip .pyc files; small image benefit.
# PYTHONUNBUFFERED=1         — flush stdout/stderr immediately; required so
#                              container log drivers see output in real time.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH"

# Non-root user — container security baseline.
# If the process is compromised, the attacker cannot write outside /app.
RUN addgroup --system appgroup \
    && adduser --system --ingroup appgroup --no-create-home appuser

WORKDIR /app

# Copy the venv from the builder stage (all deps, no build artifacts).
COPY --from=builder /opt/venv /opt/venv

# Copy application code last — maximises cache reuse.
COPY producer.py .

# Switch to the non-root user.
USER appuser

# Health check wired to the /healthz endpoint in producer.py.
# Interval 15 s: fast enough to detect a sick container promptly.
# Retries 3: avoids flapping on a single transient hiccup.
HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=3 \
    CMD python -c \
        "import urllib.request; urllib.request.urlopen('http://localhost:8080/healthz')"

EXPOSE 8080

CMD ["python", "producer.py"]
