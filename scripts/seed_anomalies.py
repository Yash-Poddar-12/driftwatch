#!/usr/bin/env python3
"""
scripts/seed_anomalies.py — Inject a labeled anomaly into the DriftWatch pipeline.

Design notes (AGENTS.md §1 — explain every choice):

WHY this script exists:
  Phase 2 verification involved running:
    docker compose run --rm -e ANOMALY_MODE=latency_spike log-producer-payments
  manually in a separate shell.  That works, but it's tedious, not reproducible,
  and leaves a stopped container hanging around.  This script formalizes the same
  operation as a CLI tool with:
    - a --service argument (which producer to target)
    - a --mode argument (which ANOMALY_MODE to inject)
    - a --duration argument (how many seconds to run before stopping)
  so you can run `python scripts/seed_anomalies.py --service payments-service
  --duration 60` from anywhere and get a clean, timed anomaly injection.

HOW it works:
  1. docker exec into the running log-producer-<service> container and
     set ANOMALY_MODE on the process via a new docker run --rm invocation.
     Wait — we can't change env vars in a running container.

  Actually, the correct approach (matching what the user already does):
    docker compose run --rm \\
      -e ANOMALY_MODE=latency_spike \\
      -e SERVICE_NAME=payments-service \\
      -e REGION=eu-west-1 \\
      -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \\
      -e KAFKA_TOPIC=logs.raw \\
      log-producer-payments
  ... but with a timeout (--duration) so it stops automatically.

  We spawn a new container from the same image (so it joins the same Compose
  network) with the anomaly env var set.  After --duration seconds we send
  SIGTERM to the process, which causes the container to stop cleanly (the
  producer.py handles SIGTERM / KeyboardInterrupt via a try/finally).

  The NEW container adds anomaly events to the same Kafka topic that the
  existing normal containers are also writing to.  The anomaly detector sees
  the blended stream: during the injection window, the sliding-window feature
  vector for that service will show elevated latency / error rate / entropy,
  causing the IsolationForest to flag it.

WHY subprocess + docker compose run (not Docker SDK)?
  The docker Python SDK (docker-py) is not in the current requirements and
  would be a new dependency.  subprocess + docker CLI keeps this script
  dependency-free — it runs wherever docker is installed, with no pip install.

USAGE:
  python scripts/seed_anomalies.py --service payments-service --duration 60
  python scripts/seed_anomalies.py --service auth-service --mode error_burst --duration 45
  python scripts/seed_anomalies.py --service inventory-service --mode unusual_status --duration 30

  --service   : one of auth-service, payments-service, inventory-service
  --mode      : latency_spike (default) | error_burst | unusual_status
  --duration  : how many seconds to run the anomaly producer (default: 60)
  --events-per-second : optional override (default matches docker-compose.yml)

REQUIREMENTS:
  docker CLI must be in PATH and the DriftWatch Compose stack must already
  be running (`docker compose up -d` first).
"""

from __future__ import annotations

import argparse
import signal
import subprocess
import sys
import time

# ---------------------------------------------------------------------------
# Service metadata: maps logical service name → Compose service name +
# the env vars the producer expects.
# These match docker-compose.yml exactly (AGENTS.md rule 10).
# ---------------------------------------------------------------------------
SERVICE_MAP: dict[str, dict[str, str]] = {
    "auth-service": {
        "compose_service": "log-producer-auth",
        "SERVICE_NAME": "auth-service",
        "REGION": "us-east-1",
        "EVENTS_PER_SECOND": "5",
    },
    "payments-service": {
        "compose_service": "log-producer-payments",
        "SERVICE_NAME": "payments-service",
        "REGION": "eu-west-1",
        "EVENTS_PER_SECOND": "3",
    },
    "inventory-service": {
        "compose_service": "log-producer-inventory",
        "SERVICE_NAME": "inventory-service",
        "REGION": "ap-southeast-1",
        "EVENTS_PER_SECOND": "4",
    },
}

VALID_MODES: tuple[str, ...] = ("latency_spike", "error_burst", "unusual_status")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Inject a labeled anomaly into the DriftWatch pipeline by running "
            "an additional log-producer container with ANOMALY_MODE set."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--service",
        required=True,
        choices=list(SERVICE_MAP.keys()),
        help="Which simulated service to inject anomalies for.",
    )
    parser.add_argument(
        "--mode",
        default="latency_spike",
        choices=list(VALID_MODES),
        help=(
            "Which anomaly mode to inject.  "
            "latency_spike: multiplies latency 5-15x.  "
            "error_burst: 80%% of requests return 500.  "
            "unusual_status: spreads responses across 2xx/3xx/4xx unusually.  "
            "Default: latency_spike"
        ),
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=60.0,
        help="How many seconds to run the anomaly producer (default: 60).",
    )
    parser.add_argument(
        "--events-per-second",
        type=int,
        default=None,
        help=(
            "Override the events/second rate for the anomaly producer.  "
            "Default: same as the normal producer for this service."
        ),
    )
    parser.add_argument(
        "--project-name",
        default="driftwatch",
        help="Docker Compose project name (default: driftwatch).",
    )
    return parser.parse_args()


def check_docker_available() -> None:
    """Confirm docker CLI is reachable."""
    try:
        subprocess.run(
            ["docker", "info"],
            check=True,
            capture_output=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        print(
            "ERROR: docker CLI not found or Docker daemon not running.\n"
            "Make sure Docker Desktop (or Docker Engine) is running.",
            file=sys.stderr,
        )
        sys.exit(1)


def check_stack_running(project_name: str, compose_service: str) -> None:
    """
    Verify the target compose service is running.

    We check for the container by its expected name:
      <project>-<service>-1  (Compose v2 naming convention)
    or the older:
      <project>_<service>_1  (Compose v1)
    """
    result = subprocess.run(
        ["docker", "ps", "--format", "{{.Names}}"],
        check=True,
        capture_output=True,
        text=True,
    )
    running_containers = result.stdout.strip().splitlines()

    # Accept either naming convention.
    expected_v2 = f"{project_name}-{compose_service}-1"
    # Also accept the container_name we set in docker-compose.yml
    expected_named = compose_service.replace("log-producer-", "log-producer-")

    # The container_name in our compose is e.g. "log-producer-payments"
    # (without project prefix).
    matches = [
        c for c in running_containers
        if compose_service in c or expected_named in c
    ]
    if not matches:
        print(
            f"ERROR: No running container found for service '{compose_service}'.\n"
            f"Running containers: {running_containers}\n"
            f"Start the stack first with: docker compose up -d",
            file=sys.stderr,
        )
        sys.exit(1)
    print(f"✓ Found running container(s): {matches}")


def run_anomaly_producer(
    project_name: str,
    compose_service: str,
    svc_meta: dict[str, str],
    mode: str,
    duration: float,
    events_per_second: int | None,
) -> None:
    """
    Spawn a new container from the same image with ANOMALY_MODE set, let it
    run for `duration` seconds, then stop it cleanly.

    WHY docker compose run --rm?
      compose run starts a one-off container from a service definition.
      --rm removes it automatically when it exits.
      The new container inherits the Compose network (driftwatch_driftwatch),
      so it can reach Kafka at kafka:9092 and write to the same topic.
      We override just the ANOMALY_MODE and optionally EVENTS_PER_SECOND env vars.

    WHY not modify the running container?
      You can't change env vars of a running container.  The cleanest approach
      for a short anomaly injection is to add a second producer (same image,
      same topic, different env) alongside the normal one for the duration.
      The detector sees the combined stream — which is a realistic simulation
      of a service instance in a bad state while others are healthy.
    """
    eps = str(events_per_second) if events_per_second else svc_meta["EVENTS_PER_SECOND"]

    cmd = [
        "docker", "compose",
        "--project-name", project_name,
        "run",
        "--rm",
        "-e", f"ANOMALY_MODE={mode}",
        "-e", f"SERVICE_NAME={svc_meta['SERVICE_NAME']}",
        "-e", f"REGION={svc_meta['REGION']}",
        "-e", f"EVENTS_PER_SECOND={eps}",
        "-e", "KAFKA_BOOTSTRAP_SERVERS=kafka:9092",
        "-e", "KAFKA_TOPIC=logs.raw",
        "--no-deps",       # don't also start dependency services
        compose_service,
    ]

    print(
        f"\n🔥 Starting anomaly injection:"
        f"\n   service  : {svc_meta['SERVICE_NAME']}"
        f"\n   mode     : {mode}"
        f"\n   duration : {duration}s"
        f"\n   rate     : {eps} events/s"
        f"\n   cmd      : {' '.join(cmd)}\n"
    )

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    start_time = time.time()
    deadline = start_time + duration

    # Register a SIGINT/SIGTERM handler to clean up if the user Ctrl-C's.
    def _cleanup(signum: int, frame: object) -> None:
        print("\n⚠  Interrupted — stopping anomaly producer early …")
        proc.terminate()
        sys.exit(0)

    signal.signal(signal.SIGINT, _cleanup)
    signal.signal(signal.SIGTERM, _cleanup)

    # Stream producer output and enforce the duration limit.
    try:
        while True:
            elapsed = time.time() - start_time
            remaining = deadline - time.time()
            if remaining <= 0:
                break

            # Non-blocking check for new output (1 s poll interval).
            try:
                line = proc.stdout.readline()  # type: ignore[union-attr]
                if line:
                    print(f"  [producer] {line}", end="")
            except Exception:
                pass

            if proc.poll() is not None:
                # Container exited on its own.
                print("  [producer] container exited early.")
                break

            time.sleep(0.5)
    finally:
        elapsed = time.time() - start_time
        if proc.poll() is None:
            print(f"\n⏹  Duration ({duration}s) reached — stopping anomaly producer …")
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
        print(
            f"✅ Anomaly injection complete.\n"
            f"   Ran for {elapsed:.1f}s. "
            f"   Check the Grafana dashboard (http://localhost:3000) for flagged points\n"
            f"   on the '{svc_meta['SERVICE_NAME']}' anomaly score panel.\n"
            f"   Or query directly:\n"
            f"     docker exec -it timescaledb psql -U driftwatch -d driftwatch \\\n"
            f"       -c \"SELECT time, service_name, anomaly_score FROM anomalies ORDER BY time DESC LIMIT 10;\""
        )


def main() -> None:
    args = parse_args()

    check_docker_available()

    svc_meta = SERVICE_MAP[args.service]
    check_stack_running(args.project_name, svc_meta["compose_service"])

    run_anomaly_producer(
        project_name=args.project_name,
        compose_service=svc_meta["compose_service"],
        svc_meta=svc_meta,
        mode=args.mode,
        duration=args.duration,
        events_per_second=args.events_per_second,
    )


if __name__ == "__main__":
    main()
