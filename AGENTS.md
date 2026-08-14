# AGENTS.md — Multi-Region Log Anomaly Detection Pipeline

> Canonical instructions for **any** AI coding agent working in this repository — Claude Code, Codex, Antigravity, or anything else, regardless of which underlying model is powering it. Read this file in full before making any changes. Tool-specific files (`CLAUDE.md`, etc.) are thin pointers back to this file — **this file is the single source of truth.** If a tool-specific file ever seems to conflict with this one, this one wins.

---

## 1. Core principle
This is a **learning project**. The owner (a student) is intentionally building this to gain hands-on experience with Docker, Kubernetes, AWS, and CI/CD. You are expected to **write and ship the actual code and infra directly** — don't hold back waiting for the owner to type it themselves. What matters is that **every change comes with a clear explanation alongside it**: what you wrote, why you chose that approach over the alternatives, and what concept it demonstrates (e.g. why Kafka runs as a `StatefulSet` instead of a `Deployment`). This applies especially to infra-layer changes (Dockerfiles, K8s manifests, Terraform/CDK, GitHub Actions workflows) — the owner needs to be able to explain every part of this system in an interview, so the explanation is not optional, but implementation speed should not be held back either.

## 2. Hard rules
1. **Never commit secrets.** No AWS keys, DB passwords, API tokens, or `.env` contents in tracked files. Use `.env.example` with placeholder values only. If you detect a real secret in a diff, stop and flag it instead of committing.
2. **Never modify `main`/`prod` deployment targets directly.** All infra changes go through a feature branch and the CI/CD pipeline in `.github/workflows/`. Do not run `kubectl apply` or `terraform apply` against the AWS/EKS environment from a local shell as a shortcut.
3. **Do not delete the local Docker Compose path.** Even after Kubernetes manifests exist, `docker-compose.yml` must remain functional for fast local iteration. If you change a service's config, update both Compose and K8s manifests together.
4. **Do not silently change the anomaly detection algorithm.** If you swap Isolation Forest for another model, or change feature engineering logic, document the change and rationale in `docs/decisions/` (an ADR) rather than just overwriting the file.
5. **Keep images small and pinned.** Always pin base image versions (e.g. `python:3.11-slim`, not `python:latest`). Never introduce a new dependency without adding it to the relevant `requirements.txt` / `package.json`.
6. **Every new service needs:** a `Dockerfile`, an entry in `docker-compose.yml`, a corresponding K8s `Deployment` + `Service` YAML (or Helm template), a health check endpoint, and a short section added to `README.md`'s Component Breakdown.
7. **Tests before infra.** If you add or change anomaly-detection logic, add/update a unit test in `tests/` that asserts detection behavior on a known synthetic anomaly before touching deployment configs.
8. **Ask before scope changes.** New AWS service, new cloud region, new major dependency (e.g. swapping Kafka for another broker) — confirm with the owner first. This is a resume/learning project; scope changes affect what the owner can credibly explain in an interview.
9. **Never fabricate benchmark numbers or "results."** Only use numbers actually produced by running the code. Mark placeholders clearly: `<TODO: run benchmark_script.py and fill in>`.
10. **Respect the folder structure in `README.md` Section 3.** Don't reorganize directories without updating every reference (Dockerfile `COPY` paths, CI workflow paths, K8s volume mounts, README, this file).

## 3. Style conventions
- **Commits:** Conventional Commits — `feat:`, `fix:`, `infra:`, `docs:`, `test:`, `chore:`. E.g. `infra: add HPA for anomaly-detector deployment`.
- **Python:** PEP 8, type-hint all function signatures, format with `black`, lint with `ruff`.
- **YAML (K8s/Compose):** 2-space indentation, always include `resources.requests`/`limits`, always include `readinessProbe` and `livenessProbe` on Deployments.
- **Branch naming:** `agent/<tool>/<short-task-desc>` — see Section 5 below. (e.g. `agent/claude-code/log-producer-service`)
- **Documentation:** any nontrivial architectural decision gets a short ADR in `docs/decisions/NNNN-title.md` (context, decision, consequences — 1 page max).

## 4. When in doubt
If a task is ambiguous or could be solved multiple ways with different learning value (e.g. "just use a managed service" vs. "self-host it to learn how it works"), **default to the option with more hands-on learning value** — that is the explicit purpose of this project — unless the owner says otherwise.

## 5. Multi-tool coordination (read this — it's the part that's new)
This repo is worked on by **several different AI coding agents/tools** (e.g. Claude Code, Codex, Antigravity), sometimes running different underlying models (e.g. GLM 5.2, GPT, Gemini, Claude), sometimes on the same day. **None of these agents share memory or context with each other.** Coordination happens entirely through files committed to this repo. Follow this strictly:

1. **Check `TASKS.md` before starting anything.** It is the single source of truth for what's done, in progress, or blocked. Do not start a task another tool has already marked in-progress without the owner's explicit go-ahead.
2. **Claim a task before writing code.** Update `TASKS.md` to mark the task in-progress and note which tool/model is doing it (e.g. `🟨 Claude Code / GLM 5.2`).
3. **One agent, one branch, one task at a time.** Never work directly on `main`. Branch as `agent/<tool>/<short-task-desc>`. Never have two agents editing the same branch, or the same files on different branches, concurrently — the owner will hit merge conflicts and won't know which version is "right."
4. **Mark tasks done and leave a summary.** When finished, update `TASKS.md` to done and leave 2-3 lines on what was built and any follow-ups — so a *different* tool picking up the next task has context without re-reading your whole diff.
5. **Don't silently overwrite another agent's merged work.** Different models have different opinions on implementation. If you think a previous decision was wrong, propose the change as a new ADR in `docs/decisions/` instead of silently rewriting — silent rewrites cause thrashing across tools and waste the owner's review time.
6. **Tool-specific files only add mechanics, never new rules.** `CLAUDE.md` (and any future `GEMINI.md`, `.cursorrules`, etc.) should only contain tool-specific operating notes (e.g. "use the Read tool before editing"). Project rules live here, in `AGENTS.md`, only.
