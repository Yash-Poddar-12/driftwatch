# CLAUDE.md

Thin pointer file for Claude Code — including sessions routed through a custom model provider (e.g. GLM 5.2 via an Anthropic-compatible endpoint).

**Read and follow `AGENTS.md` at the repository root in full before doing anything else.** It is the canonical source of truth for this project's rules, conventions, architecture, and — critically — the multi-tool coordination rules in its Section 5, since this repo is also worked on via Codex and Antigravity.

Claude Code–specific operating notes (mechanics only — no project rules belong here):
- At the start of every session in this repo, explicitly open `AGENTS.md`, `README.md`, and `TASKS.md` — don't rely on prior conversation memory carrying over between sessions.
- Before claiming a task in `TASKS.md`, confirm no other tool has it in progress.
- If Skills or Subagents are configured for this repo, prefer them for repeatable patterns (e.g. "scaffold a new service") over re-deriving the pattern from scratch each session.
- If you disagree with a rule in `AGENTS.md`, say so and propose an edit to `AGENTS.md` itself — don't just follow a different convention silently, since Codex and Antigravity sessions won't see that divergence.
