# Lakehouse Agent Kit — Roadmap

> Companion to [`IMPLEMENTATION_PLAN.md`](./IMPLEMENTATION_PLAN.md) (the design) and
> [`BACKLOG.md`](./BACKLOG.md) (the ready-to-file task list). This file is the high-level
> map: what's done, what's next, and how a new contributor/session picks up a piece.

## Status at a glance

| Phase | Scope | Status |
|-------|-------|--------|
| **0 — Bootstrap** | Repo, plan, CONTRIBUTING, skill template, CI skeleton | done, merged |
| **1 — Foundations** | `lakehouse-core`, connection-profile contract, skills linter, CI | done, merged |
| **1.5 — Generalization engine** | Shared endpoint **resolver** (config to discover to prefer to fail), lint rules that enforce it, cited `reference-stack` profile | done, merged |
| **2 — Engine skills** | Port the 9 engine skills, each resolver-driven + cross-reviewed | in progress (pilot specced; see note) |
| **3 — Reference-stack ops** | `lifecycle`, `dashboard` (decision), `delta-sharing` | not started |
| **4 — Distribution** | Single-source generator to plugin marketplace + `pip`/`uvx` installer, CI drift | not started |
| **5 — MCP (optional)** | Document/bundle complementary community MCP servers | not started |
| **6 — v1 release** | Docs, examples, versioned release, announce | not started |

> **Pilot note (Phase 2):** `spark`, `delta`, and `iceberg` were implemented and
> cross-reviewed in a pilot run, but the work was lost before push due to a sandbox disk
> failure (local-only commits; nothing merged was affected). They are fully specced in
> `BACKLOG.md` (LAK-1/2/3) including the exact review fixes and the already-resolved §8
> answers so re-running them is quick. **Process change:** push every reviewed skill branch
> to the remote immediately after review so reviewed work can't be stranded locally again.

## The north star

A **cross-agent [Agent Skills](https://agentskills.io) toolkit** for an open lakehouse
(Spark, Delta, Iceberg, Unity Catalog OSS, Kafka, Airflow, MLflow, Delta Sharing) — the
open-source analogue of the Databricks agent toolkit. Skills are **stack-agnostic**: every
endpoint is resolved from a **lakehouse connection profile** via `lakehouse-core`'s resolver
(discover-then-prefer), never hardcoded, and a linter enforces that in CI.

## Phase detail & acceptance

### Phase 2 — Engine skills (`type:skill-port`)
Port each source skill from `open-lakehouse/.claude/skills/` into `skills/<name>/`,
parameterized against the resolver. **One skill per PR, cross-reviewed by a different vendor.**
Skills: `spark`, `delta`, `iceberg`, `unity-catalog`, `kafka`, `airflow`, `mlflow`, `sdp`, `object-store`.
**Acceptance (every skill):** CONTRIBUTING-compliant frontmatter (6 portable fields);
`SKILL.md` <= ~500 lines with progressive disclosure; **no hardcoded host/port/container**
(all endpoints via `resolve_endpoint.py`, identity confirmed not auto-selected); relevant
§8 contradiction resolved + cited; provenance tags; all skills-CI gates exit 0.
Backlog: **LAK-1 … LAK-9**.

### Phase 3 — Reference-stack ops (`phase:3`)
Opt-in skills bound to a concrete deployment. Backlog: **LAK-10 … LAK-12**.

### Phase 4 — Distribution (`type:distribution`)
One source of truth (`plugin.meta.json`) to generated per-agent bundles (`dist/`), a plugin
marketplace, and a `pip`/`uvx` cross-agent installer; CI fails on drift. Backlog: **LAK-13 … LAK-19**.

### Phase 5 — MCP (optional) (`phase:5`)
Point at existing community MCP servers as a complementary live-tools layer; don't rebuild.
Backlog: **LAK-20**.

### Phase 6 — v1 release (`phase:6`)
Docs pass, worked examples, versioned release, announcement. Backlog: **LAK-21**.

### Cross-cutting / tech-debt (`type:tech-debt`, `type:docs`)
Known fixes and follow-ups surfaced during Phase 1.5 and the Phase 2 pilot. Backlog: **LAK-22 … LAK-30**.

## How to pick up a task (for any session/contributor)

1. **Clone fresh** and branch off `master`: `git clone … && git switch -c <task> master`.
2. **Read the contract:** [`CONTRIBUTING.md`](./CONTRIBUTING.md), [`IMPLEMENTATION_PLAN.md`](./IMPLEMENTATION_PLAN.md)
   §3.4/§6/§8, and the resolver contract in `skills/lakehouse-core/scripts/resolve_endpoint.py`
   + `skills/lakehouse-core/SKILL.md`.
3. **For a skill port:** read the source skill from `open-lakehouse/.claude/skills/<source>/`
   (pin + cite the commit), create `skills/<name>/SKILL.md` (+ `references/`, `scripts/`),
   resolve every endpoint via the resolver, resolve the assigned §8 item, add provenance.
4. **Green the gates** (all exit 0, from repo root):
   `python3 -I scripts/lint_skills.py --skills-dir skills/` and `--self-test`, plus the three
   `skills/lakehouse-core/scripts/*.py --self-test`.
5. **Cross-review** with a different-vendor agent against the task's acceptance criteria; fix
   blocking findings; loop until clean.
6. **Open a PR** and push the branch promptly; a human merges. Keep each task's file scope
   disjoint (one skill dir) so parallel work doesn't conflict.

The canonical, per-task breakdown lives in [`BACKLOG.md`](./BACKLOG.md). Each `LAK-n` maps to one GitHub issue.
