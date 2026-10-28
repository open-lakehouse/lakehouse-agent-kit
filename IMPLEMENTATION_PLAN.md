# Lakehouse Agent Kit — Implementation Plan (v1)

> Status: **Draft for review** (Lisa, Andre, Tony). Date: 2026-10-21.
> Owner: TBD. Target: a cross-agent **Lakehouse Agent Kit** — the
> open-source analogue of the Databricks agent toolkit (`databricks/databricks-agent-skills`).

---

## 1. Why

We have a Databricks agent toolkit (a parent `databricks-core` skill + ~30 product
skills that drive the `databricks` CLI, distributed as cross-agent plugins). There is
no equivalent for an **open** lakehouse stack (Spark + Delta + Iceberg + Unity Catalog
OSS + Kafka + Airflow + MLflow + Delta Sharing).

The [`open-lakehouse/open-lakehouse`](https://github.com/open-lakehouse/open-lakehouse)
reference architecture already ships 12 "built-in" skills under `.claude/skills/`. They
are high-quality but **welded to that one repo's Docker Compose stack** — hardcoded
`localhost` ports, container names, a 147 KB `./lakehouse` CLI, repo-relative paths, and
pinned versions. The opportunity: **extract the portable engine knowledge into a
standalone, parameterized, cross-agent toolkit** that works against *any* deployment of
the stack, and distribute it the way the Databricks toolkit is distributed.

## 2. What we're building

A repo (`lakehouse-agent-kit`) that ships:

1. **A set of spec-portable Agent Skills** organized as a parent/entry skill
   (`lakehouse-core`) + product skills, each driving the native engine CLIs via
   progressive disclosure.
2. **A connection-profile abstraction** that replaces the reference repo's hardcoded
   endpoints with user-supplied config — the core of the "extraction" work.
3. **Multi-target distribution** from one source of truth: Claude Code (+ Cursor/Codex/
   Copilot) plugin marketplace, a `pip`/`uvx` installer that writes into each agent's
   native skills dir, and a plain copy-in / `.zip` fallback.
4. **(Optional) a complementary MCP layer** that points at existing community MCP servers
   rather than reimplementing them.

Non-goals for v1: building a new runtime CLI for the stack; reimplementing engine MCP
servers; bundling/operating the compute stack itself.

## 3. Design principles

1. **Spec-portable skills.** Author with the six portable Agent Skills frontmatter
   fields only — `name`, `description`, `license`, `compatibility`, `metadata`,
   `allowed-tools`. Claude-Code-only extensions (`when_to_use`, `hooks`, `model`, …) are
   isolated into generated per-target bundles, never the skill body. (Keeps skills usable
   on Claude.ai, Claude Code, Cursor, Codex, Copilot, and the Skills API.)
2. **Progressive disclosure.** `SKILL.md` ≤ ~500 lines; push depth into `references/*.md`
   and executable `scripts/` loaded only on demand.
3. **Parent/child routing** — mirror `databricks-core`: `lakehouse-core` handles
   connection + profile selection + a tool/version floor check, then routes to the
   matching product skill. Product skills declare `parent: lakehouse-core` (in portable
   `metadata`) and open with "FIRST: use the parent skill."
4. **Discover, don't hardcode; prefer, don't guess.** No skill body contains a literal
   `localhost:PORT`, container name, or repo path. Every environment-specific value is
   resolved at runtime in a fixed **resolution order**:
   1. **Explicit config** — a profile/env value, if set, is authoritative.
   2. **Discover** — otherwise probe the live environment: teach the agent the *discovery
      command* (ask the tool/API for its configured endpoint, test whether the
      conventional port is reachable, list running containers, read a well-known config
      file), never a baked-in answer.
   3. **Preferred default** — fall back to the conventional port/name (the "prefer X")
      only after verifying it is actually reachable, and label it as a preference.
   4. **Fail loud** — if nothing resolves, stop with a clear message on how to set it.

   Topology is *discovered*; **identity and targeting with side effects** (which profile,
   catalog, schema, bucket, or credentials to act on) is **never** guessed — that keeps
   the `lakehouse-core` "never auto-select" rule intact. Concretely: "use Spark on 15002"
   becomes "resolve the Spark Connect endpoint from config; else discover a reachable one,
   preferring 15002; verify before use; otherwise report how to set it." `lakehouse-core`
   ships shared resolver helpers so every skill applies this order identically.
5. **Separate engine knowledge from stack operations.** Portable engine skills vs.
   reference-stack operations skills (the latter opt-in, tied to a concrete deployment).
6. **One source of truth → many targets.** A single metadata file + generator produces
   every per-agent plugin bundle and marketplace catalog; CI fails on drift.
7. **Version-pinned & honest.** Pin underlying tool versions via `compatibility`; record
   "verified on <version>/<date>" provenance; fix the known contradictions (see §8)
   during extraction instead of copying them forward.

## 4. Proposed repo layout

```
lakehouse-agent-kit/
├── README.md                     # product framing + quickstart
├── IMPLEMENTATION_PLAN.md        # this file
├── CONTRIBUTING.md               # skill-authoring contract (frontmatter, size, lint)
├── LICENSE                       # Apache-2.0 (proposed)
├── skills/                       # SOURCE OF TRUTH — spec-portable skills
│   ├── _template/SKILL.md        # copy-me template
│   ├── lakehouse-core/           # parent/entry skill (connect, profile, route, floors)
│   ├── spark/                    # Apache Spark (Connect-first, spark-submit gotchas)
│   ├── delta/                    # Delta Lake ops (OPTIMIZE/VACUUM/MERGE/time travel/UniForm/CDF)
│   ├── iceberg/                  # Iceberg ops (compaction, snapshots, schema evolution)
│   ├── unity-catalog/            # UC OSS REST, Spark config, credential vending, engine wiring
│   ├── kafka/                    # topics, connectors, Structured Streaming read/write
│   ├── airflow/                  # DAG authoring, 2→3 changes, triggering Spark
│   ├── mlflow/                   # tracking, autologging, registry, AI gateway
│   ├── sdp/                      # Spark Declarative Pipelines (pyspark.pipelines + CLI)
│   ├── delta-sharing/            # open sharing: server + client profile + presign notes
│   └── object-store/            # S3-compatible store (SeaweedFS/MinIO) gotchas  [generalized from seaweedfs-ops]
├── templates/
│   ├── lakehouse.profile.example.(toml|env)   # the connection-profile contract
│   └── reference-stack.profile.*              # maps profile → open-lakehouse compose stack
├── reference-stack/              # OPT-IN operations skills, bound to a concrete deployment
│   ├── lifecycle/                # start/stop/demo/troubleshoot runbooks (needs a CLI/compose)
│   └── dashboard/                # the repo's viewer app (if we keep it)
├── dist/                         # GENERATED (never hand-edited): per-target plugin bundles
│   ├── claude/ .claude-plugin/plugin.json
│   ├── cursor/ ...
│   ├── codex/  ...
│   └── copilot/ (.github/skills vendoring)
├── .claude-plugin/marketplace.json            # Claude Code marketplace catalog
├── metaplugin/plugin.meta.json                # single source for version + target gen
├── installer/                    # pip/uvx (or npm) cross-agent installer
├── scripts/                      # generator + validators (skills lint, floor checks, drift)
├── docs/                         # connection-profile guide, per-engine quickstarts, FAQ
└── .github/workflows/            # CI: skill lint, frontmatter/portability check, dist drift
```

## 5. Skill inventory — what to port and how

From the reference repo's 12 skills:

| Source skill | Target | v1? | Generalization work |
|---|---|---|---|
| `spark-4-1` | `spark` | ✅ | Replace `spark-master-41`/`:15002`/network names with profile vars; keep image gotchas behind "if you run the stock `apache/spark` image". |
| `delta-ops` | `delta` | ✅ | Drop stack-only catalog names; resolve Delta-vs-Iceberg "default" contradiction (§8). |
| `iceberg-ops` | `iceberg` | ✅ | Parameterize catalog URI/warehouse; reconcile write-vs-read-only claim (§8). |
| `unity-catalog-oss` | `unity-catalog` | ✅ | Parameterize UC endpoint (`:8081`→profile); fix backing-store contradiction (H2 vs Postgres, §8). |
| `kafka-streaming` | `kafka` | ✅ | Replace `docker exec kafka` + `kafka:9092`/`localhost:9092` with profile bootstrap servers + generic CLI path. |
| `airflow-3` | `airflow` | ✅ | Replace `docker exec airflow-*` with profile-driven invocation; keep 2→3 breaking-change content. |
| `mlflow` | `mlflow` | ✅ | Parameterize tracking URI/gateway; strip stack-only gateway routes. |
| `sdp` (+5 sub-files) | `sdp` | ✅ | Mostly portable (OSS `pyspark.pipelines`); parameterize UC target recipe. |
| `seaweedfs-ops` | `object-store` | ✅ | Generalize to "S3-compatible object store"; keep SeaweedFS-specific gotchas as a labeled section; add MinIO note. |
| `delta-sharing` | `delta-sharing` | ⚠️ partial | Keep the OSS sharing-server + client-profile knowledge; the repo's re-signing proxy is stack-specific → `reference-stack`. |
| `lakehouse-lifecycle` (+4 sub + 2 scripts) | `reference-stack/lifecycle` | ⚠️ opt-in | Only works with a concrete CLI + compose. Ship as reference-stack profile; generalize the runbook shape (preflight → start → smoke → stop). |
| `dashboard` | `reference-stack/dashboard` | ❓ decide | Entirely repo-specific Next.js viewer. Keep only if we ship/point to a reference app. |

**9 portable engine skills** form v1's core. **3** are deployment-bound and move to an
opt-in `reference-stack` profile.

Every "Generalization work" note above is an instance of the §3.4 resolution order:
"replace `<host:port>`/container name with profile vars" means *resolve via config →
discover → preferred default → fail*, calling the shared `lakehouse-core` resolver helpers
rather than re-implementing discovery in each skill. Each port/endpoint a source skill
hardcodes becomes a documented *preferred default*, not an assumption.

## 6. The central problem: the connection-profile abstraction

Every coupling point the reference skills bake in must become a **profile value**:

- Endpoints: UC REST, Spark Connect (`sc://`), Kafka bootstrap, object-store S3 endpoint,
  MLflow tracking/gateway, Airflow, Delta Sharing — as host:port, not `localhost:NNNN`.
- Invocation: how to run `spark-submit`/`spark-sql`, `kafka-*.sh`, `airflow`, `mlflow`
  (local binary vs. `docker exec <name>` vs. k8s) — a small, documented set of modes.
- Identity: S3 keys, UC auth/token, Postgres creds — referenced by env var name, never
  inlined.
- Naming: catalogs/schemas/buckets/warehouse paths — defaults overridable per profile.

Profiles express **preferred defaults, not hard assumptions.** A value that is present in
the profile is authoritative; a value that is *absent* triggers the resolution order from
§3.4 (discover → preferred default → fail loud), not an immediate error — so a deployment
that moved Kafka off 9092 or runs Spark Connect on a non-standard port still works without
anyone editing the skill. Only *identity/targeting* resolution refuses to guess.

Deliverables: `templates/lakehouse.profile.example.*` (the contract), a `docs/` guide,
shared `lakehouse-core` **resolver helpers** (one implementation of config → discover →
preferred-default → fail that every product skill calls, so discovery/preference logic is
never re-hand-rolled per skill), a `lakehouse-core` routine that detects/validates a
profile and refuses to guess *identity* (mirrors the Databricks "NEVER auto-select a
profile" rule), and a shipped `reference-stack.profile` that reproduces the open-lakehouse
compose values so the runbook skills still work out of the box.

## 7. Distribution & versioning

**Three channels, one source of truth** (`metaplugin/plugin.meta.json` + `scripts/` generator, CI drift-checked):

| Channel | Use | Notes |
|---|---|---|
| Plugin marketplace (`.claude-plugin/marketplace.json`, `git-subdir` source) | Claude Code (+ Cursor/Codex) one-command install + updates | Per-target `plugin.json` generated; Copilot cloud takes no plugins → vendor into `.github/skills/`. |
| `pip`/`uvx` installer | **cross-agent workhorse** — auto-detect agents, write skills into each agent's native dir, per-skill + experimental selection | Mirrors `databricks aitools install`. |
| Copy-in / `.zip` | lowest common denominator; any skills-aware agent via `--add-dir` | No auto-update. |

**Versioning:** pin tool versions via the `compatibility` field (≤500 chars) and enforce
a **version floor check** in `lakehouse-core` (present-but-too-old ⇒ upgrade). Single-source
the toolkit version in `plugin.meta.json`; never hand-edit generated files.

**MCP (optional, later):** point at existing community servers (Spark history,
Trino, Iceberg, Unity Catalog) as a complementary live-tools layer; do not rebuild.

## 8. Known issues to fix during extraction (do not copy forward)

The reference skills contain contradictions/staleness we must resolve with a single
verified answer per topic:

- **UC backing store:** H2 (compose/CLAUDE.md) vs. Postgres (`unity-catalog-oss` skill).
- **`iceberg.*` writability:** "read-only" (spark/UC skills) vs. actual writes in
  `smoke.sh` + `iceberg-ops`.
- **Default table format:** Iceberg (`delta-ops`) vs. Delta primary write path (CLAUDE.md).
- **Kafka version:** 3.5 vs 3.6 (CP 7.5).
- **Version drift** between the `lakehouse-engineer` agent file and the skills.
- **Internal refs** (`PR #13`, `T-4.8`, `I-45`, `§2.2`) and a dangling `[[databricks-build-proxies]]`
  wiki-link — strip or replace with public references.

Each must be re-verified against a running stack before the skill ships.

## 9. Phased delivery

- **Phase 0 — Bootstrap (this scaffold).** Repo, README, this plan, CONTRIBUTING,
  skill template, CI skeleton, governance decisions. *(prose — done directly)*
- **Phase 1 — Foundations.** `lakehouse-core` parent skill; connection-profile contract +
  reference-stack profile; skills linter + frontmatter/portability validator; `plugin.meta.json`
  + generator producing one target.
- **Phase 1.5 — Resolver + lint for generalization (pre-req for Phase 2).** Add shared
  **resolver helpers** to `lakehouse-core` implementing the §3.4 order (config → discover →
  preferred default → fail) once, for all skills to call; extend the skills linter to flag
  literal `localhost:PORT`/container names *and* bare port literals used as assumptions
  (require they appear only as a labeled preferred default behind a resolver). This makes
  the generalization principle enforceable, not just documented.
- **Phase 2 — Port the 9 engine skills.** One skill per task/PR, parameterized against the
  profile via the resolver helpers (discover-then-prefer, never hardcode), each
  **cross-reviewed by a different vendor**; resolve the §8 issues; add "verified on"
  provenance.
- **Phase 3 — Reference-stack ops.** `lifecycle` + (decide) `dashboard` + delta-sharing proxy,
  as the opt-in reference-stack profile.
- **Phase 4 — Distribution.** Multi-target generation + marketplace + `pip`/`uvx` installer;
  CI drift checks.
- **Phase 5 — MCP (optional).** Document/bundle complementary community MCP servers.
- **Phase 6 — v1 release.** Docs, examples, versioned release, announce.

## 10. Open decisions for Andre & Tony

1. **v1 engine scope** — all 9, or a first cut (e.g. spark/delta/iceberg/unity-catalog/kafka)?
2. **BYO-stack vs. vendor a reference stack** — do we ship/point to a runnable stack, or
   stay deployment-agnostic with a profile only?
3. **Installer now vs. later** — plugin marketplace + copy-in first, add the `pip`/`uvx`
   installer in Phase 4, or build it up front?
4. **MCP** — reuse community servers, or invest in a first-party one for UC/Iceberg?
5. **Relationship to `open-lakehouse`** — is it the upstream source of truth for skill
   content (sync direction), or do skills fork and diverge?
6. **Repo home / org, naming** (`lakehouse-agent-kit`?), **license** (Apache-2.0?),
   and **maintainership**.

## 11. Risks

- **Source staleness** (§8) — mitigate with verification-against-running-stack gating.
- **Fast-moving OSS versions** — `compatibility` pins + a re-verification cadence.
- **Cross-agent dialect drift** — contain harness-specifics in generated bundles; keep
  skill bodies spec-portable; CI parity checks.
- **Scope creep into a runtime CLI/MCP** — hold the line: skills drive *existing* CLIs in v1.

## 12. How this gets built (delegation model)

This repo is built by polly's orchestration pattern: each skill or component is a scoped
**implement** task in its own git worktree that opens its **own PR**; every PR is
**cross-reviewed by a different-vendor sub-agent** against an acceptance contract; a human
merges. Investigation/verification tasks (e.g. confirming the §8 contradictions on a
running stack) are delegated **explore** tasks. polly never merges.

---

### Source evidence
- Reference architecture audit: `open-lakehouse/open-lakehouse@a8e1b93` `.claude/skills/`
  (12 skills, two-key frontmatter, repo-coupling catalogue, §8 contradictions).
- Packaging/landscape research: Anthropic Agent Skills spec & progressive disclosure;
  Claude Code plugin + marketplace manifests; `databricks/databricks-agent-skills` parent/
  product routing + multi-target generation + `aitools install`; UCFunctionToolkit; community
  MCP servers. (Full citations in the planning threads.)
