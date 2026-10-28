# Lakehouse Agent Kit — Backlog

Ready-to-file task list. Each **LAK-n** maps 1:1 to a GitHub issue (file them with the
labels shown). See [`ROADMAP.md`](./ROADMAP.md) for phase context and the contributor
workflow, and [`CONTRIBUTING.md`](./CONTRIBUTING.md) for the authoring contract.

**Common acceptance for every skill port:** 6 portable frontmatter fields; `SKILL.md`
<= ~500 lines + progressive disclosure; **no hardcoded host/port/container** — all endpoints
resolved via `skills/lakehouse-core/scripts/resolve_endpoint.py`, identity (catalog/schema/
bucket/creds) confirmed never auto-selected; assigned §8 item resolved + cited; provenance
tags (`verified on <tool>@<ver>, <date>`); all skills-CI gates exit 0; cross-reviewed by a
different vendor; one skill per PR. Source skills are under `open-lakehouse/.claude/skills/`
(pin + cite the commit; the pilot used `open-lakehouse@39f32e68`).

Suggested labels: `phase:2`..`phase:6`, `type:skill-port`, `type:distribution`,
`type:tech-debt`, `type:docs`, `type:infra`, `good-first-issue`, `needs-verification`.

---

## Phase 2 — Engine skills (`phase:2`, `type:skill-port`)

### LAK-1 — Port `spark` skill  `phase:2` `type:skill-port`
Source `spark-4-1`. Spark Connect-first workflows, `spark-submit`/`spark-sql` gotchas,
catalog/storage wiring, a read-only `connect_check.py`. Resolve `spark.connect_uri`, UC REST,
Iceberg REST, S3 via the resolver; master URL discovered/asked, never built; invocation modes
from the profile.
**Pre-applied review fixes (from the lost pilot):** `connect_check.py` must (a) never print
`str(exc)` — only the exception class + static remediation (no-secret-echo); (b) fast-fail on
a portless URI before any Connect attempt (no new default port); (c) bound the Connect attempt
with a ~60s deadline (a TCP-open-but-not-Connect port hangs otherwise); and `catalog-wiring.md`
must `.rstrip("/")` before `.removesuffix("/api/2.1/unity-catalog")`. Keep stock `apache/spark`
image quirks in a labeled section. Verified on `spark@4.1.0`.

### LAK-2 — Port `delta` skill  `phase:2` `type:skill-port`
Source `delta-ops`. OPTIMIZE/Z-ORDER, VACUUM, MERGE, time travel, CDF, UniForm as references.
Resolve `spark.connect_uri` + `object_store.warehouse`; catalog/schema/table confirmed, not
auto-selected. **§8 resolved:** Delta is the configured default / primary write path; Iceberg
REST is read-only in the reference stack (cite `config/spark/spark-defaults.conf.example` +
`CLAUDE.md`). Guard VACUUM retention (< default needs explicit approval; don't disable the
retention check routinely). Verified on `delta@4.3.1`.

### LAK-3 — Port `iceberg` skill  `phase:2` `type:skill-port` `needs-verification`
Source `iceberg-ops`. Compaction/`rewrite_data_files`, snapshot expiration, schema & partition
evolution, maintenance. Parameterize catalog REST (`unity_catalog.iceberg_rest`) + warehouse
(`object_store`). **§8 to resolve:** read-only vs writable — verify against `smoke.sh` +
`iceberg-ops` + UC config and state one cited answer. **Also settle:** Iceberg REST URL format
`/iceberg` vs `/iceberg/v1` (see LAK-23) and align the skill to it.

### LAK-4 — Port `unity-catalog` skill  `phase:2` `type:skill-port` `needs-verification`
Source `unity-catalog-oss`. UC OSS REST, Spark wiring, credential vending, engine wiring.
Resolve `unity_catalog.rest_url` (base URL; strip `/api/2.1/unity-catalog` suffix for the
connector). **§8 to resolve:** backing store H2 vs Postgres — verify and cite. Confirm the
UC REST vs Iceberg REST base-URL conventions used by LAK-3/LAK-1.

### LAK-5 — Port `kafka` skill  `phase:2` `type:skill-port` `needs-verification`
Source `kafka-streaming`. Topics, connectors, Structured Streaming read/write. Resolve
`kafka.bootstrap_servers`; replace `docker exec kafka` with profile-driven invocation.
**§8 to resolve:** Kafka version 3.5 vs 3.6 (CP 7.5) — verify and cite.

### LAK-6 — Port `airflow` skill  `phase:2` `type:skill-port`
Source `airflow-3`. DAG authoring, Airflow 2to3 breaking changes, triggering Spark. Resolve
`airflow.base_url`; replace `docker exec airflow-*` with profile-driven invocation.

### LAK-7 — Port `mlflow` skill  `phase:2` `type:skill-port`
Source `mlflow`. Tracking, autologging, registry, AI gateway. Resolve `mlflow.tracking_uri`
and `mlflow.gateway_uri` (gateway has no preferred default by design — config/candidate only);
strip stack-only gateway routes.

### LAK-8 — Port `sdp` skill  `phase:2` `type:skill-port`
Source `sdp` (+ 5 sub-files). Spark Declarative Pipelines (`pyspark.pipelines` + CLI). Mostly
portable; parameterize the UC target recipe via the profile. Preserve progressive disclosure
across the sub-files.

### LAK-9 — Port `object-store` skill  `phase:2` `type:skill-port`
Source `seaweedfs-ops`, generalized to "S3-compatible object store". Resolve
`object_store.s3_endpoint`/`region`/`path_style`; generic preferred default MinIO `9000`
(SeaweedFS `8333` lives in the reference-stack profile). Keep SeaweedFS-specific gotchas in a
labeled section; add a MinIO note.

---

## Phase 3 — Reference-stack ops (`phase:3`)

### LAK-10 — `reference-stack/lifecycle` runbook skill  `phase:3` `type:skill-port`
Source `lakehouse-lifecycle` (+ sub-files + scripts). Opt-in; only works with a concrete
CLI/compose. Generalize the runbook shape (preflight to start to smoke to stop); drive via the
`reference-stack` profile. Deployment-bound — ship under `reference-stack/`.

### LAK-11 — Decision + port `reference-stack/dashboard`  `phase:3` `type:docs`
Source `dashboard` (repo-specific Next.js viewer). **Decision issue:** do we ship/point to a
reference app at all? If yes, port under `reference-stack/`; if no, document the decision.

### LAK-12 — Port `delta-sharing` skill  `phase:3` `type:skill-port`
Source `delta-sharing`. Portable: OSS sharing server + client profile + presign notes. The
repo's re-signing proxy is stack-specific to `reference-stack/`.

---

## Phase 4 — Distribution (`phase:4`, `type:distribution`)

### LAK-13 — `metaplugin/plugin.meta.json` single source of truth  `phase:4` `type:infra`
One metadata file (name, version, skill list, per-target config) that every bundle + catalog
is generated from. Single-source the toolkit version here.

### LAK-14 — Generator + `dist/` per-target bundles + CI drift check  `phase:4` `type:infra`
`scripts/` generator produces per-agent bundles under `dist/` (never hand-edited); CI fails if
`dist/` drifts from source + `plugin.meta.json`.

### LAK-15 — Claude Code plugin + `.claude-plugin/marketplace.json`  `phase:4` `type:distribution`
Generated `plugin.json` + marketplace catalog (git-subdir source) for one-command install/update.

### LAK-16 — Cursor / Codex target generation  `phase:4` `type:distribution`
Generate the `.agents/skills` (+ agent-specific) bundles; validate against each agent's loader.

### LAK-17 — Copilot vendoring into `.github/skills/`  `phase:4` `type:distribution`
Copilot cloud takes no plugins; vendor skills into `.github/skills/` (+ note surface-dependent
personal-skill scope).

### LAK-18 — `pip`/`uvx` cross-agent installer  `phase:4` `type:distribution`
The cross-agent workhorse: auto-detect installed agents, write skills into each agent's native
dir, per-skill + experimental selection (mirror `databricks aitools install`).

### LAK-19 — Skills CI: full lint + dist drift on every PR  `phase:4` `type:infra`
Extend `skills-ci.yml` to lint every skill (frontmatter/portability/resolver rules) and run the
`dist/` drift check and all `--self-test`s.

---

## Phase 5 — MCP optional (`phase:5`)

### LAK-20 — Document/bundle complementary community MCP servers  `phase:5` `type:docs`
Point at existing MCP servers (Spark history, Trino, Iceberg, Unity Catalog) as an optional
live-tools layer; do not rebuild. Document how they complement the skills.

---

## Phase 6 — Release (`phase:6`)

### LAK-21 — v1 release  `phase:6` `type:docs`
Docs pass, worked examples per engine, versioned release, announcement.

---

## Cross-cutting / tech-debt (`type:tech-debt`, `type:docs`)

### LAK-22 — Fix broken `spark-sql --remote` example  `type:tech-debt` `good-first-issue`
`CONTRIBUTING.md` (portability rule 2) and `skills/lakehouse-core/SKILL.md` show
`spark-sql --remote "$LAKEHOUSE_SPARK_CONNECT_URI"`, which **fails on Spark 4.1**
(`NoClassDefFoundError`, verified against a live 4.1.0 server). Replace with the working
PySpark-snippet / `.remote(...)` guidance. Keep the discover-then-prefer example intact.

### LAK-23 — Resolve Iceberg REST URL format  `type:tech-debt` `needs-verification`
`templates/lakehouse.profile.example.*` ends the Iceberg REST URL in `/iceberg/v1`; the
reference-stack profile uses `/iceberg`. Clients often append `v1/` themselves. Verify against a
running UC/Iceberg REST catalog and make the resolver registry, templates, docs, and LAK-3/LAK-4
consistent on one form.

### LAK-24 — Optional `[spark]` master-URL profile key  `type:tech-debt`
Add an optional `spark.master_url` key to the profile contract + resolver so the `spark` skill
needn't discover/ask for the master. Backward-compatible (optional).

### LAK-25 — Resolver type cleanups  `type:tech-debt` `good-first-issue`
In `resolve_endpoint.py`: fix the stale tuple type-annotation on `candidates_text()` and add a
shared `Candidate` type alias (Phase 1.5 recorded non-blockers). No behavior change.

### LAK-26 — Shrink `banner.png`  `type:docs` `good-first-issue`
`docs/assets/banner.png` is ~4.77 MB. Palette-quantize the pixel art to < 1 MB with no visible
loss so the README loads fast.

### LAK-27 — Delete stale `phase-1-foundations` branch  `type:infra` `good-first-issue`
Superseded by merged `master` (PR #1). Delete the remote branch to avoid confusion.

### LAK-28 — Resolve remaining §8 contradictions during ports  `type:tech-debt` `needs-verification`
Tracking: UC backing store (H2 vs Postgres, LAK-4), Kafka version (3.5 vs 3.6, LAK-5), and the
`lakehouse-engineer` agent-file version drift. Strip any remaining repo-internal refs
(`PR #`, `T-`, `I-`, container names) as skills are ported.

### LAK-29 — Resolver health-check follow-ups  `type:tech-debt`
Consider a protocol-level reachability check beyond a bare TCP probe (a TCP-open-but-wrong-service
port currently looks "reachable"); confirm `mlflow.gateway_uri` intentionally has no preferred default.

### LAK-30 — Durability + merge-gating process  `type:infra`
Push every reviewed skill branch to the remote immediately after review (the Phase 2 pilot was
lost to a local disk failure before push). Consider branch protection that blocks merge unless
`skills-ci` passes.
