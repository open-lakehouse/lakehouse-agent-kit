# Lakehouse Agent Toolkit

Cross-agent **Agent Skills** for building on an open-source lakehouse stack —
Apache Spark, Delta Lake, Apache Iceberg, Unity Catalog (OSS), Kafka, Airflow,
MLflow, and Delta Sharing. The open-source analogue of the Databricks agent toolkit.

> **Status: planning / scaffold.** See [`IMPLEMENTATION_PLAN.md`](./IMPLEMENTATION_PLAN.md)
> for the full design, phased roadmap, and open decisions.

## What it is

A parent/entry skill (`lakehouse-core`) that handles connection + profile selection +
tool-version checks, then routes to focused **product skills** (`spark`, `delta`,
`iceberg`, `unity-catalog`, `kafka`, `airflow`, `mlflow`, `sdp`, `delta-sharing`,
`object-store`). Each skill is portable Markdown with progressive disclosure
(`SKILL.md` + `references/` + `scripts/`) and drives the engine's native CLI.

Skills are **stack-agnostic**: endpoints, credentials, and invocation modes come from a
**lakehouse connection profile** (see [`templates/`](./templates)) rather than hardcoded
hosts/ports — so the toolkit works against any deployment, with a ready-made profile for
the [`open-lakehouse`](https://github.com/open-lakehouse/open-lakehouse) reference stack.

## Why

The `open-lakehouse` reference architecture already ships excellent built-in skills, but
they are welded to one Docker Compose deployment. This repo extracts the portable engine
knowledge into a standalone, parameterized, cross-agent toolkit and distributes it via a
plugin marketplace, a `pip`/`uvx` installer, and plain copy-in.

## Install (planned)

```bash
# Claude Code / Cursor / Codex — plugin marketplace
claude plugin marketplace add <org>/lakehouse-agent-toolkit
claude plugin install lakehouse-tools@lakehouse-marketplace

# Any skills-aware agent — cross-agent installer
uvx lakehouse-agent-toolkit install        # auto-detects agents, writes skills into each

# Lowest common denominator — copy the skills dir in
cp -r skills ~/.claude/skills/
```

## Layout

See [`IMPLEMENTATION_PLAN.md` §4](./IMPLEMENTATION_PLAN.md). Source of truth is
[`skills/`](./skills); everything under `dist/` is generated — never hand-edit it.

## Contributing

Read [`CONTRIBUTING.md`](./CONTRIBUTING.md) — the skill-authoring contract (frontmatter,
size limits, portability, verification provenance). Start from
[`skills/_template/SKILL.md`](./skills/_template/SKILL.md).

## License

Apache-2.0 (proposed — see [`LICENSE`](./LICENSE)).
