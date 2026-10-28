# Contributing — skill-authoring contract

Skills are the product. Keep them portable, focused, and honest.

## Frontmatter: portable fields only

Author `SKILL.md` with **only** the six spec-portable YAML fields so the skill works on
Claude.ai, Claude Code, Cursor, Codex, Copilot, and the Skills API:

```yaml
---
name: <kebab-case-slug>                 # required; matches the directory name
description: <one line, specific>       # required; the model uses this to decide when to load
license: Apache-2.0
compatibility: Requires <tool> >= <version> on PATH or via a lakehouse profile
metadata:
  version: "0.1.0"
  parent: lakehouse-core                # product skills only
allowed-tools: [Bash, Read]             # optional, scope down if useful
---
```

Claude-Code-only extensions (`when_to_use`, `hooks`, `model`, `argument-hint`,
`disable-model-invocation`, …) **do not** go in the skill body — they belong in the
generated per-target plugin bundles under `dist/`. Anything beyond the six fields will
error on other platforms.

## Size & progressive disclosure

- `SKILL.md` ≤ ~500 lines. One concept per skill.
- Push depth into `references/<topic>.md` and executable `scripts/`, loaded on demand.
- The combined `description` text is truncated at ~1,536 chars in the skill listing — put
  the "when to load me" trigger first.

## Portability rules (the extraction discipline)

1. **No hardcoded endpoints.** Never write `localhost:8081`, `sc://localhost:15002`, a
   container name, or a repo-relative path in a skill body. Reference **lakehouse
   profile** values and documented placeholders instead.
2. **Discover, don't hardcode; prefer, don't guess.** When a source skill says "use X on
   port N", do **not** copy that literal. Instruct the agent to resolve the value in this
   order, and call the shared `lakehouse-core` resolver helpers rather than re-rolling it:
   1. **Config** — a profile/env value, if set, wins.
   2. **Discover** — else probe the live environment (ask the tool/API for its endpoint,
      test reachability of the conventional port, list running containers, read a
      well-known config). Teach the *discovery command*, not the answer.
   3. **Preferred default** — else fall back to the conventional value, verify it's
      reachable, and label it a preference.
   4. **Fail loud** — else stop and say how to set it. Never silently assume.

   Resolve *topology* this way (where a service lives, which port it's on). Do **not**
   auto-resolve *identity/targeting with side effects* — which profile, catalog, schema,
   bucket, or credentials to act on is confirmed with the user, never guessed.

   ```md
   ❌ Run `spark-sql --remote sc://localhost:15002`                 # baked-in assumption
   ✅ Resolve the Spark Connect endpoint: use `$LAKEHOUSE_SPARK_CONNECT_URI` / the profile
      if set; else check whether a Spark Connect server is reachable (preferring the
      conventional port 15002) and use that; else report that no endpoint was found and
      how to set one. Then: `spark-sql --remote "$LAKEHOUSE_SPARK_CONNECT_URI"`
   ```
3. **No secrets inline.** Reference credentials by env-var name.
4. **Invocation modes are explicit.** If a step runs a CLI, show the profile-driven form
   (local binary / `docker exec` / k8s), not one deployment's exact command — resolved via
   the same order as rule 2.
5. **Deployment-specific content** (a specific compose stack, proxy, or app) goes under
   `reference-stack/`, not in a portable engine skill.

## Honesty & provenance

- Pin tool/CLI versions in `compatibility`.
- Tag verified claims: `verified on <tool>@<version>, <date>`.
- Do not copy forward unresolved contradictions — resolve them against a running stack
  (see `IMPLEMENTATION_PLAN.md` §8) and cite the result.
- Cross-links use `[[other-skill]]`; every target must exist.

## Review

Every skill/PR is cross-reviewed by a **different-vendor** agent against its acceptance
contract before a human merges. CI runs the frontmatter/portability linter and the
`dist/` drift check.

Run the linter locally before opening a PR:

```bash
python3 -I scripts/lint_skills.py
```

It enforces the rules above with `file:line` messages: portable frontmatter only,
`name` == directory, description/compatibility/body limits, no hardcoded `localhost:`,
`127.0.0.1`, `sc://localhost`, literal `docker exec <container>`, or `:PORT` literals,
and every `[[skill-link]]` must be a skill slug that resolves to an existing
`skills/<slug>/` (no placeholders, no paths). Link only to skills that already exist;
name planned ones in backticks.

To show a deliberate anti-pattern, put a standalone comment on the line **before** it,
naming the rule(s) and why:

```markdown
<!-- lint-allow: localhost -- shows readers the hardcoded form to avoid -->
```

It exempts only the next line and only the named rules (`sc-localhost`, `loopback-ip`,
`localhost`, `docker-exec`, `bare-port`); a malformed, inline, or unjustified
`lint-allow` is itself a violation.
