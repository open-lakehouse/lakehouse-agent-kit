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
2. **No secrets inline.** Reference credentials by env-var name.
3. **Invocation modes are explicit.** If a step runs a CLI, show the profile-driven form
   (local binary / `docker exec` / k8s), not one deployment's exact command.
4. **Deployment-specific content** (a specific compose stack, proxy, or app) goes under
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
