---
name: skill-slug
description: One specific line stating what this covers and WHEN to load it. Put the trigger first — the model reads this to decide relevance. Reference the lakehouse profile, never a hardcoded host.
license: Apache-2.0
compatibility: Requires <tool> >= <version> reachable via a lakehouse connection profile
metadata:
  version: "0.1.0"
  parent: lakehouse-core
allowed-tools: [Bash, Read]
---

# <Component> <version-range>

<One paragraph grounding the skill: what engine/version this covers and how it connects —
via the **lakehouse profile** (`$LAKEHOUSE_<SECTION>_<KEY>` values), NOT a hardcoded
host, port, or container name.>

**FIRST: use the parent skill `lakehouse-core`** to resolve the connection profile and
verify the tool version floor before running anything here.

## When to use this

- <intent 1>
- <intent 2>

## Resolve the environment (discover, don't hardcode; prefer, don't guess)

Resolve the endpoint before using it — never assume a literal host/port. Use the profile
key for this section (e.g. `$LAKEHOUSE_<SECTION>_<KEY>`, such as
`$LAKEHOUSE_SPARK_CONNECT_URI`):

1. **Config** — use the `$LAKEHOUSE_<SECTION>_<KEY>` value / the lakehouse profile if set.
2. **Discover** — else probe: ask the tool/API for its configured endpoint, test whether
   the conventional port (`<PREFERRED_PORT>`) is reachable, or list running containers.
3. **Preferred default** — else fall back to `<host>:<PREFERRED_PORT>`, verify it's
   reachable, and treat it as a preference, not a certainty.
4. **Fail loud** — else stop and tell the user how to set `$LAKEHOUSE_<SECTION>_<KEY>`.

Call `lakehouse-core`'s resolver for this — `python3 -I scripts/resolve_endpoint.py
<section>.<key> --profile "<name-the-user-chose>" --format env` (path relative to
`lakehouse-core`) — don't re-implement discovery here. A port literal may appear only as a
labeled preferred default (see CONTRIBUTING.md).
Discover *topology* this way — but never auto-pick *which* catalog/schema/bucket/creds to
act on; confirm those.

## <Task section>

```bash
# endpoint is the RESOLVED value from the step above — not a baked-in host/port literal
<cli> --endpoint "$LAKEHOUSE_<SECTION>_<KEY>" ...
```

## Common pitfalls / gotchas

- <verified gotcha> — verified on <tool>@<version>, <date>.

## See also

- [[lakehouse-core]], `<related-skill>` (switch to a double-bracket link only once that skill exists)

<!--
Depth goes in references/<topic>.md and scripts/, loaded on demand. Keep this file
≤ ~500 lines. Portable frontmatter fields only (see CONTRIBUTING.md).
-->
