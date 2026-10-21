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

## <Task section>

```bash
# profile-driven invocation — not a single deployment's exact command
<cli> --endpoint "$LAKEHOUSE_<SECTION>_<KEY>" ...
```

## Common pitfalls / gotchas

- <verified gotcha> — verified on <tool>@<version>, <date>.

## See also

- [[lakehouse-core]], [[<related-skill>]]

<!--
Depth goes in references/<topic>.md and scripts/, loaded on demand. Keep this file
≤ ~500 lines. Portable frontmatter fields only (see CONTRIBUTING.md).
-->
