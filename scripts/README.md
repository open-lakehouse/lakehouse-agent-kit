# scripts/ — generator + validators

- `lint_skills.py` — stdlib-only skills linter (portable frontmatter, name/description/size limits, portability — including bare/prose port assumptions unless labeled as a preferred default behind lakehouse-core's `resolve_endpoint.py` — and `[[skill-link]]` resolution). Run `python3 -I scripts/lint_skills.py`; `--self-test` checks the linter itself. Runs in CI (`.github/workflows/skills-ci.yml`).

Still to come (Phase 1/4): build the per-target plugin bundles in dist/ from metaplugin/plugin.meta.json; check dist/ drift in CI.
