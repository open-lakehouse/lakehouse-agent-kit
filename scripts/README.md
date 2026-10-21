# scripts/ — generator + validators

- `lint_skills.py` — stdlib-only skills linter (portable frontmatter, name/description/size limits, portability, `[[skill-link]]` resolution). Run `python3 -I scripts/lint_skills.py`; `--self-test` checks the linter itself. Runs in CI (`.github/workflows/skills-ci.yml`).

Still to come (Phase 1/4): build the per-target plugin bundles in dist/ from metaplugin/plugin.meta.json; check dist/ drift in CI.
