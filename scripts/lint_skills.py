#!/usr/bin/env python3
"""Lint every skills/*/SKILL.md against the skill-authoring contract (CONTRIBUTING.md).

Stdlib-only; run as `python3 -I scripts/lint_skills.py`. Checks:

  frontmatter   only the six spec-portable fields (name, description, license,
                compatibility, metadata, allowed-tools); flags Claude-Code-only keys
  name          present, kebab-case, equals the directory name
  description   present, <= 1536 chars (skill-listing truncation limit)
  compatibility <= 500 chars when present
  metadata      a mapping; `parent`, when set, names an existing skill
  size          body <= 500 lines
  portability   no hardcoded `localhost:`, `127.0.0.1`, `sc://<literal-host>`, literal
                `docker exec <container-name>`, bare `:PORT` literals, or prose port
                assumptions (`port 9092`). A port literal is allowed only as a LABELED
                preferred default in a skill that INVOKES lakehouse-core's resolver (a
                non-comment command line `python3 … resolve_endpoint.py <section>.<key>`
                inside a code fence that is not an `output`/`text` fence; prose mentions
                do not count): the line says "preferred default" / "prefer…" /
                "conventional", or sits under a heading containing "preferred default".
                Fences get no blanket exemption — not even ```output.
  links         every [[skill-link]] is a kebab-case slug that resolves to an existing
                skills/<slug>/SKILL.md inside the skills directory (no placeholders)

Directories whose name starts with `_` (e.g. `_template`) are copy-me templates: every
rule applies except name == directory.

Suppression: a line consisting only of the HTML comment
    <!-- lint-allow: <rule>[,<rule>...] -- <justification, >= 10 chars> -->
exempts the NEXT line from the named portability rule(s) only; when the next line opens a
code fence, it exempts that whole fenced block (comments inside a fence would render). Any
other use of
`lint-allow` (inline, unknown rule, missing justification, nothing following) is itself
a violation. Reserve it for deliberate anti-pattern examples.

Exit status: 0 clean, 1 violations found, 2 usage error.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import tempfile

PORTABLE_KEYS = ("name", "description", "license", "compatibility", "metadata", "allowed-tools")
# Known harness-specific keys, named in messages so authors know where they belong.
CLAUDE_ONLY_KEYS = {
    "when_to_use", "hooks", "model", "argument-hint", "disable-model-invocation",
    "user-invocable", "context", "agent", "paths", "shell", "effort",
}
NAME_RE = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*")  # always .fullmatch()
TOP_KEY_RE = re.compile(r"^([A-Za-z0-9_][A-Za-z0-9_-]*)\s*:(.*)$")
LINK_RE = re.compile(r"\[\[([^\]\n]+)\]\]")

MAX_DESCRIPTION = 1536
MAX_COMPATIBILITY = 500
MAX_NAME = 64
MAX_BODY_LINES = 500
PORTABILITY_RULES = ("sc-localhost", "loopback-ip", "localhost", "sc-uri", "docker-exec",
                     "bare-port", "port-assumption")
ALLOW_RE = re.compile(
    r"^\s*<!--\s*lint-allow:\s*([a-z-]+(?:\s*,\s*[a-z-]+)*)\s+--\s+(.*?)\s*-->\s*$")
MIN_JUSTIFICATION = 10

# Portability rules, in priority order. A later match that overlaps an earlier one on
# the same line is suppressed so one endpoint yields one message.
HOST_RULES = (
    ("sc-localhost", re.compile(r"sc://localhost(?::\d+)?", re.I),
     "hardcoded Spark Connect URI; use the profile's spark.connect_uri"),
    ("loopback-ip", re.compile(r"(?<![\d.])127\.0\.0\.1(?::\d+)?(?![\d])"),
     "hardcoded loopback address; reference a lakehouse profile value"),
    ("localhost", re.compile(r"\blocalhost:\d*", re.I),
     "hardcoded localhost endpoint; reference a lakehouse profile value"),
    # Any other literal Spark Connect host. Generic words (`sc://host:port`) and template
    # placeholders (`sc://SPARK_HOST`) are illustrations, not endpoints.
    ("sc-uri", re.compile(r"sc://(?!(?:host|hostname)\b|[A-Z][A-Z0-9_]*_HOST\b)[A-Za-z0-9][\w.-]*(?::\d+)?"),
     "hardcoded Spark Connect host; resolve spark.connect_uri via lakehouse-core's resolver"),
)
DOCKER_EXEC_RE = re.compile(r"\bdocker\s+(?:container\s+)?exec\b")
# Options of `docker exec` that consume the following token as their value.
DOCKER_VALUE_OPTS = {"-u", "--user", "-e", "--env", "-w", "--workdir", "--env-file", "--detach-keys"}
# `:PORT` = colon + 1-5 digits not preceded by a digit (times, ratios), `[` (slices) or
# `:`, and not followed by a word char or `.x` (versions).
PORT_RE = re.compile(r"(?<![\d\[:]):(\d{1,5})(?!\w)(?!\.\w)")
# The token directly before the colon; used to skip colon-number identifiers that are
# not host:port — file line refs (`foo.py:42`) and labels like `RFC:7231`, `PR:456`.
PREFIX_TOKEN_RE = re.compile(r"([\w.#/-]*)$")
FILE_REF_RE = re.compile(
    r"\.(?:py|md|sh|toml|env|json|ya?ml|txt|sql|scala|java|kt|ts|js|go|rs|cfg|ini|conf)$")
# Prose port assumption: "port 9092", "ports 8080". Two+ digits keeps "port 1 of 3" out.
PORT_WORD_RE = re.compile(r"\bports?\s+(?:number\s+)?(\d{2,5})\b", re.I)
# Labels that make a port literal a stated preference rather than an assumption...
PREFERRED_LABEL_RE = re.compile(r"\bpreferred[ -]defaults?\b|\bprefer(?:s|red|ring)?\b|\bconventional\b", re.I)
# ...honored only when the skill actually INVOKES the shared resolver: a python command
# running resolve_endpoint.py on a section.key, on a fenced (non-output) command line.
RESOLVER_CALL_RE = re.compile(
    r"(?:^|[\s(`\"'$;&|])python3?(?:\s+-[A-Za-z]+)*\s+[^\s\"'`]*\bresolve_endpoint\.py"
    r"\s+[\"']?[a-z_]+\.[a-z_]+\b")
NON_COMMAND_FENCES = {"output", "text", "txt", "plain", "plaintext", "log", "md", "markdown"}
HEADING_RE = re.compile(r"^(#{1,6})\s+(.*)$")
FENCE_RE = re.compile(r"^\s*(`{3,}|~{3,})\s*([\w-]*)")
NON_PORT_LABELS = {
    "rfc", "pr", "prs", "pull", "issue", "issues", "bug", "cve", "ticket", "jira", "gh",
    "line", "lines", "step", "phase", "page", "section", "sec", "chapter", "item", "task",
    "spark", "kafka", "flip", "kip", "pep", "sip", "spip", "v", "version",
}


class Linter:
    def __init__(self, skills_dir: str):
        self.skills_dir = skills_dir
        self.violations: list[str] = []

    def report(self, path: str, line: int, rule: str, msg: str) -> None:
        self.violations.append(f"{path}:{line}: [{rule}] {msg}")

    # -- discovery ---------------------------------------------------------------
    def skill_dirs(self) -> list[str]:
        return sorted(
            d for d in os.listdir(self.skills_dir)
            if os.path.isdir(os.path.join(self.skills_dir, d)) and not d.startswith(".")
        )

    def skill_exists(self, slug: str) -> bool:
        """True only for a kebab-case slug whose SKILL.md really lives under skills_dir
        (rejects `../x`, absolute paths, and symlinks pointing outside)."""
        if not NAME_RE.fullmatch(slug):
            return False
        root = os.path.realpath(self.skills_dir)
        target = os.path.realpath(os.path.join(root, slug, "SKILL.md"))
        return os.path.commonpath([root, target]) == root and os.path.isfile(target)

    def run(self) -> int:
        dirs = self.skill_dirs()
        root = os.path.realpath(self.skills_dir)
        for d in dirs:
            path = os.path.join(self.skills_dir, d, "SKILL.md")
            if os.path.commonpath([root, os.path.realpath(os.path.join(root, d))]) != root:
                self.report(os.path.join(self.skills_dir, d), 0, "link",
                            "skill directory is a symlink that resolves outside skills/")
                continue
            if not os.path.isfile(path):
                self.report(os.path.join(self.skills_dir, d), 0, "missing-skill-md",
                            "skill directory has no SKILL.md")
                continue
            self.lint_file(path, d)
        return len(dirs)

    # -- per file ----------------------------------------------------------------
    def lint_file(self, path: str, dirname: str) -> None:
        with open(path, encoding="utf-8") as fh:
            lines = fh.read().splitlines()
        if not lines or lines[0].strip() != "---":
            self.report(path, 1, "frontmatter", "file must start with a '---' YAML frontmatter block")
            return
        end = next((i for i in range(1, len(lines)) if lines[i].strip() == "---"), None)
        if end is None:
            self.report(path, 1, "frontmatter", "unterminated frontmatter (no closing '---')")
            return
        fields = self.parse_frontmatter(path, lines, end)
        self.check_fields(path, dirname, fields)

        body_lines = len(lines) - end - 1
        if body_lines > MAX_BODY_LINES:
            self.report(path, end + 2, "size",
                        f"body is {body_lines} lines (> {MAX_BODY_LINES}); move depth into references/")

        fences = self.fence_info(lines, end + 1)
        has_resolver = any(self.invokes_resolver(lines[i], fences[i]) for i in range(end + 1, len(lines)))
        allowed: set[str] = set()        # rules suppressed for the current line
        fence_allowed: set[str] = set()  # rules suppressed for the whole current fence
        label_level = 0                  # heading level of an open "preferred default" section
        for idx, text in enumerate(lines, start=1):
            info, marker = fences[idx - 1]
            if marker == "open":  # a lint-allow right before the fence covers all of it
                fence_allowed = allowed
            elif info is None:
                fence_allowed = set()
            if info is None:
                hm = HEADING_RE.match(text)
                if hm:
                    level = len(hm.group(1))
                    if re.search(r"preferred[ -]defaults?", hm.group(2), re.I):
                        label_level = level
                    elif label_level and level <= label_level:
                        label_level = 0
            labeled = bool(label_level) or bool(PREFERRED_LABEL_RE.search(text))
            next_allowed = self.parse_allow(path, idx, text, lines)
            self.check_portability(path, idx, text, allowed | fence_allowed, labeled=labeled,
                                   has_resolver=has_resolver)
            self.check_links(path, idx, text)
            allowed = next_allowed

    @staticmethod
    def fence_info(lines: list[str], start: int) -> list:
        """Per line: (info string of the enclosing fence or None, marker) where marker is
        "open", "close", or None. Marker lines carry their fence's info string."""
        out: list = [(None, None)] * len(lines)
        fence = None  # (marker, info)
        for i in range(start, len(lines)):
            fm = FENCE_RE.match(lines[i])
            if fm and fence is None:
                fence = (fm.group(1), fm.group(2).lower())
                out[i] = (fence[1], "open")
            elif fm and fm.group(1)[0] == fence[0][0] and len(fm.group(1)) >= len(fence[0]) \
                    and not fm.group(2):
                out[i] = (fence[1], "close")
                fence = None
            else:
                out[i] = (fence[1] if fence else None, None)
        return out

    @staticmethod
    def invokes_resolver(text: str, fence) -> bool:
        """A real resolver invocation: a command line inside a non-output code fence, with
        shell comments (`# …`, including trailing ones) removed first."""
        info, marker = fence
        if info is None or marker or info in NON_COMMAND_FENCES:
            return False
        line = re.sub(r"(?:^|\s)#.*$", "", text.strip())
        return bool(RESOLVER_CALL_RE.search(line))

    def parse_allow(self, path: str, line: int, text: str, lines: list[str]) -> set[str]:
        """Validate a `lint-allow` comment; return the rules it suppresses on the next line."""
        if "lint-allow" not in text:
            return set()
        m = ALLOW_RE.match(text)
        if not m:
            self.report(path, line, "lint-allow", "must be a standalone line "
                        "'<!-- lint-allow: <rule>[,<rule>] -- <justification> -->'")
            return set()
        rules = {r.strip() for r in m.group(1).split(",")}
        unknown = sorted(rules - set(PORTABILITY_RULES))
        if unknown:
            self.report(path, line, "lint-allow", f"unknown rule(s) {', '.join(unknown)}; "
                        f"allowed: {', '.join(PORTABILITY_RULES)}")
            return set()
        if len(m.group(2).strip()) < MIN_JUSTIFICATION:
            self.report(path, line, "lint-allow",
                        f"justification after '--' must be at least {MIN_JUSTIFICATION} chars")
            return set()
        if line >= len(lines) or not lines[line].strip():
            self.report(path, line, "lint-allow", "must be followed by the line it exempts")
            return set()
        return rules

    def parse_frontmatter(self, path: str, lines: list[str], end: int) -> dict:
        """Minimal top-level YAML reader: key -> (line_no, scalar_text, child_lines)."""
        fields: dict = {}
        current = None
        for i in range(1, end):
            raw = lines[i]
            if not raw.strip() or raw.lstrip().startswith("#"):
                continue
            if raw[0] in " \t":
                if current is None:
                    self.report(path, i + 1, "frontmatter", "indented line before any key")
                else:
                    fields[current][2].append(raw.strip())
                continue
            m = TOP_KEY_RE.match(raw)
            if not m:
                self.report(path, i + 1, "frontmatter", f"unparseable frontmatter line: {raw.strip()!r}")
                current = None
                continue
            key, value = m.group(1), m.group(2).strip()
            if key in fields:
                self.report(path, i + 1, "frontmatter", f"duplicate key '{key}'")
            fields[key] = (i + 1, value, [])
            current = key
        return fields

    @staticmethod
    def scalar(entry) -> str:
        """Join a scalar value (including folded/literal block continuation lines)."""
        _, value, children = entry
        parts = [] if value in ("", ">", "|", ">-", "|-") else [value]
        text = " ".join(parts + children).strip()
        if len(text) >= 2 and text[0] == text[-1] and text[0] in "\"'":
            text = text[1:-1]
        return text

    def check_fields(self, path: str, dirname: str, fields: dict) -> None:
        for key, (line, _, _) in fields.items():
            if key in PORTABLE_KEYS:
                continue
            if key in CLAUDE_ONLY_KEYS:
                why = "Claude-Code-only extension; it belongs in the generated dist/ bundle, not the skill body"
            else:
                why = "not one of the six spec-portable fields"
            self.report(path, line, "frontmatter-key", f"'{key}' is {why}")

        if "name" not in fields:
            self.report(path, 1, "name", "missing required 'name'")
        else:
            line = fields["name"][0]
            name = self.scalar(fields["name"])
            if not NAME_RE.fullmatch(name) or len(name) > MAX_NAME:
                self.report(path, line, "name", f"'{name}' must be kebab-case, <= {MAX_NAME} chars")
            if not dirname.startswith("_") and name != dirname:
                self.report(path, line, "name", f"'{name}' does not match directory '{dirname}'")

        if "description" not in fields or not self.scalar(fields["description"]):
            self.report(path, fields.get("description", (1,))[0], "description",
                        "missing or empty required 'description'")
        else:
            n = len(self.scalar(fields["description"]))
            if n > MAX_DESCRIPTION:
                self.report(path, fields["description"][0], "description",
                            f"description is {n} chars (> {MAX_DESCRIPTION}, truncated in skill listing)")

        if "compatibility" in fields:
            n = len(self.scalar(fields["compatibility"]))
            if n > MAX_COMPATIBILITY:
                self.report(path, fields["compatibility"][0], "compatibility",
                            f"compatibility is {n} chars (> {MAX_COMPATIBILITY})")

        if "metadata" in fields:
            line, value, children = fields["metadata"]
            if value not in ("", "{}"):
                self.report(path, line, "metadata", "'metadata' must be a mapping")
            for child in children:
                m = re.match(r"^parent\s*:\s*(.*)$", child)
                if m:
                    parent = m.group(1).strip().strip("\"'")
                    if not self.skill_exists(parent):  # also rejects non-slug / traversal
                        self.report(path, line, "metadata",
                                    f"metadata.parent '{parent}' is not an existing skill")

    def check_portability(self, path: str, line: int, text: str, allowed: set[str],
                          labeled: bool = False, has_resolver: bool = False) -> None:
        taken: list[tuple[int, int]] = []
        report = self.report

        def emit(p, ln, rule, msg):
            if rule not in allowed:
                report(p, ln, rule, msg)

        def free(start: int, end: int) -> bool:
            return all(end <= s or start >= e for s, e in taken)

        for rule, rx, msg in HOST_RULES:
            for m in rx.finditer(text):
                if free(m.start(), m.end()):
                    taken.append((m.start(), m.end()))
                    emit(path, line, rule, f"{msg}: {m.group(0)!r}")

        for m in DOCKER_EXEC_RE.finditer(text):
            target = self.docker_target(text[m.end():])
            if target is not None:
                taken.append((m.start(), m.end()))
                emit(path, line, "docker-exec",
                            f"literal container name in `docker exec {target}`; use the profile's "
                            "<section>.container (e.g. \"$LAKEHOUSE_SPARK_CONTAINER\")")

        def port_literal(rule: str, m, msg: str) -> None:
            """Port rules: skipped only for a labeled preferred default in a skill that
            invokes the resolver (or via an explicit lint-allow)."""
            if not free(m.start(), m.end()):
                return
            taken.append((m.start(), m.end()))
            if labeled:
                if has_resolver:
                    return
                msg += ("; it is labeled a preferred default, but this skill never invokes "
                        "lakehouse-core's resolve_endpoint.py (a fenced command line, not a mention)")
            emit(path, line, rule, msg)

        for m in PORT_RE.finditer(text):
            port = m.group(1)
            if not (0 < int(port) <= 65535) or self.not_a_port(text[: m.start()]):
                continue
            port_literal("bare-port", m, f"hardcoded port ':{port}'; resolve the endpoint via "
                         "lakehouse-core's resolver (profile value, else discovery, else a "
                         "labeled preferred default)")

        for m in PORT_WORD_RE.finditer(text):
            if 0 < int(m.group(1)) <= 65535:
                port_literal("port-assumption", m, f"port assumption {m.group(0)!r}; resolve the "
                             "endpoint via lakehouse-core's resolver, or label it a preferred default")

    @staticmethod
    def not_a_port(before: str) -> bool:
        """True when the token before `:N` marks an identifier, not a host."""
        token = PREFIX_TOKEN_RE.search(before).group(1)
        return bool(FILE_REF_RE.search(token)) or token.lstrip("#").lower() in NON_PORT_LABELS

    @staticmethod
    def docker_target(rest: str):
        """Return the literal container token after `docker exec`, or None if it is a
        variable/placeholder or absent (e.g. prose like "`docker exec` / k8s")."""
        if rest.startswith("`") or not rest.strip():
            return None
        tokens = rest.split()
        i = 0
        while i < len(tokens):
            tok = tokens[i]
            if tok.startswith("-") and len(tok) > 1:
                i += 2 if (tok in DOCKER_VALUE_OPTS and "=" not in tok) else 1
                continue
            tok = tok.strip("`")
            if not tok or tok.lstrip("\"'").startswith(("$", "<", "{{")) or tok in ("...", "…"):
                return None
            return tok
        return None

    def check_links(self, path: str, line: int, text: str) -> None:
        for m in LINK_RE.finditer(text):
            target = m.group(1).split("|", 1)[0].strip()
            if not NAME_RE.fullmatch(target):
                self.report(path, line, "link", f"[[{target}]] is not a kebab-case skill slug")
            elif not self.skill_exists(target):
                self.report(path, line, "link", f"[[{target}]] does not resolve to skills/{target}/SKILL.md")


# -- self-test -------------------------------------------------------------------------
GOOD = """---
name: good-skill
description: Load when testing the linter. Uses the lakehouse profile only.
license: Apache-2.0
compatibility: Requires tool >= 1.0 reachable via a lakehouse connection profile
metadata:
  version: "0.1.0"
  parent: other-skill
allowed-tools: [Bash, Read]
---

# Good

Run `docker exec -it "$LAKEHOUSE_SPARK_CONTAINER" spark-sql` or `docker exec` / k8s.
docker exec -u root <container> bash
At 10:30 on 2026-10-21T10:30:00, slice x[:10], ratio 1:10, see load_profile.py:42.
Per RFC:7231, PR:456, issue:123, #issue:9, KIP:848 and Spark:4.0.1 nothing here is a port.
Endpoint "$LAKEHOUSE_KAFKA_BOOTSTRAP_SERVERS". See [[other-skill]] and `planned-skill`.
<!-- lint-allow: localhost,bare-port -- deliberate anti-pattern shown to readers -->
Never write localhost:8081 in a skill; read [unity_catalog].rest_url instead.
"""

BAD = """---
name: wrong-name
description: Bad skill.
when_to_use: always
metadata:
  parent: nope
---

Connect to sc://localhost:15002 and localhost:8081 and 127.0.0.1:9000.
docker exec kafka kafka-topics.sh --list
UC on uc-server:8080, also :9092, and http://web:8/ (one-digit port).
See [[missing-skill]], [[<related-skill>]], [[../outside-skill]], [[linked-skill]].
Inline exemption localhost:8081 <!-- lint-allow: localhost -- not a standalone line -->
<!-- lint-allow: localhost -- -->
localhost:7000 stays flagged because the justification above is empty.
<!-- lint-allow: everything -- unknown rule names are rejected -->
<!-- lint-allow: bare-port -- only the named rule is suppressed -->
Suppressed port uc:9000 but localhost:9001 is still flagged.
Run `spark-sql --remote sc://spark-master:15002` and point Kafka clients at port 9092.
Prefer :15002 for Spark Connect (labeled, but this skill never calls the resolver).
"""

# Uses the resolver and labels every port literal as a preferred default: must be clean.
GOOD_RESOLVER = """---
name: resolver-skill
description: Load when testing the linter's preferred-default handling.
metadata:
  parent: other-skill
---

Resolve the endpoint (config, then discovery, then a preferred default):

```bash
eval "$(python3 -I scripts/resolve_endpoint.py spark.connect_uri --format env)"
```

If nothing is configured it tries the preferred default `sc://<host>:15002`, verified reachable.
Kafka: preferring the conventional port 9092 when nothing is configured or discovered.
Generic shapes `sc://host:port`, `sc://SPARK_HOST`, and `<host>:<PREFERRED_PORT>` are fine.

## Preferred defaults (used only after config and discovery)

| Service | Shape |
|---|---|
| Unity Catalog REST | `http://<host>:8080/api/2.1/unity-catalog` |

### MLflow

- tracking `http://<host>:5000`

## Example output

<!-- lint-allow: bare-port,port-assumption -- example server output, not an endpoint to use -->
```output
Spark Connect server started at 0.0.0.0:15002 (port 15002)
Ready; accepting connections on :15002
```

Kafka 3.6.1, built 2026-10-21 at 10:30:00; see load_profile.py:42.
"""

# Calls the resolver, but assumes ports outside any preferred-default label or context.
BAD_RESOLVER = """---
name: assume-skill
description: Load when testing the linter's port-assumption rules.
---

```sh
python3 -I scripts/resolve_endpoint.py unity_catalog.rest_url --profile "$LAKEHOUSE_PROFILE"
```
Then just connect to :15002 directly.

## Preferred defaults

- Spark Connect `<host>:15002`

## Usage

Call uc-server:8081 for REST; Kafka listens on port 9092.
```output
Listening on localhost:15002
Kafka listens on port 9092; Spark Connect on :15002
```
```bash
spark-sql --remote sc://spark:15002
```
<!-- lint-allow: port-assumption -- deliberate anti-pattern shown to readers -->
Never write "Kafka on port 9092" in a skill.
"""
BAD_RESOLVER_EXPECT = {"bare-port": 3, "port-assumption": 2, "localhost": 1, "sc-uri": 1}

# Only MENTIONS the resolver (prose, negation, shell comments, output fence): no exemption.
MENTION_ONLY = """---
name: mention-skill
description: Load when testing that a resolver mention is not an invocation.
---

Do not use `python3 -I scripts/resolve_endpoint.py spark.connect_uri`; resolve_endpoint.py is optional.
<!-- python3 -I scripts/resolve_endpoint.py spark.connect_uri -->
```bash
# python3 -I scripts/resolve_endpoint.py spark.connect_uri
echo skip  # python3 -I scripts/resolve_endpoint.py spark.connect_uri
```
```output
$ python3 -I scripts/resolve_endpoint.py spark.connect_uri
```
```text
python3 -I scripts/resolve_endpoint.py spark.connect_uri
```

The preferred default is `sc://<host>:15002`.
"""
MENTION_ONLY_EXPECT = {"bare-port": 1}

BAD_EXPECT = {
    "name": 1, "frontmatter-key": 1, "metadata": 1, "sc-localhost": 1, "localhost": 4,
    "loopback-ip": 1, "docker-exec": 1, "bare-port": 4, "link": 4, "lint-allow": 3,
    "sc-uri": 1, "port-assumption": 1,
}


def self_test() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        # A real skill outside the skills dir, reachable only via `../` traversal.
        os.makedirs(os.path.join(tmp, "outside-skill"))
        with open(os.path.join(tmp, "outside-skill", "SKILL.md"), "w", encoding="utf-8") as fh:
            fh.write(GOOD.replace("good-skill", "outside-skill"))
        tmp = os.path.join(tmp, "skills")
        for slug, text in (("good-skill", GOOD), ("other-skill", GOOD.replace("good-skill", "other-skill")),
                           ("bad-skill", BAD), ("resolver-skill", GOOD_RESOLVER),
                           ("assume-skill", BAD_RESOLVER), ("mention-skill", MENTION_ONLY)):
            os.makedirs(os.path.join(tmp, slug))
            with open(os.path.join(tmp, slug, "SKILL.md"), "w", encoding="utf-8") as fh:
                fh.write(text)
        # A symlinked skill dir escaping the skills dir must neither lint nor resolve.
        os.symlink(os.path.join(tmp, "..", "outside-skill"), os.path.join(tmp, "linked-skill"))
        linter = Linter(tmp)
        linter.run()
    failures = []
    clean = ("/good-skill/", "/other-skill/", "/resolver-skill/")
    good = [v for v in linter.violations if any(c in v for c in clean)]
    if good:
        failures.append("spurious flags on clean skill:\n  " + "\n  ".join(good))
    escaped = [v for v in linter.violations if "/linked-skill:0: [link]" in v]
    if len(escaped) != 1:
        failures.append(f"symlinked skill dir escaping skills/ not reported exactly once: {escaped}")
    for slug, expect in (("bad-skill", BAD_EXPECT), ("assume-skill", BAD_RESOLVER_EXPECT),
                         ("mention-skill", MENTION_ONLY_EXPECT)):
        counts: dict[str, int] = {}
        for v in linter.violations:
            if f"/{slug}/" in v:
                rule = v.split("[", 1)[1].split("]", 1)[0]
                counts[rule] = counts.get(rule, 0) + 1
        if counts != expect:
            failures.append(f"{slug} rule counts {counts} != expected {expect}")
    for slug in ("bad-skill", "mention-skill"):
        hint = [v for v in linter.violations if f"/{slug}/" in v and "never invokes" in v]
        if len(hint) != 1:
            failures.append(f"{slug}: labeled-without-resolver port not reported exactly once: {hint}")
    for f in failures:
        print(f"self-test FAIL: {f}", file=sys.stderr)
    if failures:
        return 1
    print(f"self-test OK: {len(clean)} clean fixtures have 0 flags; bad fixtures flag "
          f"{sum(BAD_EXPECT.values())} + {sum(BAD_RESOLVER_EXPECT.values())} + "
          f"{sum(MENTION_ONLY_EXPECT.values())} expected violations; "
          "escaping symlink dir rejected")
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    ap.add_argument("--skills-dir", default=os.path.join(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))), "skills"), help="skills directory (default: <repo>/skills)")
    ap.add_argument("--self-test", action="store_true", help="run built-in fixture tests and exit")
    args = ap.parse_args(argv)
    if args.self_test:
        return self_test()
    if not os.path.isdir(args.skills_dir):
        print(f"error: skills directory not found: {args.skills_dir}", file=sys.stderr)
        return 2
    linter = Linter(args.skills_dir)
    n = linter.run()
    rel = os.path.relpath(args.skills_dir)
    for v in linter.violations:
        print(v.replace(args.skills_dir, rel, 1))
    if linter.violations:
        print(f"lint_skills: {len(linter.violations)} violation(s) in {n} skill(s)", file=sys.stderr)
        return 1
    print(f"lint_skills: OK — {n} skill(s) clean")
    return 0


if __name__ == "__main__":
    sys.exit(main())
